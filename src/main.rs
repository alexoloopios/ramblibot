#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]

use std::{
    collections::{HashSet, VecDeque},
    env,
    ffi::c_void,
    fs,
    panic::AssertUnwindSafe,
    path::PathBuf,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use chromiumoxide::{Browser, BrowserConfig, Page};
use directories::ProjectDirs;
use futures::StreamExt;
use rungui::{
    App, Button, Buttons, HBox, Label, ListBox, MessageKind, NativeHandle, Timer, VBox, Window,
    message_box,
};
use scraper::{Html, Selector};
use tokio::{
    sync::{
        Notify,
        mpsc::{self as tokio_mpsc, error::TryRecvError},
    },
    task::JoinHandle,
    time::{sleep, timeout},
};

const RAMBLIO_LOGIN_URL: &str = "https://ramblio.com/login";
const RAMBLIO_HANGOUTS_URL: &str = "https://ramblio.com/hangouts";
const RECONNECT_DELAYS: [Duration; 5] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
    Duration::from_secs(8),
];

const MB_OK: u32 = 0x0000_0000;
const MB_ICONERROR: u32 = 0x0000_0010;
const MB_SETFOREGROUND: u32 = 0x0001_0000;
const NOTIFICATION_KIND_OTHER: i32 = 4;
const NOTIFICATION_PROCESSING_IMPORTANT_MOST_RECENT: i32 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Hangout {
    id: String,
    title: String,
    details: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AppMode {
    Ready,
    Login,
    Background,
    Switching,
}

enum BrowserCommand {
    OpenLogin,
    LoginCompleted(String),
    ResumeBackground,
    RefreshHangouts,
    LoadMoreHangouts,
    JoinHangout(String),
    Stop,
    Shutdown,
}

enum WorkerEvent {
    Status(String),
    Mode(AppMode),
    Hangouts(Vec<Hangout>),
    AccountIdentity(String),
    Alert(String),
}

enum Wake {
    Command(Option<BrowserCommand>),
    Dropped,
}

#[derive(PartialEq, Eq)]
enum LoopControl {
    Continue,
    Exit,
}

enum Interrupt {
    Stop,
    Shutdown,
}

enum Recover {
    Restored(BrowserSession),
    Stopped,
    Shutdown,
    Failed(String),
}

#[derive(Clone)]
struct SessionWatch {
    lost: Arc<AtomicBool>,
    wake: Arc<Notify>,
    intentional: Arc<AtomicBool>,
}

impl SessionWatch {
    fn new() -> Self {
        Self {
            lost: Arc::new(AtomicBool::new(false)),
            wake: Arc::new(Notify::new()),
            intentional: Arc::new(AtomicBool::new(false)),
        }
    }

    fn mark_intentional(&self) {
        self.intentional.store(true, Ordering::SeqCst);
    }

    fn signal_lost(&self) {
        if self.intentional.load(Ordering::SeqCst) {
            return;
        }
        self.lost.store(true, Ordering::SeqCst);
        self.wake.notify_one();
    }

    fn is_unplanned(&self) -> bool {
        !self.intentional.load(Ordering::SeqCst) && self.lost.load(Ordering::SeqCst)
    }
}

struct DisconnectOnDrop(SessionWatch);

impl Drop for DisconnectOnDrop {
    fn drop(&mut self) {
        self.0.signal_lost();
    }
}

struct BrowserSession {
    browser: Browser,
    page: Page,
    handler_task: JoinHandle<()>,
    visible: bool,
    login_monitor: Option<JoinHandle<()>>,
    watch: SessionWatch,
}

struct WorkerCtx {
    profile_dir: PathBuf,
    events: mpsc::Sender<WorkerEvent>,
    monitor_commands: tokio_mpsc::UnboundedSender<BrowserCommand>,
}

struct BrowserWorker {
    commands: Option<tokio_mpsc::UnboundedSender<BrowserCommand>>,
    events: Option<mpsc::Receiver<WorkerEvent>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl BrowserWorker {
    fn new(profile_dir: PathBuf) -> Self {
        let (command_tx, command_rx) = tokio_mpsc::unbounded_channel();
        let monitor_commands = command_tx.clone();
        let (event_tx, event_rx) = mpsc::channel();

        let worker_thread = thread::Builder::new()
            .name("ramblibot-browser-worker".to_owned())
            .spawn(move || {
                let panic_events = event_tx.clone();
                let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    worker_main(profile_dir, event_tx, command_rx, monitor_commands);
                }));
                if result.is_err() {
                    // The panic hook already showed a dialog. Put the window back on Ready
                    // so it does not keep offering actions for a dead browser.
                    let _ = panic_events.send(WorkerEvent::Hangouts(Vec::new()));
                    let _ = panic_events.send(WorkerEvent::Mode(AppMode::Ready));
                    let _ = panic_events.send(WorkerEvent::Status(
                        "Ramblibot stopped unexpectedly. The session was closed.".to_owned(),
                    ));
                }
            })
            .expect("failed to create browser worker thread");

        Self {
            commands: Some(command_tx),
            events: Some(event_rx),
            thread: Some(worker_thread),
        }
    }

    fn command_sender(&self) -> tokio_mpsc::UnboundedSender<BrowserCommand> {
        self.commands
            .as_ref()
            .expect("browser commands are available")
            .clone()
    }

    fn take_event_receiver(&mut self) -> mpsc::Receiver<WorkerEvent> {
        self.events.take().expect("browser events are taken once")
    }

    fn send(&self, command: BrowserCommand) {
        if let Some(commands) = &self.commands {
            let _ = commands.send(command);
        }
    }
}

impl Drop for BrowserWorker {
    fn drop(&mut self) {
        // Stop the receive loop explicitly. Joining after sending Stop deadlocked:
        // the loop kept waiting for commands, and its captured sender kept the
        // channel alive forever.
        self.send(BrowserCommand::Shutdown);
        drop(self.commands.take());
        if let Some(worker_thread) = self.thread.take() {
            let _ = worker_thread.join();
        }
    }
}

fn worker_main(
    profile_dir: PathBuf,
    event_tx: mpsc::Sender<WorkerEvent>,
    command_rx: tokio_mpsc::UnboundedReceiver<BrowserCommand>,
    monitor_commands: tokio_mpsc::UnboundedSender<BrowserCommand>,
) {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            emit_failure(
                &event_tx,
                format!("Could not start browser worker: {error}"),
            );
            return;
        }
    };

    runtime.block_on(session_loop(
        profile_dir,
        event_tx,
        command_rx,
        monitor_commands,
    ));
}

async fn session_loop(
    profile_dir: PathBuf,
    events: mpsc::Sender<WorkerEvent>,
    mut command_rx: tokio_mpsc::UnboundedReceiver<BrowserCommand>,
    monitor_commands: tokio_mpsc::UnboundedSender<BrowserCommand>,
) {
    if let Err(error) = fs::create_dir_all(&profile_dir) {
        emit_failure(
            &events,
            format!("Could not create the Ramblibot profile folder: {error}"),
        );
        return;
    }

    let ctx = WorkerCtx {
        profile_dir,
        events,
        monitor_commands,
    };
    let mut session: Option<BrowserSession> = None;
    let mut joined_room: Option<String> = None;
    let mut pending: VecDeque<BrowserCommand> = VecDeque::new();

    loop {
        let command = if let Some(command) = pending.pop_front() {
            command
        } else {
            match wait_for_command_or_drop(&mut command_rx, &session).await {
                Wake::Dropped => {
                    if recover_if_dropped(
                        &mut session,
                        &mut joined_room,
                        &mut command_rx,
                        &mut pending,
                        &ctx,
                    )
                    .await
                        == LoopControl::Exit
                    {
                        break;
                    }
                    continue;
                }
                Wake::Command(None) => {
                    shutdown_session(&mut session).await;
                    break;
                }
                Wake::Command(Some(command)) => command,
            }
        };

        if matches!(command, BrowserCommand::Shutdown) {
            shutdown_session(&mut session).await;
            break;
        }
        if dispatch(command, &mut session, &mut joined_room, &ctx).await == LoopControl::Exit {
            break;
        }
        // The handler can die while a command is still running. Check after it
        // returns, not only while the loop is waiting for the next command.
        if recover_if_dropped(
            &mut session,
            &mut joined_room,
            &mut command_rx,
            &mut pending,
            &ctx,
        )
        .await
            == LoopControl::Exit
        {
            break;
        }
    }
}

async fn wait_for_command_or_drop(
    command_rx: &mut tokio_mpsc::UnboundedReceiver<BrowserCommand>,
    session: &Option<BrowserSession>,
) -> Wake {
    let Some(active) = session.as_ref() else {
        return Wake::Command(command_rx.recv().await);
    };
    if active.watch.is_unplanned() {
        return Wake::Dropped;
    }
    if active.watch.intentional.load(Ordering::SeqCst) {
        return Wake::Command(command_rx.recv().await);
    }

    let notified = active.watch.wake.notified();
    tokio::pin!(notified);
    // Prefer Stop and Shutdown over a disconnect wake so closing the session
    // is never turned into a reconnect.
    tokio::select! {
        biased;
        command = command_rx.recv() => Wake::Command(command),
        _ = &mut notified => Wake::Dropped,
    }
}

async fn dispatch(
    command: BrowserCommand,
    session: &mut Option<BrowserSession>,
    joined_room: &mut Option<String>,
    ctx: &WorkerCtx,
) -> LoopControl {
    match command {
        BrowserCommand::OpenLogin => {
            if session.is_some() {
                return LoopControl::Continue;
            }
            match open_browser_session(&ctx.profile_dir, true, RAMBLIO_LOGIN_URL, &ctx.events).await
            {
                Ok(opened) => {
                    let mut opened = opened;
                    let page = opened.page.clone();
                    let commands = ctx.monitor_commands.clone();
                    opened.login_monitor = Some(tokio::spawn(async move {
                        let mut email = String::new();
                        loop {
                            if let Ok(value) = page
                                .evaluate_expression(
                                    "document.querySelector('input[type=email]')?.value || ''",
                                )
                                .await
                                && let Ok(current) = value.into_value::<String>()
                                && !current.trim().is_empty()
                            {
                                email = current.trim().to_owned();
                            }
                            let url = page.url().await.unwrap_or_default().unwrap_or_default();
                            if url.starts_with("https://ramblio.com/") && !url.contains("/login") {
                                if let Ok(html) = page.content().await {
                                    let document = Html::parse_document(&html);
                                    let login_link = Selector::parse("a[href*='login']").unwrap();
                                    let email_field =
                                        Selector::parse("input[type='email']").unwrap();
                                    if document.select(&login_link).next().is_none()
                                        && document.select(&email_field).next().is_none()
                                    {
                                        let _ =
                                            commands.send(BrowserCommand::LoginCompleted(email));
                                        break;
                                    }
                                }
                            }
                            sleep(Duration::from_secs(1)).await;
                        }
                    }));
                    *session = Some(opened);
                    emit(&ctx.events, WorkerEvent::Mode(AppMode::Login));
                    emit(
                        &ctx.events,
                        WorkerEvent::Status(
                            "Sign in in the browser. Ramblibot will save the session and continue automatically when sign-in completes.".to_owned(),
                        ),
                    );
                }
                Err(error) => {
                    emit(&ctx.events, WorkerEvent::Mode(AppMode::Ready));
                    emit(&ctx.events, WorkerEvent::Status(error));
                }
            }
        }
        BrowserCommand::LoginCompleted(identity) => {
            let Some(mut visible_session) = session.take() else {
                return LoopControl::Continue;
            };
            if !visible_session.visible {
                *session = Some(visible_session);
                return LoopControl::Continue;
            }
            let identity = if identity.trim().is_empty() {
                "Ramblio account".to_owned()
            } else {
                identity
            };
            if let Some(monitor) = visible_session.login_monitor.take() {
                monitor.abort();
            }
            close_browser_session(&mut visible_session).await;
            emit(&ctx.events, WorkerEvent::AccountIdentity(identity));
            emit(&ctx.events, WorkerEvent::Mode(AppMode::Switching));
            emit(
                &ctx.events,
                WorkerEvent::Status(
                    "Sign-in detected. Saving the session and opening hangouts…".to_owned(),
                ),
            );
            match open_browser_session(&ctx.profile_dir, false, RAMBLIO_HANGOUTS_URL, &ctx.events)
                .await
            {
                Ok(background_session) => {
                    let (hangouts, authenticated) = fetch_hangouts(&background_session.page).await;
                    if !authenticated {
                        let mut background_session = background_session;
                        close_browser_session(&mut background_session).await;
                        emit(&ctx.events, WorkerEvent::Mode(AppMode::Ready));
                        emit(
                            &ctx.events,
                            WorkerEvent::Status(
                                "Ramblio sign-in was not confirmed. Open the sign-in browser and try again.".to_owned(),
                            ),
                        );
                    } else {
                        *session = Some(background_session);
                        emit(&ctx.events, WorkerEvent::Hangouts(hangouts));
                        emit(&ctx.events, WorkerEvent::Mode(AppMode::Background));
                        emit(
                            &ctx.events,
                            WorkerEvent::Status("Signed in. Choose a hangout to join.".to_owned()),
                        );
                    }
                }
                Err(error) => {
                    emit(&ctx.events, WorkerEvent::Mode(AppMode::Ready));
                    emit(&ctx.events, WorkerEvent::Status(error));
                }
            }
        }
        BrowserCommand::ResumeBackground => {
            if session.is_some() {
                return LoopControl::Continue;
            }
            emit(&ctx.events, WorkerEvent::Mode(AppMode::Switching));
            emit(
                &ctx.events,
                WorkerEvent::Status("Connecting to the saved Ramblio session…".to_owned()),
            );
            match open_browser_session(&ctx.profile_dir, false, RAMBLIO_HANGOUTS_URL, &ctx.events)
                .await
            {
                Ok(background_session) => {
                    let (hangouts, authenticated) = fetch_hangouts(&background_session.page).await;
                    if authenticated {
                        *session = Some(background_session);
                        emit(&ctx.events, WorkerEvent::Hangouts(hangouts));
                        emit(&ctx.events, WorkerEvent::Mode(AppMode::Background));
                        emit(
                            &ctx.events,
                            WorkerEvent::Status(
                                "Connected to the saved session. Choose a hangout to join."
                                    .to_owned(),
                            ),
                        );
                    } else {
                        let mut expired = background_session;
                        close_browser_session(&mut expired).await;
                        emit(&ctx.events, WorkerEvent::Mode(AppMode::Ready));
                        emit(
                            &ctx.events,
                            WorkerEvent::Status(
                                "The saved Ramblio session has expired. Open the sign-in browser to sign in again.".to_owned(),
                            ),
                        );
                    }
                }
                Err(error) => {
                    emit(&ctx.events, WorkerEvent::Mode(AppMode::Ready));
                    emit(&ctx.events, WorkerEvent::Status(error));
                }
            }
        }
        BrowserCommand::RefreshHangouts => {
            if let Some(active) = session.as_mut().filter(|active| !active.visible) {
                match refresh_hangouts(&active.page).await {
                    Ok(hangouts) => {
                        let count = hangouts.len();
                        emit(&ctx.events, WorkerEvent::Hangouts(hangouts));
                        emit(
                            &ctx.events,
                            WorkerEvent::Status(format!(
                                "Hangout list refreshed. {count} available."
                            )),
                        );
                    }
                    Err(error) => emit(&ctx.events, WorkerEvent::Status(error)),
                }
            }
        }
        BrowserCommand::LoadMoreHangouts => {
            if let Some(active) = session.as_mut().filter(|active| !active.visible) {
                match load_more_hangouts(&active.page).await {
                    Ok(hangouts) => {
                        let count = hangouts.len();
                        emit(&ctx.events, WorkerEvent::Hangouts(hangouts));
                        emit(
                            &ctx.events,
                            WorkerEvent::Status(format!("Hangout list expanded. {count} listed.")),
                        );
                    }
                    Err(error) => emit(&ctx.events, WorkerEvent::Status(error)),
                }
            }
        }
        BrowserCommand::JoinHangout(id) => {
            if let Some(active) = session.as_mut().filter(|active| !active.visible) {
                match join_hangout(&active.page, &id).await {
                    Ok(title) => {
                        *joined_room = Some(id);
                        emit(
                            &ctx.events,
                            WorkerEvent::Status(format!(
                                "Ramblio connection started for {title}. The background session stays open, with audio output muted."
                            )),
                        );
                    }
                    Err(error) => emit(&ctx.events, WorkerEvent::Status(error)),
                }
            }
        }
        BrowserCommand::Stop => {
            *joined_room = None;
            if let Some(mut active) = session.take() {
                close_browser_session(&mut active).await;
            }
            emit(&ctx.events, WorkerEvent::Hangouts(Vec::new()));
            emit(&ctx.events, WorkerEvent::Mode(AppMode::Ready));
            emit(&ctx.events, WorkerEvent::Status("Stopped.".to_owned()));
        }
        BrowserCommand::Shutdown => {
            shutdown_session(session).await;
            return LoopControl::Exit;
        }
    }
    LoopControl::Continue
}

async fn recover_if_dropped(
    session: &mut Option<BrowserSession>,
    joined_room: &mut Option<String>,
    command_rx: &mut tokio_mpsc::UnboundedReceiver<BrowserCommand>,
    pending: &mut VecDeque<BrowserCommand>,
    ctx: &WorkerCtx,
) -> LoopControl {
    let Some(mut dropped) = take_unplanned(session) else {
        return LoopControl::Continue;
    };
    let visible = dropped.visible;
    close_browser_session(&mut dropped).await;
    if visible {
        *joined_room = None;
        emit_failure(
            &ctx.events,
            "The sign-in browser closed unexpectedly.".to_owned(),
        );
        return LoopControl::Continue;
    }

    match reconnect(joined_room, command_rx, pending, ctx).await {
        Recover::Restored(active) => {
            *session = Some(active);
            LoopControl::Continue
        }
        Recover::Stopped => {
            *joined_room = None;
            emit(&ctx.events, WorkerEvent::Hangouts(Vec::new()));
            emit(&ctx.events, WorkerEvent::Mode(AppMode::Ready));
            emit(&ctx.events, WorkerEvent::Status("Stopped.".to_owned()));
            LoopControl::Continue
        }
        Recover::Shutdown => LoopControl::Exit,
        Recover::Failed(message) => {
            *joined_room = None;
            emit_failure(&ctx.events, message);
            LoopControl::Continue
        }
    }
}

fn take_unplanned(session: &mut Option<BrowserSession>) -> Option<BrowserSession> {
    let unplanned = session
        .as_ref()
        .is_some_and(|active| active.watch.is_unplanned());
    if unplanned { session.take() } else { None }
}

async fn reconnect(
    joined_room: &Option<String>,
    command_rx: &mut tokio_mpsc::UnboundedReceiver<BrowserCommand>,
    pending: &mut VecDeque<BrowserCommand>,
    ctx: &WorkerCtx,
) -> Recover {
    let mut last_error = "Could not restore the Ramblio connection.".to_owned();
    for (attempt, delay) in RECONNECT_DELAYS.iter().copied().enumerate() {
        let attempt_number = attempt + 1;
        let message = if attempt_number == 1 {
            "Connection dropped. Reconnecting…".to_owned()
        } else {
            format!("Connection dropped. Reconnecting (attempt {attempt_number} of 5)…")
        };
        emit(&ctx.events, WorkerEvent::Status(message));

        match sleep_unless_interrupted(delay, command_rx, pending).await {
            Some(Interrupt::Stop) => return Recover::Stopped,
            Some(Interrupt::Shutdown) => return Recover::Shutdown,
            None => {}
        }
        if let Some(interrupt) = pull_interrupt(command_rx, pending) {
            return match interrupt {
                Interrupt::Stop => Recover::Stopped,
                Interrupt::Shutdown => Recover::Shutdown,
            };
        }

        match try_restore(joined_room, ctx).await {
            Ok(active) => return Recover::Restored(active),
            Err(error) if is_terminal_session_error(&error) => return Recover::Failed(error),
            Err(error) => last_error = error,
        }
    }
    if let Some(interrupt) = pull_interrupt(command_rx, pending) {
        return match interrupt {
            Interrupt::Stop => Recover::Stopped,
            Interrupt::Shutdown => Recover::Shutdown,
        };
    }
    Recover::Failed(format!(
        "Could not restore the Ramblio connection after it dropped. {last_error}"
    ))
}

async fn try_restore(
    joined_room: &Option<String>,
    ctx: &WorkerCtx,
) -> Result<BrowserSession, String> {
    let opened =
        open_browser_session(&ctx.profile_dir, false, RAMBLIO_HANGOUTS_URL, &ctx.events).await?;
    if let Some(id) = joined_room {
        match join_hangout(&opened.page, id).await {
            Ok(title) => {
                emit(&ctx.events, WorkerEvent::Mode(AppMode::Background));
                emit(
                    &ctx.events,
                    WorkerEvent::Status(format!("Connection restored. Rejoined {title}.")),
                );
                Ok(opened)
            }
            Err(error) => {
                let mut opened = opened;
                close_browser_session(&mut opened).await;
                Err(error)
            }
        }
    } else {
        let (hangouts, authenticated) = fetch_hangouts(&opened.page).await;
        // fetch_hangouts reports a failed navigation as signed out. A live page
        // that still shows the login form is the expired-session case; a dead
        // page is a drop and should be retried.
        let page_responded = opened.page.url().await.is_ok();
        if !authenticated {
            let mut opened = opened;
            close_browser_session(&mut opened).await;
            if page_responded {
                Err(
                    "The saved Ramblio session has expired. Open the sign-in browser to sign in again."
                        .to_owned(),
                )
            } else {
                Err("Could not read the restored Ramblio session.".to_owned())
            }
        } else {
            let count = hangouts.len();
            emit(&ctx.events, WorkerEvent::Hangouts(hangouts));
            emit(&ctx.events, WorkerEvent::Mode(AppMode::Background));
            emit(
                &ctx.events,
                WorkerEvent::Status(format!(
                    "Connection restored. Hangout list refreshed. {count} available."
                )),
            );
            Ok(opened)
        }
    }
}

fn is_terminal_session_error(error: &str) -> bool {
    error.contains("sign-in is required")
        || error.contains("session has expired")
        || error.contains("sign-in was not confirmed")
}

async fn sleep_unless_interrupted(
    delay: Duration,
    command_rx: &mut tokio_mpsc::UnboundedReceiver<BrowserCommand>,
    pending: &mut VecDeque<BrowserCommand>,
) -> Option<Interrupt> {
    let deadline = Instant::now() + delay;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        tokio::select! {
            _ = sleep(remaining) => return None,
            command = command_rx.recv() => {
                match command {
                    Some(BrowserCommand::Stop) => return Some(Interrupt::Stop),
                    Some(BrowserCommand::Shutdown) | None => return Some(Interrupt::Shutdown),
                    Some(other) => pending.push_back(other),
                }
            }
        }
    }
}

fn pull_interrupt(
    command_rx: &mut tokio_mpsc::UnboundedReceiver<BrowserCommand>,
    pending: &mut VecDeque<BrowserCommand>,
) -> Option<Interrupt> {
    loop {
        match command_rx.try_recv() {
            Ok(BrowserCommand::Stop) => return Some(Interrupt::Stop),
            Ok(BrowserCommand::Shutdown) => return Some(Interrupt::Shutdown),
            Ok(command) => pending.push_back(command),
            Err(TryRecvError::Empty) => return None,
            Err(TryRecvError::Disconnected) => return Some(Interrupt::Shutdown),
        }
    }
}

async fn shutdown_session(session: &mut Option<BrowserSession>) {
    if let Some(mut active) = session.take() {
        close_browser_session(&mut active).await;
    }
}

fn emit(events: &mpsc::Sender<WorkerEvent>, event: WorkerEvent) {
    let _ = events.send(event);
}

fn emit_failure(events: &mpsc::Sender<WorkerEvent>, message: String) {
    emit(events, WorkerEvent::Hangouts(Vec::new()));
    emit(events, WorkerEvent::Mode(AppMode::Ready));
    emit(events, WorkerEvent::Status(message.clone()));
    emit(events, WorkerEvent::Alert(message));
}

fn profile_dir() -> PathBuf {
    env::var_os("APPDATA")
        .map(PathBuf::from)
        .map(|path| path.join("Ramblibot").join("ChromiumProfile"))
        .unwrap_or_else(|| {
            ProjectDirs::from("com", "Ramblibot", "Ramblibot")
                .expect("could not locate the application data directory")
                .data_dir()
                .join("ChromiumProfile")
        })
}

fn identity_file() -> PathBuf {
    env::var_os("APPDATA")
        .map(PathBuf::from)
        .map(|path| path.join("Ramblibot").join("account_label.txt"))
        .unwrap_or_else(|| {
            ProjectDirs::from("com", "Ramblibot", "Ramblibot")
                .expect("could not locate the application data directory")
                .data_dir()
                .join("account_label.txt")
        })
}

async fn open_browser_session(
    data_dir: &std::path::Path,
    visible: bool,
    start_url: &str,
    events: &mpsc::Sender<WorkerEvent>,
) -> Result<BrowserSession, String> {
    let _ = events.send(WorkerEvent::Status(if visible {
        "Starting sign-in browser…".to_owned()
    } else {
        "Starting hidden browser session…".to_owned()
    }));

    let builder = BrowserConfig::builder().user_data_dir(data_dir);
    let config = if visible {
        builder.with_head().build()
    } else {
        builder.new_headless_mode().build()
    }
    .map_err(|error| format!("Could not configure Chromium: {error}"))?;

    let (mut browser, mut handler) = Browser::launch(config)
        .await
        .map_err(|error| format!("Could not launch Chromium: {error}"))?;

    let watch = SessionWatch::new();
    let handler_watch = watch.clone();
    let handler_task = tokio::spawn(async move {
        let _guard = DisconnectOnDrop(handler_watch);
        while let Some(event) = handler.next().await {
            if event.is_err() {
                break;
            }
        }
    });

    match browser.new_page(start_url).await {
        Ok(page) => Ok(BrowserSession {
            browser,
            page,
            handler_task,
            visible,
            login_monitor: None,
            watch,
        }),
        Err(error) => {
            watch.mark_intentional();
            let _ = browser.close().await;
            let _ = browser.wait().await;
            handler_task.abort();
            Err(format!("Could not open Ramblio: {error}"))
        }
    }
}

async fn close_browser_session(session: &mut BrowserSession) {
    // Set this before aborting the handler so its exit is not treated as a drop.
    session.watch.mark_intentional();
    if let Some(monitor) = session.login_monitor.take() {
        monitor.abort();
    }
    // Browser shutdown must not hold the UI's close path indefinitely if
    // Chromium has stopped responding. The profile directory is deliberately
    // left untouched so its cookies and saved sign-in remain available.
    let _ = timeout(Duration::from_secs(2), session.browser.close()).await;
    let _ = timeout(Duration::from_secs(2), session.browser.wait()).await;
    session.handler_task.abort();
}

async fn refresh_hangouts(page: &Page) -> Result<Vec<Hangout>, String> {
    page.goto(RAMBLIO_HANGOUTS_URL)
        .await
        .map_err(|error| format!("Could not open Ramblio hangouts: {error}"))?;
    sleep(Duration::from_millis(500)).await;
    let html = page
        .content()
        .await
        .map_err(|error| format!("Could not read the hangout list: {error}"))?;
    Ok(parse_hangouts(&html))
}

async fn load_more_hangouts(page: &Page) -> Result<Vec<Hangout>, String> {
    let buttons = page
        .find_elements("button, [role='button']")
        .await
        .map_err(|error| format!("Could not inspect hangout list controls: {error}"))?;
    for button in buttons {
        let text = button.inner_text().await.ok().flatten().unwrap_or_default();
        let label = button
            .attribute("aria-label")
            .await
            .ok()
            .flatten()
            .unwrap_or_default();
        if normalize_text(format!("{text} {label}")).eq_ignore_ascii_case("load more") {
            button
                .click()
                .await
                .map_err(|error| format!("Could not load more hangouts: {error}"))?;
            sleep(Duration::from_millis(500)).await;
            let html = page
                .content()
                .await
                .map_err(|error| format!("Could not read the expanded hangout list: {error}"))?;
            return Ok(parse_hangouts(&html));
        }
    }
    Err("There are no more hangouts to load.".to_owned())
}

async fn fetch_hangouts(page: &Page) -> (Vec<Hangout>, bool) {
    if page.goto(RAMBLIO_HANGOUTS_URL).await.is_err() {
        return (Vec::new(), false);
    }
    sleep(Duration::from_millis(900)).await;
    let html = match page.content().await {
        Ok(html) => html,
        Err(_) => return (Vec::new(), false),
    };
    let document = Html::parse_document(&html);
    let login_link = Selector::parse("a[href*='login']").unwrap();
    let email_field = Selector::parse("input[type='email']").unwrap();
    let authenticated = document.select(&login_link).next().is_none()
        && document.select(&email_field).next().is_none();
    (parse_hangouts(&html), authenticated)
}

fn parse_hangouts(html: &str) -> Vec<Hangout> {
    let document = Html::parse_document(html);
    let anchors = Selector::parse("a[href]").expect("static anchor selector is valid");
    let headings = Selector::parse("h2").expect("static heading selector is valid");
    let mut seen = HashSet::new();
    let mut hangouts = Vec::new();

    for anchor in document.select(&anchors) {
        let Some(href) = anchor.value().attr("href") else {
            continue;
        };
        let Some(path) = href
            .strip_prefix("./hangouts/")
            .or_else(|| href.strip_prefix("/hangouts/"))
        else {
            continue;
        };
        let id = path.split(['/', '?', '#']).next().unwrap_or_default();
        if !is_room_id(id) || !seen.insert(id.to_owned()) {
            continue;
        }

        let title = anchor
            .select(&headings)
            .next()
            .map(|heading| normalize_text(heading.text().collect::<String>()))
            .filter(|title| !title.is_empty())
            .unwrap_or_else(|| normalize_text(anchor.text().collect::<String>()));
        if title.is_empty() {
            continue;
        }

        hangouts.push(Hangout {
            id: id.to_owned(),
            title,
            details: normalize_text(anchor.text().collect::<String>()),
        });
    }

    hangouts
}

fn is_room_id(value: &str) -> bool {
    value.len() == 36
        && value
            .chars()
            .all(|character| character.is_ascii_hexdigit() || character == '-')
}

fn normalize_text(text: String) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

async fn join_hangout(page: &Page, id: &str) -> Result<String, String> {
    if !is_room_id(id) {
        return Err(
            "That hangout selection is invalid. Refresh the list and try again.".to_owned(),
        );
    }

    let room = page
        .goto(format!("https://ramblio.com/hangouts/{id}"))
        .await
        .map_err(|error| format!("Could not open the selected hangout: {error}"))?;
    let headings = Selector::parse("h1, h2").expect("static room title selector is valid");
    let title = room
        .content()
        .await
        .ok()
        .and_then(|html| {
            let document = Html::parse_document(&html);
            document
                .select(&headings)
                .next()
                .map(|heading| normalize_text(heading.text().collect::<String>()))
        })
        .filter(|title| !title.is_empty())
        .unwrap_or_else(|| "selected hangout".to_owned());

    // Ramblio renders some room controls after the initial document load. Poll
    // the live DOM briefly instead of giving up after a fixed 500 ms delay.
    let mut observed_controls = Vec::new();
    for _ in 0..40 {
        let controls = page
            .find_elements("button, [role='button'], input[type='submit'], a[href], [onclick]")
            .await
            .map_err(|error| format!("Could not inspect hangout controls: {error}"))?;
        for control in controls {
            let text = control
                .inner_text()
                .await
                .ok()
                .flatten()
                .unwrap_or_default();
            let aria = control
                .attribute("aria-label")
                .await
                .ok()
                .flatten()
                .unwrap_or_default();
            let title_attr = control
                .attribute("title")
                .await
                .ok()
                .flatten()
                .unwrap_or_default();
            let value = control
                .attribute("value")
                .await
                .ok()
                .flatten()
                .unwrap_or_default();
            let href = control
                .attribute("href")
                .await
                .ok()
                .flatten()
                .unwrap_or_default();
            let label = normalize_text(format!("{text} {aria} {title_attr} {value}"));
            if !label.is_empty()
                && observed_controls.len() < 8
                && !observed_controls.contains(&label)
            {
                observed_controls.push(label.clone());
            }
            let lower_label = label.to_ascii_lowercase();
            if lower_label.contains("join")
                || lower_label.contains("connect to audio")
                || lower_label.contains("connect audio")
            {
                control.click().await.map_err(|error| {
                    format!("Could not activate Ramblio's Join control: {error}")
                })?;
                return Ok(title);
            }
            // Some versions of the site expose the action as a link instead of
            // a button, so include its accessible name and URL in the match.
            if href.to_ascii_lowercase().contains("join") {
                control
                    .click()
                    .await
                    .map_err(|error| format!("Could not activate Ramblio's Join link: {error}"))?;
                return Ok(title);
            }
        }
        sleep(Duration::from_millis(250)).await;
    }

    let html = page
        .content()
        .await
        .map_err(|error| format!("Could not inspect the selected hangout: {error}"))?;
    if html.to_ascii_lowercase().contains("sign in required") {
        return Err(
            "Ramblio says sign-in is required. Sign in again, then refresh hangouts.".to_owned(),
        );
    }
    let found = if observed_controls.is_empty() {
        "No buttons or links were exposed on the room page.".to_owned()
    } else {
        format!("Controls found: {}.", observed_controls.join("; "))
    };
    let final_url = page
        .url()
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| "unknown".to_owned());
    let page_summary = page
        .evaluate_expression(
            "(document.querySelector('main')?.innerText || document.body?.innerText || '').trim().replace(/\\s+/g, ' ').slice(0, 240)",
        )
        .await
        .ok()
        .and_then(|value| value.into_value::<String>().ok())
        .filter(|summary| !summary.is_empty())
        .map(|summary| format!(" Page text: {summary}"))
        .unwrap_or_default();
    Err(format!(
        "Ramblio's room page did not expose a Join control. {found} Final page: {final_url}.{page_summary}"
    ))
}

struct UiState {
    mode: AppMode,
    hangouts: Vec<Hangout>,
    account_identity: Option<String>,
    commands: tokio_mpsc::UnboundedSender<BrowserCommand>,
    events: mpsc::Receiver<WorkerEvent>,
    window: Window,
    open_login: Button,
    sign_in_as: Button,
    refresh: Button,
    load_more: Button,
    stop: Button,
    empty: Label,
    list: ListBox,
    details: Label,
    status_label: Label,
}

impl UiState {
    fn apply_controls(&self) {
        let ready = self.mode == AppMode::Ready;
        let background = self.mode == AppMode::Background;
        self.open_login.set_enabled(ready);
        self.sign_in_as.set_enabled(ready);
        self.sign_in_as.set_visible(self.account_identity.is_some());
        self.refresh.set_enabled(background);
        self.load_more
            .set_enabled(background && !self.hangouts.is_empty());
        self.stop.set_enabled(self.mode != AppMode::Ready);
        self.empty
            .set_visible(background && self.hangouts.is_empty());
    }

    fn set_status(&self, message: String) {
        let text = format!("Status: {message}");
        self.status_label.set_text(&text);
        self.status_label.set_a11y_name(&text);
        raise_status_notification(self.status_label, &text);
    }

    fn set_hangouts(&mut self, hangouts: Vec<Hangout>) {
        self.hangouts = hangouts;
        let items: Vec<String> = self
            .hangouts
            .iter()
            .map(|hangout| format!("Join hangout: {}", hangout.title))
            .collect();
        self.list.set_items(&items);
        self.list.set_selected(None);
        self.show_details(None);
        self.apply_controls();
    }

    fn show_details(&self, index: Option<usize>) {
        let details = index
            .and_then(|index| self.hangouts.get(index))
            .filter(|hangout| hangout.details != hangout.title)
            .map(|hangout| hangout.details.clone())
            .unwrap_or_default();
        self.details.set_text(&details);
        self.details.set_visible(!details.is_empty());
    }

    fn set_identity(&mut self, identity: String) {
        let path = identity_file();
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        if fs::write(path, &identity).is_ok() {
            self.sign_in_as.set_text(&format!("Sign in as {identity}"));
            self.account_identity = Some(identity);
            self.apply_controls();
        }
    }

    fn drain_events(&mut self) -> Vec<String> {
        let mut alerts = Vec::new();
        while let Ok(event) = self.events.try_recv() {
            match event {
                WorkerEvent::Status(message) => self.set_status(message),
                WorkerEvent::Mode(mode) => {
                    self.mode = mode;
                    self.apply_controls();
                }
                WorkerEvent::Hangouts(hangouts) => self.set_hangouts(hangouts),
                WorkerEvent::AccountIdentity(identity) => self.set_identity(identity),
                WorkerEvent::Alert(message) => alerts.push(message),
            }
        }
        alerts
    }
}

fn install_panic_hook() {
    std::panic::set_hook(Box::new(|info| {
        show_windows_message(&format!("Ramblibot stopped unexpectedly.\n\n{info}"));
    }));
}

fn show_windows_message(message: &str) {
    let text = wide(message);
    let caption = wide("Ramblibot");
    unsafe {
        MessageBoxW(
            std::ptr::null_mut(),
            text.as_ptr(),
            caption.as_ptr(),
            MB_OK | MB_ICONERROR | MB_SETFOREGROUND,
        );
    }
}

fn wide(value: &str) -> Vec<u16> {
    value.encode_utf16().chain(std::iter::once(0)).collect()
}

fn raise_status_notification(label: Label, text: &str) {
    let Some(NativeHandle::Win32(hwnd)) = label.native_handle() else {
        return;
    };
    unsafe {
        raise_uia_notification(hwnd as *mut c_void, text);
    }
}

unsafe fn raise_uia_notification(hwnd: *mut c_void, text: &str) {
    unsafe {
        let mut provider: *mut c_void = std::ptr::null_mut();
        if UiaHostProviderFromHwnd(hwnd, &mut provider) < 0 || provider.is_null() {
            return;
        }
        let display = wide(text);
        let bstr = SysAllocString(display.as_ptr());
        if !bstr.is_null() {
            let _ = UiaRaiseNotificationEvent(
                provider,
                NOTIFICATION_KIND_OTHER,
                NOTIFICATION_PROCESSING_IMPORTANT_MOST_RECENT,
                bstr,
                std::ptr::null(),
            );
            SysFreeString(bstr);
        }
        release_unknown(provider);
    }
}

unsafe fn release_unknown(unknown: *mut c_void) {
    unsafe {
        if unknown.is_null() {
            return;
        }
        let vtable = *(unknown as *const *const usize);
        let release: unsafe extern "system" fn(*mut c_void) -> u32 =
            std::mem::transmute(*vtable.add(2));
        release(unknown);
    }
}

#[link(name = "user32")]
unsafe extern "system" {
    fn MessageBoxW(hwnd: *mut c_void, text: *const u16, caption: *const u16, typ: u32) -> i32;
}

#[link(name = "oleaut32")]
unsafe extern "system" {
    fn SysAllocString(psz: *const u16) -> *mut u16;
    fn SysFreeString(bstr: *mut u16);
}

#[link(name = "uiautomationcore")]
unsafe extern "system" {
    fn UiaHostProviderFromHwnd(hwnd: *mut c_void, provider: *mut *mut c_void) -> i32;
    fn UiaRaiseNotificationEvent(
        provider: *mut c_void,
        notification_kind: i32,
        notification_processing: i32,
        display_string: *const u16,
        activity_id: *const u16,
    ) -> i32;
}

fn main() {
    install_panic_hook();
    let app = match App::new("Ramblibot") {
        Ok(app) => app,
        Err(error) => {
            show_windows_message(&format!("Could not open the Ramblibot window: {error}"));
            return;
        }
    };

    let window = Window::new("Ramblibot");
    if !window.is_alive() {
        let detail = rungui::last_error()
            .map(|error| error.to_string())
            .unwrap_or_else(|| "unknown error".to_owned());
        show_windows_message(&format!("Could not open the Ramblibot window: {detail}"));
        return;
    }
    window.set_size(720, 420);
    window.set_min_size(640, 360);

    let column = VBox::new(window);
    column.set_padding(12);
    column.set_spacing(8);
    Label::new(column, "Ramblibot");
    Label::new(
        column,
        "Sign in once, then manage a background hangout session.",
    );

    let row = HBox::new(column);
    row.set_spacing(8);
    let open_login = Button::new(row, "Open sign-in browser");
    let sign_in_as = Button::new(row, "Sign in as");
    let refresh = Button::new(row, "Refresh hangouts");
    let load_more = Button::new(row, "Load more hangouts");
    let stop = Button::new(row, "Stop session");

    Label::new(column, "Available hangouts");
    let empty = Label::new(
        column,
        "No hangouts are listed yet. Refresh after signing in.",
    );
    let list = ListBox::new(column);
    list.set_expand(1.0);
    list.set_min_size(0, 160);
    list.set_a11y_name("Available hangouts");
    let details = Label::new(column, "");
    details.set_visible(false);

    let account_identity = fs::read_to_string(identity_file())
        .ok()
        .map(|identity| identity.trim().to_owned())
        .filter(|identity| !identity.is_empty());
    let initial_status = if account_identity.is_some() {
        "Saved Ramblio session found. Select Sign in as to reconnect.".to_owned()
    } else {
        "Ready. Open the sign-in browser to begin.".to_owned()
    };
    let status_label = Label::new(column, &format!("Status: {initial_status}"));
    status_label.set_a11y_name(&format!("Status: {initial_status}"));
    Label::new(
        column,
        "Your Chromium profile and saved account label are stored under %APPDATA%\\Ramblibot. The browser closes automatically after sign-in.",
    );

    if let Some(identity) = &account_identity {
        sign_in_as.set_text(&format!("Sign in as {identity}"));
    }

    let mut worker = BrowserWorker::new(profile_dir());
    let state = Rc::new(std::cell::RefCell::new(UiState {
        mode: AppMode::Ready,
        hangouts: Vec::new(),
        account_identity,
        commands: worker.command_sender(),
        events: worker.take_event_receiver(),
        window,
        open_login,
        sign_in_as,
        refresh,
        load_more,
        stop,
        empty,
        list,
        details,
        status_label,
    }));
    state.borrow().apply_controls();
    raise_status_notification(status_label, &format!("Status: {initial_status}"));

    {
        let state = state.clone();
        open_login.on_click(move || {
            let Ok(ui) = state.try_borrow_mut() else {
                return;
            };
            if ui.mode != AppMode::Ready {
                return;
            }
            ui.set_status("Opening Ramblio sign-in…".to_owned());
            let _ = ui.commands.send(BrowserCommand::OpenLogin);
        });
    }
    {
        let state = state.clone();
        sign_in_as.on_click(move || {
            let Ok(mut ui) = state.try_borrow_mut() else {
                return;
            };
            if ui.mode != AppMode::Ready {
                return;
            }
            let Some(identity) = ui.account_identity.clone() else {
                return;
            };
            ui.set_hangouts(Vec::new());
            ui.set_status(format!("Connecting as {identity}…"));
            let _ = ui.commands.send(BrowserCommand::ResumeBackground);
        });
    }
    {
        let state = state.clone();
        refresh.on_click(move || {
            let Ok(ui) = state.try_borrow_mut() else {
                return;
            };
            if ui.mode != AppMode::Background {
                return;
            }
            ui.set_status("Refreshing available hangouts…".to_owned());
            let _ = ui.commands.send(BrowserCommand::RefreshHangouts);
        });
    }
    {
        let state = state.clone();
        load_more.on_click(move || {
            let Ok(ui) = state.try_borrow_mut() else {
                return;
            };
            if ui.mode != AppMode::Background || ui.hangouts.is_empty() {
                return;
            }
            ui.set_status("Loading more hangouts…".to_owned());
            let _ = ui.commands.send(BrowserCommand::LoadMoreHangouts);
        });
    }
    {
        let state = state.clone();
        stop.on_click(move || {
            let Ok(ui) = state.try_borrow_mut() else {
                return;
            };
            if ui.mode == AppMode::Ready {
                return;
            }
            let _ = ui.commands.send(BrowserCommand::Stop);
        });
    }
    {
        let state = state.clone();
        list.on_select(move |index| {
            let Ok(ui) = state.try_borrow() else {
                return;
            };
            ui.show_details(index);
        });
    }
    {
        let state = state.clone();
        list.on_activate(move |index| {
            let Ok(ui) = state.try_borrow() else {
                return;
            };
            if ui.mode != AppMode::Background {
                return;
            }
            let Some(hangout) = ui.hangouts.get(index).cloned() else {
                return;
            };
            ui.set_status(format!("Joining {}…", hangout.title));
            let _ = ui.commands.send(BrowserCommand::JoinHangout(hangout.id));
        });
    }
    let state_timer = state.clone();
    let _event_timer = Timer::every(250, move || {
        let alerts = {
            let Ok(mut ui) = state_timer.try_borrow_mut() else {
                return;
            };
            ui.drain_events()
        };
        if alerts.is_empty() {
            return;
        }
        let Ok(window) = state_timer.try_borrow().map(|ui| ui.window) else {
            return;
        };
        for message in alerts {
            message_box(
                Some(window),
                MessageKind::Error,
                Buttons::Ok,
                "Ramblibot",
                &message,
            );
        }
    });

    window.show();
    app.run();
    drop(state);
    drop(worker);
}
