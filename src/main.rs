#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]

use std::{collections::HashSet, env, fs, path::PathBuf, sync::mpsc, thread, time::Duration};

use chromiumoxide::{Browser, BrowserConfig, Page};
use directories::ProjectDirs;
use eframe::egui;
use futures::StreamExt;
use scraper::{Html, Selector};
use tokio::{
    sync::mpsc as tokio_mpsc,
    task::JoinHandle,
    time::{sleep, timeout},
};

const RAMBLIO_LOGIN_URL: &str = "https://ramblio.com/login";
const RAMBLIO_HANGOUTS_URL: &str = "https://ramblio.com/hangouts";

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
}

struct BrowserSession {
    browser: Browser,
    page: Page,
    handler_task: JoinHandle<()>,
    visible: bool,
    login_monitor: Option<JoinHandle<()>>,
}

struct BrowserWorker {
    commands: Option<tokio_mpsc::UnboundedSender<BrowserCommand>>,
    events: mpsc::Receiver<WorkerEvent>,
    thread: Option<thread::JoinHandle<()>>,
}

impl BrowserWorker {
    fn new(profile_dir: PathBuf) -> Self {
        let (command_tx, mut command_rx) = tokio_mpsc::unbounded_channel();
        let monitor_commands = command_tx.clone();
        let (event_tx, event_rx) = mpsc::channel();

        let worker_thread = thread::Builder::new()
            .name("ramblibot-browser-worker".to_owned())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = event_tx.send(WorkerEvent::Status(format!(
                            "Could not start browser worker: {error}"
                        )));
                        return;
                    }
                };

                runtime.block_on(async move {
                    if let Err(error) = fs::create_dir_all(&profile_dir) {
                        let _ = event_tx.send(WorkerEvent::Status(format!(
                            "Could not create the Ramblibot profile folder: {error}"
                        )));
                        return;
                    }

                    let mut session: Option<BrowserSession> = None;
                    while let Some(command) = command_rx.recv().await {
                        match command {
                            BrowserCommand::OpenLogin => {
                                if session.is_some() {
                                    continue;
                                }
                                match open_browser_session(
                                    &profile_dir,
                                    true,
                                    RAMBLIO_LOGIN_URL,
                                    &event_tx,
                                )
                                .await
                                {
                                    Ok(opened) => {
                                        let mut opened = opened;
                                        let page = opened.page.clone();
                                        let commands = monitor_commands.clone();
                                        opened.login_monitor = Some(tokio::spawn(async move {
                                            let mut email = String::new();
                                            loop {
                                                if let Ok(value) = page
                                                    .evaluate_expression("document.querySelector('input[type=email]')?.value || ''")
                                                    .await
                                                    && let Ok(current) = value.into_value::<String>()
                                                    && !current.trim().is_empty()
                                                {
                                                    email = current.trim().to_owned();
                                                }
                                                let url = page.url().await.unwrap_or_default().unwrap_or_default();
                                                if url.starts_with("https://ramblio.com/")
                                                    && !url.contains("/login")
                                                {
                                                    if let Ok(html) = page.content().await {
                                                        let document = Html::parse_document(&html);
                                                        let login_link = Selector::parse("a[href*='login']").unwrap();
                                                        let email_field = Selector::parse("input[type='email']").unwrap();
                                                        if document.select(&login_link).next().is_none()
                                                            && document.select(&email_field).next().is_none()
                                                        {
                                                            let _ = commands.send(BrowserCommand::LoginCompleted(email));
                                                            break;
                                                        }
                                                    }
                                                }
                                                sleep(Duration::from_secs(1)).await;
                                            }
                                        }));
                                        session = Some(opened);
                                        let _ = event_tx.send(WorkerEvent::Mode(AppMode::Login));
                                        let _ = event_tx.send(WorkerEvent::Status(
                                            "Sign in in the browser. Ramblibot will save the session and continue automatically when sign-in completes.".to_owned(),
                                        ));
                                    }
                                    Err(error) => {
                                        let _ = event_tx.send(WorkerEvent::Mode(AppMode::Ready));
                                        let _ = event_tx.send(WorkerEvent::Status(error));
                                    }
                                }
                            }
                            BrowserCommand::LoginCompleted(identity) => {
                                let Some(mut visible_session) = session.take() else { continue; };
                                if !visible_session.visible { session = Some(visible_session); continue; }
                                let identity = if identity.trim().is_empty() { "Ramblio account".to_owned() } else { identity };
                                if let Some(monitor) = visible_session.login_monitor.take() { monitor.abort(); }
                                close_browser_session(&mut visible_session).await;
                                let _ = event_tx.send(WorkerEvent::AccountIdentity(identity));
                                let _ = event_tx.send(WorkerEvent::Mode(AppMode::Switching));
                                let _ = event_tx.send(WorkerEvent::Status("Sign-in detected. Saving the session and opening hangouts…".to_owned()));
                                match open_browser_session(&profile_dir, false, RAMBLIO_HANGOUTS_URL, &event_tx).await {
                                    Ok(background_session) => {
                                        let (hangouts, authenticated) = fetch_hangouts(&background_session.page).await;
                                        if !authenticated {
                                            let mut background_session = background_session;
                                            close_browser_session(&mut background_session).await;
                                            let _ = event_tx.send(WorkerEvent::Mode(AppMode::Ready));
                                            let _ = event_tx.send(WorkerEvent::Status("Ramblio sign-in was not confirmed. Open the sign-in browser and try again.".to_owned()));
                                        } else {
                                            session = Some(background_session);
                                            let _ = event_tx.send(WorkerEvent::Hangouts(hangouts));
                                            let _ = event_tx.send(WorkerEvent::Mode(AppMode::Background));
                                            let _ = event_tx.send(WorkerEvent::Status("Signed in. Choose a hangout to join.".to_owned()));
                                        }
                                    }
                                    Err(error) => { let _ = event_tx.send(WorkerEvent::Mode(AppMode::Ready)); let _ = event_tx.send(WorkerEvent::Status(error)); }
                                }
                            }
                            BrowserCommand::ResumeBackground => {
                                if session.is_some() { continue; }
                                let _ = event_tx.send(WorkerEvent::Mode(AppMode::Switching));
                                let _ = event_tx.send(WorkerEvent::Status("Connecting to the saved Ramblio session…".to_owned()));
                                match open_browser_session(&profile_dir, false, RAMBLIO_HANGOUTS_URL, &event_tx).await {
                                    Ok(background_session) => {
                                        let (hangouts, authenticated) = fetch_hangouts(&background_session.page).await;
                                        if authenticated {
                                            session = Some(background_session);
                                            let _ = event_tx.send(WorkerEvent::Hangouts(hangouts));
                                            let _ = event_tx.send(WorkerEvent::Mode(AppMode::Background));
                                            let _ = event_tx.send(WorkerEvent::Status("Connected to the saved session. Choose a hangout to join.".to_owned()));
                                        } else {
                                            let mut expired = background_session;
                                            close_browser_session(&mut expired).await;
                                            let _ = event_tx.send(WorkerEvent::Mode(AppMode::Ready));
                                            let _ = event_tx.send(WorkerEvent::Status("The saved Ramblio session has expired. Open the sign-in browser to sign in again.".to_owned()));
                                        }
                                    }
                                    Err(error) => { let _ = event_tx.send(WorkerEvent::Mode(AppMode::Ready)); let _ = event_tx.send(WorkerEvent::Status(error)); }
                                }
                            }
                            BrowserCommand::RefreshHangouts => {
                                if let Some(active) = session.as_mut().filter(|s| !s.visible) {
                                    match refresh_hangouts(&active.page).await {
                                        Ok(hangouts) => {
                                            let count = hangouts.len();
                                            let _ = event_tx.send(WorkerEvent::Hangouts(hangouts));
                                            let _ = event_tx.send(WorkerEvent::Status(format!(
                                                "Hangout list refreshed. {count} available."
                                            )));
                                        }
                                        Err(error) => {
                                            let _ = event_tx.send(WorkerEvent::Status(error));
                                        }
                                    }
                                }
                            }
                            BrowserCommand::LoadMoreHangouts => {
                                if let Some(active) = session.as_mut().filter(|s| !s.visible) {
                                    match load_more_hangouts(&active.page).await {
                                        Ok(hangouts) => {
                                            let count = hangouts.len();
                                            let _ = event_tx.send(WorkerEvent::Hangouts(hangouts));
                                            let _ = event_tx.send(WorkerEvent::Status(format!(
                                                "Hangout list expanded. {count} listed."
                                            )));
                                        }
                                        Err(error) => {
                                            let _ = event_tx.send(WorkerEvent::Status(error));
                                        }
                                    }
                                }
                            }
                            BrowserCommand::JoinHangout(id) => {
                                if let Some(active) = session.as_mut().filter(|s| !s.visible) {
                                    let result = join_hangout(&active.page, &id).await;
                                    let message = match result {
                                        Ok(title) => format!(
                                            "Ramblio connection started for {title}. The background session stays open, with audio output muted."
                                        ),
                                        Err(error) => error,
                                    };
                                    let _ = event_tx.send(WorkerEvent::Status(message));
                                }
                            }
                            BrowserCommand::Stop => {
                                if let Some(mut active) = session.take() {
                                    if let Some(monitor) = active.login_monitor.take() { monitor.abort(); }
                                    close_browser_session(&mut active).await;
                                }
                                let _ = event_tx.send(WorkerEvent::Hangouts(Vec::new()));
                                let _ = event_tx.send(WorkerEvent::Mode(AppMode::Ready));
                                let _ = event_tx.send(WorkerEvent::Status("Stopped.".to_owned()));
                            }
                            BrowserCommand::Shutdown => {
                                if let Some(mut active) = session.take() {
                                    close_browser_session(&mut active).await;
                                }
                                break;
                            }
                        }
                    }

                    if let Some(mut active) = session {
                        close_browser_session(&mut active).await;
                    }
                });
            })
            .expect("failed to create browser worker thread");

        Self {
            commands: Some(command_tx),
            events: event_rx,
            thread: Some(worker_thread),
        }
    }

    fn send(&self, command: BrowserCommand) {
        if let Some(commands) = &self.commands {
            let _ = commands.send(command);
        }
    }

    fn take_events(&self) -> Vec<WorkerEvent> {
        self.events.try_iter().collect()
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

    let handler_task = tokio::spawn(async move {
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
        }),
        Err(error) => {
            let _ = browser.close().await;
            let _ = browser.wait().await;
            handler_task.abort();
            Err(format!("Could not open Ramblio: {error}"))
        }
    }
}

async fn close_browser_session(session: &mut BrowserSession) {
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

struct RamblibotApp {
    mode: AppMode,
    hangouts: Vec<Hangout>,
    status: String,
    account_identity: Option<String>,
    worker: BrowserWorker,
}

impl RamblibotApp {
    fn new() -> Self {
        let account_identity = fs::read_to_string(identity_file())
            .ok()
            .map(|identity| identity.trim().to_owned())
            .filter(|identity| !identity.is_empty());
        let status = if account_identity.is_some() {
            "Saved Ramblio session found. Select Sign in as to reconnect.".to_owned()
        } else {
            "Ready. Open the sign-in browser to begin.".to_owned()
        };
        Self {
            mode: AppMode::Ready,
            hangouts: Vec::new(),
            status,
            account_identity,
            worker: BrowserWorker::new(profile_dir()),
        }
    }
}

impl eframe::App for RamblibotApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        for event in self.worker.take_events() {
            match event {
                WorkerEvent::Status(message) => self.status = message,
                WorkerEvent::Mode(mode) => self.mode = mode,
                WorkerEvent::Hangouts(hangouts) => self.hangouts = hangouts,
                WorkerEvent::AccountIdentity(identity) => {
                    let path = identity_file();
                    if let Some(parent) = path.parent() {
                        let _ = fs::create_dir_all(parent);
                    }
                    if fs::write(path, &identity).is_ok() {
                        self.account_identity = Some(identity);
                    }
                }
            }
        }

        ui.heading("Ramblibot");
        ui.label("Sign in once, then manage a background hangout session.");
        ui.add_space(8.0);

        ui.horizontal(|ui| {
            if ui
                .add_enabled(
                    self.mode == AppMode::Ready,
                    egui::Button::new("Open sign-in browser"),
                )
                .clicked()
            {
                self.status = "Opening Ramblio sign-in…".to_owned();
                self.worker.send(BrowserCommand::OpenLogin);
            }
            if let Some(identity) = self.account_identity.as_ref() {
                if ui
                    .add_enabled(
                        self.mode == AppMode::Ready,
                        egui::Button::new(format!("Sign in as {identity}")),
                    )
                    .clicked()
                {
                    self.hangouts.clear();
                    self.status = format!("Connecting as {identity}…");
                    self.worker.send(BrowserCommand::ResumeBackground);
                }
            }
            if ui
                .add_enabled(
                    self.mode == AppMode::Background,
                    egui::Button::new("Refresh hangouts"),
                )
                .clicked()
            {
                self.status = "Refreshing available hangouts…".to_owned();
                self.worker.send(BrowserCommand::RefreshHangouts);
            }
            if ui
                .add_enabled(
                    self.mode == AppMode::Background && !self.hangouts.is_empty(),
                    egui::Button::new("Load more hangouts"),
                )
                .clicked()
            {
                self.status = "Loading more hangouts…".to_owned();
                self.worker.send(BrowserCommand::LoadMoreHangouts);
            }
            if ui
                .add_enabled(
                    self.mode != AppMode::Ready,
                    egui::Button::new("Stop session"),
                )
                .clicked()
            {
                self.worker.send(BrowserCommand::Stop);
            }
        });

        ui.add_space(8.0);
        ui.label("Available hangouts");
        if self.mode == AppMode::Background && self.hangouts.is_empty() {
            ui.label("No hangouts are listed yet. Refresh after signing in.");
        }
        egui::ScrollArea::vertical()
            .max_height(240.0)
            .show(ui, |ui| {
                for hangout in &self.hangouts {
                    let accessible_name = format!("Join hangout: {}", hangout.title);
                    if ui.button(accessible_name).clicked() {
                        self.status = format!("Joining {}…", hangout.title);
                        self.worker
                            .send(BrowserCommand::JoinHangout(hangout.id.clone()));
                    }
                    if hangout.details != hangout.title {
                        ui.small(&hangout.details);
                    }
                }
            });

        ui.add_space(8.0);
        let status = ui.label(format!("Status: {}", self.status));
        ui.ctx().accesskit_node_builder(status.id, |node| {
            node.set_live(egui::accesskit::Live::Polite);
        });
        ui.small("Your Chromium profile and saved account label are stored under %APPDATA%\\Ramblibot. The browser closes automatically after sign-in.");
        ui.ctx().request_repaint_after(Duration::from_millis(250));
    }
}

fn main() -> eframe::Result {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([720.0, 420.0]),
        ..Default::default()
    };
    eframe::run_native(
        "Ramblibot",
        options,
        Box::new(|_creation_context| Ok(Box::new(RamblibotApp::new()))),
    )
}
