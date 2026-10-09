# Ramblibot

A Windows desktop controller for signing into Ramblio once, browsing available hangouts from the app, and keeping the selected room joined in a hidden Chromium process.

## Build and run

Install the stable Rust toolchain with `rustup`, then from this folder:

```powershell
cargo run
```

Google Chrome or Chromium must be installed so the browser controller can find it.

To build an executable:

```powershell
cargo build --release
```

The executable is written to `target\release\ramblibot.exe`.

## Use

1. Select **Open sign-in browser** in Ramblibot and complete the magic-link sign-in in the visible browser.
2. When Ramblio returns to its signed-in page, Ramblibot detects the completed sign-in, saves the Chromium profile, closes the visible browser, and opens the hangout list in a hidden browser.
3. Choose **Join hangout: [room name]** in Ramblibot. **Refresh hangouts** reloads the list; **Load more hangouts** requests the next group from Ramblio.
4. The background browser remains running in that room until **Stop session** or the app is closed. After stopping, use **Sign in as [email]** to reconnect with the saved session and show hangouts again. If the session expired, sign in again through **Open sign-in browser**.

Chromium's cookies and profile data are stored under `%APPDATA%\Ramblibot\ChromiumProfile`. The email entered on the sign-in page is saved in `%APPDATA%\Ramblibot\account_label.txt` so the app can label the reconnect button. Use a dedicated secondary account.

## Accessibility

The Ramblibot controls use eframe's AccessKit support to expose names, roles, and enabled states through Windows UI Automation. The hangout list consists of individually named buttons, and status changes use a polite live region. Keyboard focus follows the visible control order; Tab moves through the session controls and then the room join buttons.

Test with NVDA and Speech Viewer before relying on a release. Confirm that the join buttons announce the room names, the live status changes are spoken, and refresh/load-more errors are announced. A successful build and UI Automation tree inspection do not verify NVDA speech or that a signed-in Ramblio account exposes every room action in headless Chromium.
