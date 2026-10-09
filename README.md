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
4. The background browser remains running in that room until **Stop session** or the app is closed. If the hidden browser connection drops, Ramblibot tries again up to five times and rejoins that room, or reloads the hangout list when no room was joined. A drop that cannot be restored opens an error dialog. After stopping, use **Sign in as [email]** to reconnect with the saved session and show hangouts again. If the session expired, sign in again through **Open sign-in browser**.

Chromium's cookies and profile data are stored under `%APPDATA%\Ramblibot\ChromiumProfile`. The email entered on the sign-in page is saved in `%APPDATA%\Ramblibot\account_label.txt` so the app can label the reconnect button. Use a dedicated secondary account.

## Accessibility

The window is built with [RunGUI](https://github.com/graydon/rungui), so the controls are native Win32 controls exposed through UI Automation. Buttons expose their captions and enabled state. The hangout list is a list box named **Available hangouts**; each item is named **Join hangout: [room name]**. Tab moves through the session buttons and then the list. Arrow keys move between rooms, and Enter or a double-click joins the selected room. When a room's details differ from its title, those details appear in a label under the list for the selected row.

Status text is a label whose accessible name is the full status sentence, such as "Status: Signed in. Choose a hangout to join." Each change also raises a UI Automation notification so a screen reader can speak the latest status. If the browser worker cannot start, the hidden connection cannot be restored, the sign-in browser closes unexpectedly, or the process panics, Ramblibot opens an error dialog.

Test with NVDA and Speech Viewer before relying on a release. Confirm that the list announces the room names, status changes are spoken, the error dialog is announced, and refresh/load-more errors are announced. A successful build and UI Automation tree inspection do not verify NVDA speech or that a signed-in Ramblio account exposes every room action in headless Chromium.
