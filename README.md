# Dual Copy

A small Windows desktop app that copies one source folder to two destinations.
It reads each source chunk once, writes it using two worker threads, and waits
for both writes before reading the next chunk.

## Features

- Background scanning through an unbounded queue while copying starts immediately.
- Overall progress, speed, time remaining, and full source path.
- A current-file progress bar and write speed for each destination.
- Live scan totals and completed/remaining file and folder counts.
- Skip completed files using size and modification time.
- Resume interrupted partial files independently at each destination.
- Automatic retries for temporary network and file-busy errors.
- Saved paths and window size, per-run transfer logs, and a final summary.
- Skip source junctions and links; reject destination links.
- Keep extra destination files and copy empty folders.

## Run

Use `DualCopy.exe` from a Windows build, or install Python 3.10+ with Tcl/Tk
and run:

```powershell
python dual_copy.py
```

Choose a source and two separate destination folders. Their contents are copied
directly into the destination folders. Click **Start / Resume**. **Stop safely**
keeps partial files for a later restart.

Settings and logs are stored in `DualCopyData` beside the app. Keep the app in a
writable folder. The initial paths are examples from the original setup; change
them to your folders before starting. The app never starts a transfer automatically.

## Build a standalone Windows executable

On Windows with Python 3.12 and Tcl/Tk installed:

```powershell
python -m pip install -r requirements-build.txt
python -m PyInstaller --noconfirm --onefile --windowed --name DualCopy dual_copy.py
```

The executable is written to `dist/DualCopy.exe`. The included GitHub Actions
workflow runs the tests and builds a downloadable executable for each push or
manual workflow run. It does not publish a release automatically.

## Tests

```powershell
python tests/test_dual_copy.py
python tests/test_session.py
python tests/test_streaming.py
```

Tests use temporary local folders, not the configured source/destination paths.
They cover parallel writes and the read barrier, resume with different offsets,
skip behavior, file replacement, settings, logs, summaries, retry limits,
cancellation, recovery from a simulated disconnect during a write, overlapping
scanning/copying, scanner shutdown, and folder completion with growing totals.

## Scanning and progress

The scanner uses a separate thread and an unbounded, thread-safe queue of file
metadata. It never waits for copying to catch up. Destination subfolders are
created as needed, including empty folders; there is no full-tree preparation
phase. One coordinator owns aggregate counters and consumes discovery events.
Worker callbacks are serialized, and only the GUI thread updates widgets.

While scanning, the app shows files/folders found, complete, and pending, with an
indeterminate overall bar. These are discoveries so far, not final totals. After
scanning, the bar switches to a percentage and estimated remaining time. The two
current-file progress bars stay determinate throughout. A folder is complete
only after its scan is sealed, all child folders and files have finished, and
both destination directories exist. A stopped/failed scan is clearly marked as
partial in the summary. Scanners are stopped and joined before retrying.

## Copy behavior and limitations

Existing files with matching size and modification time are skipped without
hashing. Changed files are replaced after the new file is fully written. A
saved partial copy is trusted when its recorded source path, size and timestamp
still match. This is not checksum verification. Keep source files unchanged
during copying.

Each retry rescans the tree and uses completed files and saved partials. There
are up to 12 retries with five-second waits. A Windows I/O call that is already
blocked must return before cancellation can take effect.

The app copies file timestamps/basic attributes where supported. It does not
preserve ACLs, alternate data streams, hard-link relationships, or directory
timestamps. Different mapped paths can refer to the same underlying storage;
choose genuinely separate destinations.

See [USER-GUIDE.txt](USER-GUIDE.txt) for more details.
