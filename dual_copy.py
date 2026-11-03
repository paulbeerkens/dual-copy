"""Two-destination folder copier. Python 3.10+; standard library only."""
import json
import errno
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
import queue
import re
import shutil
import stat
import threading
import time
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from contextlib import ExitStack
from concurrent.futures import ThreadPoolExecutor, as_completed

CHUNK = 4 * 1024 * 1024
PART = '.dualcopy-part'
META = '.dualcopy-meta'
APP_DIR = Path(sys.executable).parent if getattr(sys, 'frozen', False) else Path(__file__).resolve().parent
DATA_DIR = APP_DIR / 'DualCopyData'


def load_settings(directory=DATA_DIR):
    try:
        value = json.loads((directory / 'settings.json').read_text(encoding='utf-8'))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def save_settings(values, geometry, directory=DATA_DIR):
    directory.mkdir(parents=True, exist_ok=True)
    temp = directory / 'settings.tmp'
    temp.write_text(json.dumps({'paths': values, 'geometry': geometry}, indent=2), encoding='utf-8')
    os.replace(temp, directory / 'settings.json')


def transient(error):
    return isinstance(error, OSError) and (
        getattr(error, 'winerror', None) in {21, 32, 33, 51, 53, 54, 55, 59, 64, 67, 121, 995, 1222, 1231, 1232, 1236, 1237, 2250}
        or error.errno in {errno.EAGAIN, errno.EBUSY, errno.ETIMEDOUT, errno.ECONNRESET,
                          errno.ECONNABORTED, errno.ENETDOWN, errno.ENETUNREACH, errno.EHOSTUNREACH})


def run_transfer(source, targets, emit, stop, directory=DATA_DIR, retry_limit=12, retry_delay=5):
    """Log each run, retry transient failures, and summarize even stopped runs."""
    directory.mkdir(parents=True, exist_ok=True)
    log_path = directory / ('transfer-' + datetime.now().strftime('%Y%m%d-%H%M%S-%f') + '.jsonl')
    started = time.monotonic()
    written = [0, 0]
    copied = [set(), set()]
    skipped = [set(), set()]
    total_files = 0
    complete_files = 0
    retries = 0
    error_text = ''
    current_file = str(source)
    outcome = 'Stopped'
    with log_path.open('w', encoding='utf-8', buffering=1) as log:
        def record(action, **details):
            log.write(json.dumps({'time': datetime.now(timezone.utc).isoformat(), 'action': action, **details}) + '\n')
        def receive(event):
            nonlocal total_files, complete_files, current_file
            kind = event[0]
            if kind == 'bytes':
                written[event[1]] += event[2]
                return
            if kind == 'record':
                _, action, index, path = event
                (copied if action == 'copied' else skipped)[index].add(path)
                record(action, destination=index + 1, path=path)
                return
            if kind == 'log':
                record(event[1], path=event[2])
                return
            if kind == 'files':
                complete_files, total_files = event[1:]
            if kind == 'file':
                current_file = event[1]
            if kind == 'complete':
                record('scan_links_skipped', count=event[2])
                return  # The summary ends the UI run, after the log is finalized.
            emit(event)
        record('started', source=str(source), destinations=[str(t) for t in targets])
        try:
            while True:
                try:
                    copy_tree(source, targets, receive, stop)
                    outcome = 'Complete'
                    break
                except OSError as error:
                    record('error', path=current_file, message=str(error))
                    if not transient(error) or retries >= retry_limit:
                        raise
                    retries += 1
                    record('retry', attempt=retries, delay_seconds=retry_delay)
                    emit(('status', f'Connection or file temporarily unavailable. Retry {retries}/{retry_limit} in {retry_delay:g}s. Stop safely cancels.'))
                    if stop.wait(retry_delay):
                        raise Stopped()
        except Stopped:
            outcome = 'Stopped'
            record('stopped')
        except Exception as error:
            outcome = 'Failed'
            error_text = str(error)
            record('failed', path=current_file, message=error_text)
        summary = {
            'outcome': outcome, 'elapsed': time.monotonic() - started,
            'files_complete': complete_files, 'files_total': total_files,
            'copied': [len(s) for s in copied],
            'skipped': [len(skipped[i] - copied[i]) for i in range(2)],
            'written': written, 'retries': retries,
            'failures': int(outcome == 'Failed'), 'error': error_text,
            'log': str(log_path), 'destinations': [str(t) for t in targets],
        }
        record('summary', **summary)
    emit(('summary', summary))
    return summary


class Stopped(Exception):
    pass


def linked(path):
    s = path.lstat()
    return stat.S_ISLNK(s.st_mode) or bool(
        getattr(s, 'st_file_attributes', 0) & 0x400)


def validate(source, targets):
    paths = [Path(p).resolve() for p in [source, *targets]]
    if not stat.S_ISDIR(paths[0].stat().st_mode):
        raise ValueError('Source must be an existing folder.')
    for i, a in enumerate(paths):
        for b in paths[i + 1:]:
            if a == b or a in b.parents or b in a.parents:
                raise ValueError('Source and destinations must be separate, non-overlapping folders.')
    return paths[0], paths[1:]


def matched(path, info):
    try:
        s = path.stat()
        return s.st_size == info.st_size and abs(s.st_mtime_ns - info.st_mtime_ns) <= 100
    except FileNotFoundError:
        return False


def copy_tree(source, targets, emit, stop):
    """Read one source chunk, write it to each outstanding destination."""
    source, targets = validate(source, targets)
    files, folders = [], [Path('.')]
    skipped_links = 0
    def walk_error(error):
        raise error
    last_scan_update = 0
    def scan_progress(path, force=False):
        nonlocal last_scan_update
        if stop.is_set():
            raise Stopped()
        now = time.monotonic()
        if force or now - last_scan_update >= .15:
            # Count subdirectories; the selected source folder is excluded.
            emit(('scan', len(folders) - 1, len(files), str(path)))
            last_scan_update = now
    scan_progress(source, True)
    for root, dirs, names in os.walk(source, followlinks=False, onerror=walk_error):
        if stop.is_set():
            raise Stopped()
        root = Path(root)
        for name in dirs[:]:
            scan_progress(root)
            p = root / name
            if linked(p):
                dirs.remove(name)
                skipped_links += 1
                emit(('log', 'link_skipped', str(p)))
            else:
                folders.append(p.relative_to(source))
        for name in names:
            scan_progress(root)
            p = root / name
            if linked(p):
                skipped_links += 1
                emit(('log', 'link_skipped', str(p)))
                continue
            if name.endswith((PART, META)):
                raise ValueError('Source contains reserved .dualcopy-part or .dualcopy-meta names.')
            info = p.stat()
            if not stat.S_ISREG(info.st_mode):
                raise ValueError(f'Not a regular file: {p}')
            files.append((p.relative_to(source), info))
        scan_progress(root)
    scan_progress(source, True)
    emit(('status', f'Scan complete · {len(folders) - 1:,} directories · {len(files):,} files. Preparing destinations…'))
    total = sum(s.st_size for _, s in files) * 2
    emit(('files', 0, len(files)))
    folder_remaining = {rel: 0 for rel in folders if rel != Path('.')}
    for rel, _ in files:
        for parent in rel.parents:
            if parent in folder_remaining:
                folder_remaining[parent] += 1
    emit(('folders', 0, len(folder_remaining)))
    done = transferred = 0
    start = time.monotonic()
    last = 0
    def progress(name, force=False):
        nonlocal last
        now = time.monotonic()
        if force or now - last >= .15:
            emit(('progress', done, total, transferred, now - start, str(name)))
            last = now
    # Refuse destination links, including existing ancestor folders.
    for target in targets:
        for p in [target, *target.parents]:
            if p.exists() and linked(p):
                raise ValueError(f'Destination contains a junction or link: {p}')
        for rel in folders:
            p = target / rel
            if p.exists() and linked(p):
                raise ValueError(f'Destination contains a junction or link: {p}')
            p.mkdir(parents=True, exist_ok=True)
    completed_folders = sum(count == 0 for count in folder_remaining.values())
    emit(('folders', completed_folders, len(folder_remaining)))
    for file_number, (rel, info) in enumerate(files, 1):
        if stop.is_set():
            raise Stopped()
        src = source / rel
        pending = []
        file_start = time.monotonic()
        file_done = [0, 0]
        file_written = [0, 0]
        file_update = [0, 0]
        def destination_progress(index, state='', force=False):
            now = time.monotonic()
            if force or now - file_update[index] >= .15:
                speed = file_written[index] / max(now - file_start, .001)
                emit(('destination', index, file_done[index], info.st_size, speed, state))
                file_update[index] = now
        def write_chunk(writer, data, index):
            count = writer.write(data)
            if count != len(data):
                raise OSError(f'Incomplete write to destination {index + 1}.')
            file_done[index] += count
            file_written[index] += count
            emit(('bytes', index, count))
            destination_progress(index)
            return count
        identity = {'source': str(src), 'size': info.st_size, 'mtime_ns': info.st_mtime_ns}
        for index, target in enumerate(targets):
            dest = target / rel
            part, meta = Path(str(dest) + PART), Path(str(dest) + META)
            for p in (dest, part, meta):
                if p.is_symlink() or (p.exists() and linked(p)):
                    raise ValueError(f'Destination contains a link: {p}')
            if matched(dest, info):
                done += info.st_size
                file_done[index] = info.st_size
                destination_progress(index, 'Already copied', True)
                emit(('record', 'skipped', index, str(dest)))
                continue
            offset = 0
            if part.exists() and meta.exists():
                try:
                    if json.loads(meta.read_text(encoding='utf-8')) == identity:
                        offset = min(part.stat().st_size, info.st_size)
                except (ValueError, UnicodeError):
                    pass
            # Save identity before writing; stale partials restart from zero.
            meta.write_text(json.dumps(identity), encoding='utf-8')
            pending.append((dest, part, meta, offset, index))
            done += offset
            file_done[index] = offset
            destination_progress(index, 'Resuming' if offset else '', True)
        if pending:
            emit(('file', str(src)))
            with ExitStack() as stack:
                reader = stack.enter_context(src.open('rb'))
                writers = []
                for dest, part, meta, offset, index in pending:
                    writer = stack.enter_context(part.open('r+b' if part.exists() else 'w+b'))
                    writer.truncate(offset)
                    writer.seek(offset)
                    writers.append((writer, offset, index))
                # ExitStack shuts workers down before it closes their files.
                pool = stack.enter_context(ThreadPoolExecutor(max_workers=2, thread_name_prefix='destination'))
                position = min(offset for _, offset, _ in writers)
                reader.seek(position)
                while position < info.st_size:
                    if stop.is_set():
                        raise Stopped()
                    chunk = reader.read(min(CHUNK, info.st_size - position))
                    if not chunk:
                        raise OSError(f'Source changed or became unreadable: {src}')
                    end = position + len(chunk)
                    futures = []
                    for writer, offset, index in writers:
                        begin = max(position, offset)
                        if begin < end:
                            data = memoryview(chunk)[begin - position:]
                            futures.append(pool.submit(write_chunk, writer, data, index))
                    # Both writes must finish before reading another source chunk.
                    for future in as_completed(futures):
                        count = future.result()
                        done += count
                        transferred += count
                    position = end
                    progress(src)
                for writer, _, _ in writers:
                    writer.flush()
                    os.fsync(writer.fileno())
                current = src.stat()
                if (current.st_size, current.st_mtime_ns) != (info.st_size, info.st_mtime_ns):
                    for _, part, meta, _, _ in pending:
                        meta.unlink(missing_ok=True)
                    raise OSError(f'Source changed during copy; rerun after it stops changing: {src}')
            for dest, part, meta, _, index in pending:
                shutil.copystat(src, part)
                os.replace(part, dest)
                meta.unlink(missing_ok=True)
                destination_progress(index, 'Complete', True)
                emit(('record', 'copied', index, str(dest)))
        progress(src, True)
        emit(('files', file_number, len(files)))
        for parent in rel.parents:
            if parent in folder_remaining:
                folder_remaining[parent] -= 1
                if folder_remaining[parent] == 0:
                    completed_folders += 1
        emit(('folders', completed_folders, len(folder_remaining)))
    emit(('complete', len(files), skipped_links))


def human(n):
    for unit in ('B', 'KiB', 'MiB', 'GiB', 'TiB'):
        if n < 1024 or unit == 'TiB':
            return f'{n:.1f} {unit}'
        n /= 1024


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title('Dual Copy')
        self.geometry('900x520')
        self.minsize(780, 520)
        self.events = queue.Queue()
        self.stop = threading.Event()
        self.running = False
        self.fields = []
        self.settings_timer = None
        self.last_log = None
        settings = load_settings()
        frame = ttk.Frame(self, padding=18)
        frame.pack(fill='both', expand=True)
        frame.columnconfigure(1, weight=1)
        for row, (label, value) in enumerate([
            ('Source folder', 'Z:\\Pictures'),
            ('Destination 1', '\\\\linux1\\disk2\\drobo\\Pictures'),
            ('Destination 2', 'D:\\Pictures')]):
            ttk.Label(frame, text=label).grid(row=row, column=0, sticky='w', pady=6)
            field = ttk.Entry(frame)
            field.insert(0, value)
            field.grid(row=row, column=1, sticky='ew', padx=10)
            self.fields.append(field)
            field.bind('<FocusOut>', lambda event: self.persist_settings())
            ttk.Button(frame, text='Browse…', command=lambda f=field: self.browse(f)).grid(row=row, column=2)
        ttk.Label(frame, text='Copies folder contents. Existing changed files are replaced; extra files are kept.').grid(row=3, column=0, columnspan=3, sticky='w', pady=(10, 8))
        self.bar = ttk.Progressbar(frame, maximum=100)
        self.bar.grid(row=4, column=0, columnspan=3, sticky='ew')
        self.stats = ttk.Label(frame, text='Ready')
        self.stats.grid(row=5, column=0, columnspan=3, sticky='w', pady=8)
        self.name = ttk.Label(frame, text='', wraplength=720)
        self.name.grid(row=6, column=0, columnspan=3, sticky='w')
        self.bind('<Configure>', lambda event: self.name.configure(
            wraplength=max(300, self.winfo_width() - 40)) if event.widget is self else None)
        self.destination_bars = []
        self.destination_stats = []
        for index in range(2):
            row = 7 + index
            ttk.Label(frame, text=f'Destination {index + 1} · file').grid(row=row, column=0, sticky='w', pady=8)
            bar = ttk.Progressbar(frame, maximum=100)
            bar.grid(row=row, column=1, sticky='ew', padx=10)
            label = ttk.Label(frame, text='0% · 0.0 kB/s', width=30)
            label.grid(row=row, column=2, sticky='w')
            self.destination_bars.append(bar)
            self.destination_stats.append(label)
        self.file_count = ttk.Label(frame, text='Files complete: 0 · Remaining: —')
        self.file_count.grid(row=9, column=0, columnspan=3, sticky='w', pady=(8, 0))
        self.folder_count = ttk.Label(frame, text='Folders complete: 0 · Remaining: —')
        self.folder_count.grid(row=10, column=0, columnspan=3, sticky='w', pady=(6, 0))
        buttons = ttk.Frame(frame)
        buttons.grid(row=11, column=0, columnspan=3, sticky='w', pady=12)
        self.start_button = ttk.Button(buttons, text='Start / Resume', command=self.start)
        self.start_button.pack(side='left')
        self.stop_button = ttk.Button(buttons, text='Stop safely', command=self.stop.set, state='disabled')
        self.stop_button.pack(side='left', padx=10)
        ttk.Button(buttons, text='Open last log', command=self.open_log).pack(side='left', padx=10)
        stored_paths = settings.get('paths')
        if isinstance(stored_paths, list) and len(stored_paths) == 3 and all(isinstance(p, str) for p in stored_paths):
            for field, value in zip(self.fields, stored_paths):
                field.delete(0, 'end')
                field.insert(0, value)
        geometry = settings.get('geometry', '')
        if isinstance(geometry, str) and re.fullmatch(r'\d+x\d+(?:[+-]\d+[+-]\d+)?', geometry):
            self.geometry(geometry)
        self.bind('<Configure>', self.schedule_settings, add='+')
        self.protocol('WM_DELETE_WINDOW', self.close)
        self.after(100, self.poll)

    def browse(self, field):
        if self.running:
            return
        path = filedialog.askdirectory()
        if path:
            field.delete(0, 'end')
            field.insert(0, path)
            self.persist_settings()

    def schedule_settings(self, event):
        if event.widget is self:
            if self.settings_timer:
                self.after_cancel(self.settings_timer)
            self.settings_timer = self.after(600, self.persist_settings)

    def persist_settings(self):
        self.settings_timer = None
        try:
            save_settings([f.get().strip() for f in self.fields], self.geometry())
        except OSError as error:
            self.stats['text'] = f'Could not save settings: {error}'

    def open_log(self):
        if self.last_log:
            try:
                os.startfile(self.last_log)
            except OSError as error:
                messagebox.showerror('Could not open log', str(error))
        else:
            messagebox.showinfo('Transfer log', 'A log is saved after you start a transfer.')

    def show_summary(self, summary):
        self.last_log = summary['log']
        minutes, seconds = divmod(int(summary['elapsed']), 60)
        lines = [f"{summary['outcome']} · {minutes} min {seconds} sec",
                 f"Files complete at both destinations: {summary['files_complete']:,} / {summary['files_total']:,}",
                 f"Remaining: {max(0, summary['files_total'] - summary['files_complete']):,}",
                 f"Failures: {summary['failures']} · Retries: {summary['retries']}"]
        for index in range(2):
            lines += ['', f"Destination {index + 1}: {summary['destinations'][index]}",
                      f"Files copied: {summary['copied'][index]:,} · Already present: {summary['skipped'][index]:,}",
                      f"Bytes written this run: {human(summary['written'][index])} ({summary['written'][index]:,} bytes)"]
        if summary['error']:
            lines += ['', summary['error']]
        lines += ['', f"Log: {summary['log']}"]
        window = tk.Toplevel(self)
        window.title('Transfer summary')
        window.geometry('780x480')
        text = tk.Text(window, wrap='word', padx=14, pady=14)
        text.pack(fill='both', expand=True)
        text.insert('1.0', '\n'.join(lines))
        text.configure(state='disabled')
        ttk.Button(window, text='Open log', command=self.open_log).pack(side='left', padx=14, pady=10)
        ttk.Button(window, text='Close', command=window.destroy).pack(side='right', padx=14, pady=10)

    def start(self):
        values = [f.get().strip() for f in self.fields]
        try:
            if not all(values):
                raise ValueError('Choose a source and both destinations.')
        except Exception as error:
            messagebox.showerror('Check folders', str(error))
            return
        self.running = True
        self.persist_settings()
        self.stop.clear()
        self.bar['value'] = 0
        self.file_count['text'] = 'Files complete: 0 · Remaining: scanning…'
        self.folder_count['text'] = 'Folders complete: 0 · Remaining: scanning…'
        for bar, label in zip(self.destination_bars, self.destination_stats):
            bar['value'] = 0
            label['text'] = '0% · 0.0 kB/s'
        self.start_button['state'] = 'disabled'
        self.stop_button['state'] = 'normal'
        for f in self.fields:
            f['state'] = 'disabled'
        def run():
            try:
                run_transfer(values[0], values[1:], self.events.put, self.stop)
            except Stopped:
                self.events.put(('stopped',))
            except Exception as error:
                self.events.put(('error', str(error)))
        threading.Thread(target=run, daemon=True).start()

    def poll(self):
        try:
            while True:
                event = self.events.get_nowait()
                kind = event[0]
                if kind == 'status':
                    self.stats['text'] = event[1]
                elif kind == 'scan':
                    self.stats['text'] = f'Scanning source… {event[1]:,} directories · {event[2]:,} files found'
                    self.name['text'] = f'Scanning: {event[3]}'
                elif kind == 'file':
                    self.name['text'] = f'Copying: {event[1]}'
                elif kind == 'files':
                    _, completed, total_files = event
                    self.file_count['text'] = f'Files complete: {completed:,} · Remaining: {total_files - completed:,} · Total: {total_files:,}'
                elif kind == 'folders':
                    _, completed, total_folders = event
                    self.folder_count['text'] = f'Folders complete: {completed:,} · Remaining: {total_folders - completed:,} · Total: {total_folders:,}'
                elif kind == 'destination':
                    _, index, copied, size, speed, state = event
                    percent = copied / size * 100 if size else 100
                    rate = f'{speed / 1_000_000:.1f} MB/s' if speed >= 1_000_000 else f'{speed / 1000:.1f} kB/s'
                    self.destination_bars[index]['value'] = percent
                    self.destination_stats[index]['text'] = f'{percent:.1f}% · {rate}' + (f' · {state}' if state else '')
                elif kind == 'progress':
                    _, done, total, moved, elapsed, name = event
                    speed = moved / max(elapsed, .001)
                    percent = done / total * 100 if total else 100
                    eta = f'{(total - done) / speed / 60:.1f} min remaining' if speed else 'Estimating…'
                    self.bar['value'] = percent
                    self.stats['text'] = f'{percent:.1f}% · {human(done)} / {human(total)} · {human(speed)}/s written · {eta}'
                    self.name['text'] = f'File: {name}'
                elif kind in ('summary', 'complete', 'stopped', 'error'):
                    self.running = False
                    self.start_button['state'] = 'normal'
                    self.stop_button['state'] = 'disabled'
                    for f in self.fields:
                        f['state'] = 'normal'
                    if kind == 'summary':
                        summary = event[1]
                        if summary['outcome'] == 'Complete':
                            self.bar['value'] = 100
                        self.stats['text'] = f"{summary['outcome']} · {summary['files_complete']:,} files complete · {summary['retries']} retries"
                        self.show_summary(summary)
                    elif kind == 'complete':
                        self.bar['value'] = 100
                        self.stats['text'] = f'Complete · {event[1]} files · {event[2]} junctions/links skipped'
                    elif kind == 'stopped':
                        self.stats['text'] = 'Stopped. Start / Resume continues saved partial files.'
                    else:
                        self.stats['text'] = 'Stopped after an error. Partial files kept for retry.'
                        messagebox.showerror('Copy stopped', event[1])
        except queue.Empty:
            pass
        self.after(100, self.poll)

    def close(self):
        self.persist_settings()
        if self.running:
            self.stop.set()
            self.stats['text'] = 'Stopping… Close the window again after the copy stops.'
        else:
            self.destroy()


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--self-test':
        # Packaging diagnostic: verify the embedded GUI runtime without a copy.
        root = tk.Tk()
        root.withdraw()
        root.update()
        root.destroy()
        Path(sys.argv[2]).write_text('Embedded Python and Tkinter startup passed.\n', encoding='utf-8')
    else:
        App().mainloop()
