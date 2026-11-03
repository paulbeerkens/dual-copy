import sys
import tempfile
import threading
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parents[1]))
import dual_copy as app


def no_scanner():
    assert not any(t.name == 'source-scanner' for t in threading.enumerate())


with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as tmp:
    base = Path(tmp)
    src = base / 'source'
    (src / 'later' / 'nested').mkdir(parents=True)
    (src / 'empty').mkdir()
    (src / 'first').write_bytes(b'x' * 100000)
    (src / 'later' / 'nested' / 'last').write_bytes(b'last')
    targets = [base / 'a', base / 'b']
    first_copied = threading.Event()
    real_scandir = app.os.scandir
    events = []
    def slow_scandir(path):
        if Path(path) == src / 'later':
            assert first_copied.wait(5), 'Copy waited for the entire scan'
        return real_scandir(path)
    def receive(event):
        events.append(event)
        if event[0] == 'record' and event[1] == 'copied' and Path(event[3]).name == 'first':
            first_copied.set()
    app.os.scandir = slow_scandir
    try:
        app.copy_tree(src, targets, receive, threading.Event())
    finally:
        app.os.scandir = real_scandir
    assert first_copied.is_set()
    assert [e for e in events if e[0] == 'files'][-1] == ('files', 2, 2)
    assert [e for e in events if e[0] == 'folders'][-1] == ('folders', 3, 3)
    assert [e for e in events if e[0] == 'scan'][-1][4] is True
    for t in targets:
        assert (t / 'later/nested/last').read_bytes() == b'last'
        assert (t / 'empty').is_dir()
    no_scanner()

    # Stall copying while scanner discovers more work than the coordinator drains.
    scan_done = threading.Event()
    real_scan = app.scan_source
    def fast_scan(source, inbox, cancel):
        assert inbox.maxsize == 0, 'Scanner queue must be unbounded'
        real_scan(source, inbox, cancel)
        scan_done.set()
    app.scan_source = fast_scan
    real_open = Path.open
    class SlowWriter:
        def __init__(self, stream):
            self.stream = stream
        def __getattr__(self, name):
            return getattr(self.stream, name)
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return self.stream.__exit__(*args)
        def write(self, data):
            assert scan_done.wait(5), 'Scanner blocked behind copying'
            return self.stream.write(data)
    def slow_open(path, mode='r', *args, **kwargs):
        stream = real_open(path, mode, *args, **kwargs)
        return SlowWriter(stream) if str(path).endswith(app.PART) else stream
    (src / 'first').write_bytes(b'changed-content')
    Path.open = slow_open
    try:
        app.copy_tree(src, targets, lambda e: None, threading.Event())
    finally:
        Path.open = real_open
        app.scan_source = real_scan
    no_scanner()

    # Cancel an active scan and verify it is joined before returning.
    def cancellable_scan(source, inbox, cancel):
        inbox.put(('directory', Path('unfinished')))
        cancel.wait(5)
    app.scan_source = cancellable_scan
    stop = threading.Event()
    def cancel_when_found(event):
        if event[0] == 'scan' and event[1] > 0:
            stop.set()
    try:
        try:
            app.copy_tree(src, targets, cancel_when_found, stop)
            raise AssertionError('Cancellation ignored')
        except app.Stopped:
            pass
    finally:
        app.scan_source = real_scan
    no_scanner()

    # Errors from scanner must reach session summary; discovered totals are partial.
    def broken_scan(source, inbox, cancel):
        inbox.put(('file', Path('first'), app.FileInfo(15, 0)))
        inbox.put(('error', PermissionError('Simulated scan error')))
    app.scan_source = broken_scan
    try:
        summary = app.run_transfer(src, targets, lambda e: None, threading.Event(), base / 'logs')
    finally:
        app.scan_source = real_scan
    assert summary['outcome'] == 'Failed'
    assert summary['scan_complete'] is False
    assert summary['files_total'] == 1
    assert 'Simulated scan error' in summary['error']
    no_scanner()

    # Empty source is a complete 0/0 job, not an indefinite scan.
    empty = base / 'empty-source'
    empty.mkdir()
    events = []
    app.copy_tree(empty, targets, events.append, threading.Event())
    assert events[-1] == ('complete', 0, 0)
    assert [e for e in events if e[0] == 'scan'][-1][4] is True
    no_scanner()

    # Refresh queued metadata: a source may change while waiting behind another file.
    changing = base / 'changing'
    changing.mkdir()
    (changing / 'first').write_bytes(b'first')
    (changing / 'second').write_bytes(b'old')
    new_targets = [base / 'c', base / 'd']
    def queued_scan(source, inbox, cancel):
        for name in ('first', 'second'):
            s = (source / name).stat()
            inbox.put(('file', Path(name), app.FileInfo(s.st_size, s.st_mtime_ns)))
        inbox.put(('sealed', Path('.')))
        inbox.put(('finished',))
    app.scan_source = queued_scan
    def change_waiting_file(event):
        if event[0] == 'record' and event[1] == 'copied' and Path(event[3]).name == 'first':
            (changing / 'second').write_bytes(b'updated content with different length')
    try:
        app.copy_tree(changing, new_targets, change_waiting_file, threading.Event())
    finally:
        app.scan_source = real_scan
    for target in new_targets:
        assert (target / 'second').read_bytes() == (changing / 'second').read_bytes()
    no_scanner()

print('PASS: copying before scan completes, unbounded discovery, nested/empty folder totals, scanner cancellation/join, scan errors, empty-source completion')
