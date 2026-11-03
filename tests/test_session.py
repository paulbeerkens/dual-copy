import sys
from pathlib import Path
import tempfile
import threading
import errno
import json
sys.path.insert(0, str(Path(__file__).parents[1]))
import dual_copy as app

with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as tmp:
    root = Path(tmp)
    data = root / 'data'
    app.save_settings(['a', 'b', 'c'], '900x520+20+20', data)
    assert app.load_settings(data)['paths'] == ['a', 'b', 'c']
    (data / 'settings.json').write_text('{broken')
    assert app.load_settings(data) == {}
    source = root / 'source'
    source.mkdir()
    (source / 'one').write_bytes(b'a' * 100000)
    targets = [root / 'a', root / 'b']
    events = []
    summary = app.run_transfer(source, targets, events.append, threading.Event(), data)
    assert summary['outcome'] == 'Complete'
    assert summary['copied'] == [1, 1]
    assert summary['written'] == [100000, 100000]
    assert summary['files_complete'] == summary['files_total'] == 1
    records = [json.loads(line) for line in Path(summary['log']).read_text().splitlines()]
    assert records[-1]['action'] == 'summary'
    summary = app.run_transfer(source, targets, events.append, threading.Event(), data)
    assert summary['copied'] == [0, 0] and summary['skipped'] == [1, 1]
    assert summary['written'] == [0, 0]
    original = app.copy_tree
    calls = [0]
    def flaky(*args):
        calls[0] += 1
        if calls[0] == 1:
            raise OSError(errno.ECONNRESET, 'Simulated disconnect')
        return original(*args)
    app.copy_tree = flaky
    summary = app.run_transfer(source, targets, events.append, threading.Event(), data, retry_delay=.01)
    assert summary['outcome'] == 'Complete' and summary['retries'] == 1
    def offline(*args):
        raise OSError(errno.ETIMEDOUT, 'Simulated timeout')
    app.copy_tree = offline
    summary = app.run_transfer(source, targets, events.append, threading.Event(), data, retry_limit=2, retry_delay=.01)
    assert summary['outcome'] == 'Failed' and summary['retries'] == 2 and summary['failures'] == 1
    stop = threading.Event()
    def cancel(event):
        if event[0] == 'status':
            stop.set()
    summary = app.run_transfer(source, targets, cancel, stop, data, retry_delay=10)
    assert summary['outcome'] == 'Stopped'
    app.copy_tree = original
    # Inject a disconnect inside a destination worker, after a successful chunk.
    (source / 'one').write_bytes(b'new-data' * 1500000)
    original_open = Path.open
    calls = [0]
    class DisconnectingWriter:
        def __init__(self, stream):
            self.stream = stream
        def __getattr__(self, name):
            return getattr(self.stream, name)
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return self.stream.__exit__(*args)
        def write(self, data):
            calls[0] += 1
            if calls[0] == 2:
                raise OSError(errno.ECONNRESET, 'Disconnect during write')
            return self.stream.write(data)
    def unreliable_open(path, mode='r', *args, **kwargs):
        stream = original_open(path, mode, *args, **kwargs)
        if path == Path(str(targets[0] / 'one') + app.PART):
            return DisconnectingWriter(stream)
        return stream
    Path.open = unreliable_open
    try:
        summary = app.run_transfer(source, targets, events.append, threading.Event(), data, retry_delay=.01)
    finally:
        Path.open = original_open
    assert summary['outcome'] == 'Complete' and summary['retries'] == 1
    assert summary['copied'] == [1, 1]
    for target in targets:
        assert (target / 'one').read_bytes() == (source / 'one').read_bytes()
print('PASS: settings persistence/corruption recovery, logs, copied/skipped/byte summaries, network retry/recovery, retry limit, cancellation during retry')
print('PASS: disconnect during parallel write, automatic retry and exact output recovery')
