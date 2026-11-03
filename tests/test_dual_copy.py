import sys
import types
import importlib.util
import tempfile
import threading
from pathlib import Path

try:
    import tkinter
except ImportError:
    module = types.ModuleType('tkinter')
    module.Tk = object
    for name in ('filedialog', 'messagebox', 'ttk'):
        setattr(module, name, types.ModuleType(name))
    sys.modules['tkinter'] = module

spec = importlib.util.spec_from_file_location('dual', Path(__file__).parents[1] / 'dual_copy.py')
dual = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dual)

with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as tmp:
    base = Path(tmp)
    source = base / 'source'
    source.mkdir()
    (source / 'empty').mkdir()
    content = bytes(range(256)) * 90000
    (source / 'large.bin').write_bytes(content)
    (source / 'zero').touch()
    targets = [base / 'a', base / 'b']
    stop = threading.Event()
    def interrupt(event):
        if event[0] == 'progress':
            stop.set()
    try:
        dual.copy_tree(source, targets, interrupt, stop)
        raise AssertionError('Did not stop')
    except dual.Stopped:
        pass
    assert (targets[0] / ('large.bin' + dual.PART)).exists()
    # Simulate destinations having saved different amounts.
    with (targets[1] / ('large.bin' + dual.PART)).open('r+b') as stream:
        stream.truncate(12345)
    stop.clear()
    events = []
    dual.copy_tree(source, targets, events.append, stop)
    for target in targets:
        assert (target / 'large.bin').read_bytes() == content
        assert (target / 'empty').is_dir()
        assert (target / 'zero').stat().st_size == 0
        assert not list(target.glob('*.dualcopy-*'))
        (target / 'extra').write_text('keep')
    events.clear()
    dual.copy_tree(source, targets, events.append, stop)
    assert all(e[3] == 0 for e in events if e[0] == 'progress'), 'Re-read completed files'
    (source / 'large.bin').write_bytes(b'changed')
    dual.copy_tree(source, targets, lambda e: None, stop)
    assert all((t / 'large.bin').read_bytes() == b'changed' for t in targets)
    assert all((t / 'extra').read_text() == 'keep' for t in targets)
    try:
        dual.validate(source, [source / 'nested', targets[0]])
        raise AssertionError('Overlap accepted')
    except ValueError:
        pass
    # A two-party barrier fails if destination writes run sequentially.
    # Source reads also assert that no previous write is still in flight.
    (source / 'large.bin').write_bytes(content)
    original_open = Path.open
    barrier = threading.Barrier(2, timeout=5)
    lock = threading.Lock()
    active = [0]
    writer_threads = set()
    class CheckedStream:
        def __init__(self, stream, writing):
            self.stream, self.writing = stream, writing
        def __getattr__(self, name):
            return getattr(self.stream, name)
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return self.stream.__exit__(*args)
        def write(self, data):
            with lock:
                active[0] += 1
                writer_threads.add(threading.get_ident())
            try:
                barrier.wait()
                return self.stream.write(data)
            finally:
                with lock:
                    active[0] -= 1
        def read(self, size):
            with lock:
                assert active[0] == 0, 'Read before both destination writes completed'
            return self.stream.read(size)
    def checked_open(path, mode='r', *args, **kwargs):
        stream = original_open(path, mode, *args, **kwargs)
        if str(path).endswith(dual.PART) or (path == source / 'large.bin' and mode == 'rb'):
            return CheckedStream(stream, str(path).endswith(dual.PART))
        return stream
    Path.open = checked_open
    try:
        events.clear()
        dual.copy_tree(source, targets, events.append, stop)
    finally:
        Path.open = original_open
    assert len(writer_threads) == 2
    for index in range(2):
        updates = [e for e in events if e[0] == 'destination' and e[1] == index]
        assert updates and any(e[2] == len(content) and e[5] == 'Complete' for e in updates)
        assert (targets[index] / 'large.bin').read_bytes() == content
print('PASS: dual output bytes, interrupted/unequal resume, empty files/folders, skip completed, replacement, extra-file retention, overlap rejection')
print('PASS: two concurrent writer threads, read-after-both-writes barrier, per-destination completion progress')
