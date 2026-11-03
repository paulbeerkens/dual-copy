import os
import sys
import tempfile
import threading
import subprocess
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))
import dual_copy as app

project = str(Path(__file__).parents[1])
def child_guard(kind):
    code = (
        "import sys; sys.path.insert(0, " + repr(project) + "); "
        "import dual_copy as d; "
        "guard = d.ExclusiveGuard(" + repr(kind) + "); "
        "guard.__enter__(); guard.__exit__(None,None,None)"
    )
    return subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, timeout=10)

for kind in ('app', 'transfer'):
    with app.ExclusiveGuard(kind):
        errors = []
        def competing_thread():
            try:
                with app.ExclusiveGuard(kind):
                    errors.append('guard allowed concurrent owner')
            except app.AlreadyRunning:
                pass
        thread = threading.Thread(target=competing_thread)
        thread.start()
        thread.join(5)
        assert not thread.is_alive() and not errors
        result = child_guard(kind)
        assert result.returncode != 0 and 'AlreadyRunning' in result.stderr, result.stderr
    assert child_guard(kind).returncode == 0

# A process crash releases the OS mutex without a stale lock file.
code = (
    "import sys;sys.path.insert(0," + repr(project) + ");"
    "import dual_copy as d;g=d.ExclusiveGuard('transfer');g.__enter__();"
    "print('READY',flush=True);import time;time.sleep(30)"
)
child = subprocess.Popen([sys.executable, '-c', code], stdout=subprocess.PIPE, text=True)
try:
    assert child.stdout.readline().strip() == 'READY'
finally:
    child.terminate()
    child.wait(timeout=5)
with app.ExclusiveGuard('transfer'):
    pass

# Exceptions release both process-local and OS locks.
try:
    with app.ExclusiveGuard('transfer'):
        raise ValueError('test')
except ValueError:
    pass
with app.ExclusiveGuard('transfer'):
    pass

with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as temp:
    base = Path(temp)
    source = base / 'source'
    source.mkdir()
    src = source / 'file'
    src.write_bytes(b'OLD')
    targets = [base / 'a', base / 'b']
    real_utime = os.utime
    changed = [False]
    def mutate_before_final_check(path, *args, **kwargs):
        if str(path).endswith(app.PART) and not changed[0]:
            changed[0] = True
            old = src.stat()
            src.write_bytes(b'NEW')
            real_utime(src, ns=(old.st_atime_ns, old.st_mtime_ns + 1_000_000_000))
        return real_utime(path, *args, **kwargs)
    os.utime = mutate_before_final_check
    try:
        try:
            app.copy_tree(source, targets, lambda e: None, threading.Event())
            raise AssertionError('Source mutation was not detected')
        except OSError as error:
            assert 'Source changed during finalization' in str(error)
    finally:
        os.utime = real_utime
    assert not any((t / 'file').exists() for t in targets)
    assert not any((t / ('file' + app.META)).exists() for t in targets)
    app.copy_tree(source, targets, lambda e: None, threading.Event())
    assert all((t / 'file').read_bytes() == b'NEW' for t in targets)

    # Even the remaining check-to-rename window cannot stamp old data as new.
    src.write_bytes(b'OLD')
    real_replace = os.replace
    changed[0] = False
    def mutate_after_final_check(part, dest):
        if str(part).endswith(app.PART) and not changed[0]:
            changed[0] = True
            old = src.stat()
            src.write_bytes(b'NEW')
            real_utime(src, ns=(old.st_atime_ns, old.st_mtime_ns + 1_000_000_000))
        return real_replace(part, dest)
    os.replace = mutate_after_final_check
    try:
        try:
            app.copy_tree(source, targets, lambda e: None, threading.Event())
        except OSError as error:
            assert 'Source changed during finalization' in str(error)
    finally:
        os.replace = real_replace
    assert (targets[0] / 'file').read_bytes() == b'OLD'
    assert not app.matched(targets[0] / 'file', src.stat())
    app.copy_tree(source, targets, lambda e: None, threading.Event())
    assert all((t / 'file').read_bytes() == b'NEW' for t in targets)

print('PASS: guards reject concurrent threads/processes, release after crash/exception; source changes during finalization are detected and cannot disguise stale data on restart')
