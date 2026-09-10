"""Private files and foreground child handles for the native installation consumer."""
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import selectors
import shlex
import signal
import stat
import subprocess
import time
import uuid


class NativeFailure(RuntimeError):
    """A safe classification; never carries command output or credential values."""


class CommandFailure(NativeFailure):
    def __init__(self, classification):
        super().__init__("owner-command-failed")
        self.classification = classification


def absolute(value):
    text = str(value)
    if (not text.startswith("/") or len(text) > 2048 or "//" in text
            or text.endswith("/") or any(ord(char) < 32 or ord(char) == 127 for char in text)
            or any(part in (".", "..") for part in text.split("/"))):
        raise NativeFailure("invalid-path")
    return Path(text)


def ancestors(path):
    for item in path.parents:
        try:
            metadata = item.lstat()
        except FileNotFoundError:
            continue
        if not stat.S_ISDIR(metadata.st_mode):
            raise NativeFailure("unsafe-path-ancestor")


def private_directory(path, *, create=False):
    path = absolute(path)
    ancestors(path)
    if create:
        try:
            path.mkdir(mode=0o700)
        except FileExistsError:
            pass
    metadata = path.lstat()
    if (not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) != 0o700):
        raise NativeFailure("directory-not-private")
    return path


def metadata_identity(metadata):
    return (metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mode,
            metadata.st_nlink, metadata.st_mtime_ns, metadata.st_ctime_ns)


@contextmanager
def checked_file(path, maximum, *, private=False, executable=False):
    path = absolute(path)
    ancestors(path)
    before = path.lstat()
    if (not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= maximum
            or before.st_nlink != 1 or before.st_mode & 0o022
            or (executable and not before.st_mode & 0o111)
            or (private and (before.st_uid != os.getuid() or stat.S_IMODE(before.st_mode) != 0o600))):
        raise NativeFailure("unsafe-file")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        if metadata_identity(before) != metadata_identity(os.fstat(descriptor)):
            raise NativeFailure("file-changed")
        yield descriptor, before
        if (metadata_identity(before) != metadata_identity(os.fstat(descriptor))
                or metadata_identity(before) != metadata_identity(path.lstat())):
            raise NativeFailure("file-changed")
    finally:
        os.close(descriptor)


def read_file(path, maximum, *, private=False):
    with checked_file(path, maximum, private=private) as (descriptor, metadata):
        chunks = bytearray()
        while len(chunks) <= maximum:
            block = os.read(descriptor, min(65536, maximum + 1 - len(chunks)))
            if not block:
                break
            chunks.extend(block)
        if len(chunks) != metadata.st_size:
            raise NativeFailure("file-changed")
        return bytes(chunks)


def decode_json(content):
    def fields(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise NativeFailure("duplicate-json-field")
            result[key] = value
        return result
    return json.loads(content, object_pairs_hook=fields,
                      parse_constant=lambda _: (_ for _ in ()).throw(NativeFailure("invalid-json")))


def sync_directory(directory):
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def freeze(directory, name, content):
    """The caller holds installation.lock. Existing declarations are never replaced."""
    destination = directory / name
    if destination.exists() or destination.is_symlink():
        if read_file(destination, 1048576, private=True) != content:
            raise NativeFailure("frozen-installation-drift")
        return destination
    temporary = directory / (".pending-" + uuid.uuid4().hex)
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.rename(temporary, destination)
        sync_directory(directory)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


@contextmanager
def lock(directory, name):
    private_directory(directory)
    descriptor = os.open(directory / name, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        metadata = os.fstat(descriptor)
        if (not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1
                or metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o600):
            raise NativeFailure("unsafe-lock")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise NativeFailure("installation-command-already-active") from None
        yield
    finally:
        os.close(descriptor)


def verify_artifact(item):
    with checked_file(Path(item["path"]), 536870912, executable=item["executable"]) as (descriptor, metadata):
        digest = hashlib.sha256()
        count = 0
        while block := os.read(descriptor, 65536):
            count += len(block)
            if count > 536870912:
                raise NativeFailure("artifact-exceeds-bound")
            digest.update(block)
        if count != metadata.st_size or "sha256:" + digest.hexdigest() != item["bytes_digest"]:
            raise NativeFailure("artifact-drift")


def clean_environment(home):
    # No inherited cloud/database credentials, proxy variables, runtime injections or SDK home.
    return {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "HOME": str(home),
            "TMPDIR": str(home), "LANG": "C.UTF-8"}


def process_environment(path, temporary):
    text = read_file(path, 262144, private=True).decode("utf-8")
    environment = clean_environment(private_directory(temporary))
    allowed = {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_EC2_METADATA_DISABLED",
               "AWS_SHARED_CREDENTIALS_FILE", "AWS_PROFILE", "SSL_CERT_FILE", "SSL_CERT_DIR"}
    seen = set()
    for line in text.splitlines():
        if not line or "=" not in line:
            raise NativeFailure("invalid-role-environment")
        key, encoded = line.split("=", 1)
        if (not key.isascii() or not key.replace("_", "").isalnum() or key != key.upper()
                or (not key.startswith("PLATFORM_") and key not in allowed) or key in seen):
            raise NativeFailure("invalid-role-environment")
        values = shlex.split(encoded, comments=False, posix=True)
        if len(values) != 1 or len(values[0]) > 16384 or "\0" in values[0]:
            raise NativeFailure("invalid-role-environment")
        seen.add(key)
        environment[key] = values[0]
    return environment


class Child:
    def __init__(self, process, log, required, capture):
        self.process, self.log, self.required, self.capture = process, log, required, capture
        self.output = bytearray()
        self.error = bytearray()
        self.logged = 0
        self.open_streams = 2


class ProcessGroup:
    """Only live Popen handles confer ownership. Pipes and disk logs have explicit bounds."""
    def __init__(self, logs, *, log_bound=4 * 1024 * 1024):
        self.logs = private_directory(logs, create=True)
        self.log_bound = log_bound
        self.selector = selectors.DefaultSelector()
        self.children = []
        self.stopping = False
        self.interrupted = False
        self.closed = False

    def start(self, arguments, environment, *, required=False, capture=False, cwd=None):
        self.check()
        log_path = self.logs / (uuid.uuid4().hex + ".log")
        descriptor = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        log = os.fdopen(descriptor, "wb", buffering=0)
        try:
            process = subprocess.Popen(arguments, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE, env=environment, cwd=cwd, close_fds=True)
        except BaseException:
            log.close()
            raise
        child = Child(process, log, required, capture)
        self.children.append(child)
        for stream, error in ((process.stdout, False), (process.stderr, True)):
            os.set_blocking(stream.fileno(), False)
            self.selector.register(stream, selectors.EVENT_READ, (child, error))
        return child

    def check(self):
        if not self.stopping:
            if self.interrupted:
                raise NativeFailure("interrupted")
            if any(child.required and child.process.poll() is not None for child in self.children):
                raise NativeFailure("required-process-exited")

    def pump(self, duration=0.1):
        self.check()
        for key, _ in self.selector.select(duration):
            child, error = key.data
            block = os.read(key.fileobj.fileno(), 65536)
            if not block:
                self.selector.unregister(key.fileobj)
                key.fileobj.close()
                child.open_streams -= 1
                if child.open_streams == 0:
                    child.log.close()
                continue
            remaining = self.log_bound - child.logged
            if remaining > 0 and not self.stopping:
                written = child.log.write(block[:remaining])
                child.logged += written
            if error and not self.stopping and len(child.error) <= 65536:
                child.error.extend(block[:65537 - len(child.error)])
            if child.capture and not error and not self.stopping:
                if len(child.output) + len(block) > 1048576:
                    raise NativeFailure("owner-output-exceeds-bound")
                child.output.extend(block)
        self.check()

    def run(self, arguments, environment, *, timeout=300, capture=False, checked=True):
        child = self.start(arguments, environment, capture=capture)
        deadline = time.monotonic() + timeout
        while child.process.poll() is None or child.open_streams:
            if time.monotonic() >= deadline:
                raise NativeFailure("owner-command-timeout")
            self.pump()
        code = child.process.wait()
        if not checked:
            return subprocess.CompletedProcess(arguments, code, bytes(child.output), bytes(child.error))
        if code != 0:
            # Only owning enum Display values can influence lifecycle recovery. No stderr is shown.
            classification = bytes(child.error).strip()
            raise CommandFailure(classification)
        return bytes(child.output) if capture else None

    def close(self, *, grace=10, kill_grace=3):
        if self.closed:
            return
        self.stopping = True
        errors = []
        for child in self.children:
            if child.process.poll() is None:
                try:
                    child.process.terminate()
                except OSError:
                    errors.append("terminate")
        def drain_until(deadline, streams):
            while (any(child.process.poll() is None or (streams and child.open_streams)
                       for child in self.children) and time.monotonic() < deadline):
                try:
                    self.pump(min(0.05, max(0, deadline - time.monotonic())))
                except (OSError, ValueError, NativeFailure):
                    errors.append("pipe")
                    # A broken pipe or log cannot prevent signalling and reaping other children.
                    time.sleep(min(0.01, max(0, deadline - time.monotonic())))
        deadline = time.monotonic() + grace
        drain_until(deadline, False)
        for child in self.children:
            if child.process.poll() is None:
                try:
                    child.process.kill()
                except OSError:
                    errors.append("kill")
        deadline = time.monotonic() + kill_grace
        drain_until(deadline, True)
        for child in self.children:
            try:
                child.process.wait(timeout=max(0, deadline - time.monotonic()))
            except (OSError, subprocess.TimeoutExpired):
                errors.append("reap")
            for stream in (child.process.stdout, child.process.stderr):
                if not stream.closed:
                    try:
                        self.selector.unregister(stream)
                    except (OSError, ValueError, KeyError):
                        errors.append("unregister")
                    try:
                        stream.close()
                    except OSError:
                        errors.append("close-pipe")
            try:
                child.log.close()
            except OSError:
                errors.append("close-log")
        try:
            self.selector.close()
        except OSError:
            errors.append("close-selector")
        self.closed = True
        if any(child.process.returncode is None for child in self.children):
            raise NativeFailure("owned-process-cleanup-incomplete")
        if errors:
            raise NativeFailure("owned-process-cleanup-io-failure")


@contextmanager
def signals(group):
    previous = {}
    def received(_number, _frame):
        group.interrupted = True
    for number in (signal.SIGINT, signal.SIGTERM):
        previous[number] = signal.signal(number, received)
    try:
        yield
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)
