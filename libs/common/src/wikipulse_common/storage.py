import tempfile
from pathlib import Path

MARKER_FILE = ".wikipulse-data-root"


class DataRootUnavailable(RuntimeError):
    pass


def require_data_root(path: Path) -> Path:
    # The marker distinguishes the mounted drive from the empty mountpoint directory left when the
    # external disk is unplugged, which would otherwise silently fill the system disk.
    if not path.is_dir():
        raise DataRootUnavailable(f"data root {path} does not exist")
    if not (path / MARKER_FILE).is_file():
        raise DataRootUnavailable(f"data root {path} has no {MARKER_FILE} marker; is the drive mounted?")
    try:
        with tempfile.NamedTemporaryFile(dir=path, prefix=".write-check-"):
            pass
    except OSError as exc:
        raise DataRootUnavailable(f"data root {path} is not writable: {exc}") from exc
    return path
