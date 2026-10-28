import os

import pytest

from wikipulse_common.storage import MARKER_FILE, DataRootUnavailable, require_data_root


def test_missing_directory(tmp_path):
    with pytest.raises(DataRootUnavailable, match="does not exist"):
        require_data_root(tmp_path / "unmounted")


def test_empty_mountpoint_without_marker(tmp_path):
    with pytest.raises(DataRootUnavailable, match="marker"):
        require_data_root(tmp_path)


def test_mounted_and_writable(tmp_path):
    (tmp_path / MARKER_FILE).touch()
    assert require_data_root(tmp_path) == tmp_path
    assert sorted(p.name for p in tmp_path.iterdir()) == [MARKER_FILE]


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores directory permissions")
def test_read_only_root(tmp_path):
    (tmp_path / MARKER_FILE).touch()
    tmp_path.chmod(0o500)
    try:
        with pytest.raises(DataRootUnavailable, match="not writable"):
            require_data_root(tmp_path)
    finally:
        tmp_path.chmod(0o700)
