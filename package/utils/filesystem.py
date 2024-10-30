from pathlib import Path

import os
import shutil


def get_file_name(path: str) -> str:
    basename = os.path.basename(path)
    name, ext = os.path.splitext(basename)
    return name


def find_up(path: str, pattern: str):
    path = Path(path)

    if path.is_file():
        return find_up(path.parent, pattern)

    matches = list(path.glob(pattern))

    if matches:
        return matches[0]

    if path == path.parent:
        return None

    return find_up(path.parent, pattern)


def copy(src: str, dst: str):
    if os.path.isdir(src):
        if os.path.exists(dst):
            shutil.rmtree(dst)
        shutil.copytree(src, dst, symlinks=True)
    else:
        shutil.copy2(src, dst)


def symlink(src: str, dst: str):
    if os.path.islink(dst):
        os.unlink(dst)
    os.symlink(src, dst)


def rmtree(path: str):
    if os.path.exists(path):
        shutil.rmtree(path)


def touch(path: str):
    with open(path, "a"):
        os.utime(path, None)
