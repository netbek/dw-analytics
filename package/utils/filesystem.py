from pathlib import Path

import os
import shutil


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


def symlink(src: str, dst: str):
    if os.path.islink(dst):
        os.unlink(dst)

    os.symlink(src, dst)


def rmtree(path: str):
    if os.path.exists(path):
        shutil.rmtree(path)
