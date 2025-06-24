"""
Small helper that configures *all* logging so each import only has to do::

    from etl.utils.log import get_logger
    log = get_logger(__name__)
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Union


_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def configure_logging(log_file: Union[str, Path], level: int = logging.INFO) -> None:
    """
    Replace the root logger’s handlers so every module (even those that already
    called logging.basicConfig) writes *both* to console and to `log_file`.
    """
    log_file = Path(log_file).expanduser()
    root = logging.getLogger()

    # Wipe any previous handlers (modules that called basicConfig earlier)
    for h in list(root.handlers):
        root.removeHandler(h)

    root.setLevel(level)

    # Console
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter(_FMT))
    root.addHandler(ch)

    # File
    fh = logging.FileHandler(log_file)
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter(_FMT))
    root.addHandler(fh)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
