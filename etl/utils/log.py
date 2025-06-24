"""
Small helper that configures *all* logging so each import only has to do::

    from etl.utils.log import get_logger
    log = get_logger(__name__)
"""
from __future__ import annotations

import logging
import datetime as dt
from pathlib import Path
from typing import Union
from zoneinfo import ZoneInfo

_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
# Default date-format & time-zone (overridable by caller)
_DATE_FMT = "%Y-%m-%d %H:%M:%S"
_TZ       = "Europe/Athens"

def configure_logging(
    log_file: Union[str, Path],
    level: int  = logging.INFO,
    datefmt: str = _DATE_FMT,
    tz:      str = _TZ,
) -> None:
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

        # Build a formatter that renders *asctime* in the requested TZ
        tzinfo = ZoneInfo(tz)

        class _TZFormatter(logging.Formatter):
            def converter(self, timestamp):                          # type: ignore[override]
                return dt.datetime.fromtimestamp(timestamp, tzinfo).timetuple()

        fmt = _TZFormatter(_FMT, datefmt=datefmt)

    # Console
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(fmt)
    root.addHandler(ch)

    # File
    fh = logging.FileHandler(log_file)
    fh.setLevel(level)
    ch.setFormatter(fmt)
    root.addHandler(fh)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
