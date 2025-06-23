#!/usr/bin/env python3

import os
from datetime import datetime
from zoneinfo import ZoneInfo
from collections import defaultdict

# Module‐level state to track counts and filenames per logfile prefix
_log_counters = defaultdict(int)
_logfile_names = {}

# Precompute Athens timestamp once
_NOW_ATHENS = datetime.now(ZoneInfo("Europe/Athens"))

def create_timestamped_filename(prefix: str) -> str:
    """
    Generate a timestamped filename for the given prefix, caching per prefix.
    Returns the full filename including extension.
    """
    if prefix not in _logfile_names:
        base, ext = os.path.splitext(prefix)
        ts = _NOW_ATHENS.strftime("%Y-%m-%d at %H_%M_%S")
        _logfile_names[prefix] = f"{base}_{ts}{ext}"
    return _logfile_names[prefix]


def print_and_log(
    message,
    add_timestamp=True,
    logfile_path="./main",
    also_show_to_screen=True,
    collapse_size=100,
) -> str:
    """
    Print and log messages, collapsing up to `collapse_size` messages per line.

    If `message` contains multiple lines, bypass collapsing and
    output the message in full (preserving its line breaks).

    Args:
        message (str): Text to print and log.
        add_timestamp (bool): Create a timestamped logfile on first use.
        logfile_path (str): Exact filename or path to use; extension is preserved.
        also_show_to_screen (bool): If True, echo output to console.
        collapse_size (int): Number of messages to join on one line;
                             if 0, disables collapsing entirely.

    Returns:
        str: Path to the logfile used for this run.
    """
    # Ensure we have a string
    if not isinstance(message, str):
        message = str(message)

    # Determine logfile name
    if add_timestamp:
        logfile = create_timestamped_filename(logfile_path)
    else:
        logfile = logfile_path

    # If collapsing is disabled, just write and print the message as-is
    if collapse_size == 0 or "\n" in message:
        with open(logfile, "a") as f:
            f.write(message + "\n")
        if also_show_to_screen:
            print(message)
        return logfile

    # Single-line message: apply collapsing logic
    _log_counters[logfile] += 1
    count = _log_counters[logfile]
    sep = "\t|\t"

    # Determine prefix for log
    log_prefix = ""
    if count % collapse_size == 1 and count > 1:
        log_prefix = "\n"
    elif count % collapse_size != 1:
        log_prefix = sep

    # Append message to logfile
    with open(logfile, "a") as f:
        f.write(f"{log_prefix}{message}")

    # Echo to console
    if also_show_to_screen:
        if count % collapse_size == 1 and count > 1:
            print()
            print(message, end="")
        elif count % collapse_size == 1:
            print(message, end="")
        else:
            print(sep + message, end="")
        if count % collapse_size == 0:
            print()
        try:
            import sys; sys.stdout.flush()
        except Exception:
            pass

    return logfile
