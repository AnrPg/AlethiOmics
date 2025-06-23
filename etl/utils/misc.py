#!/usr/bin/env python3

import time
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from collections import defaultdict

# Module‐level state to track counts and filenames per logfile prefix
_log_counters = defaultdict(int)
_logfile_names = {}

# Set the locale for time‐based formatting
# e.g. for Greek (as used in Europe/Athens timezone):
now_athens = datetime.now(ZoneInfo("Europe/Athens"))
# or for U.S. English:
# locale.setlocale(locale.LC_TIME, 'en_US.UTF-8')

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
        collapse_size (int): Number of messages to join on one line.

    Returns:
        str: Path to the logfile used for this run.
    """
    # Ensure we have a string
    if not isinstance(message, str):
        message = str(message)

    # Determine or create the logfile name once per prefix
    if add_timestamp:
        if logfile_path not in _logfile_names:
            # Split name and extension to insert timestamp
            base, ext = os.path.splitext(logfile_path)
            ts = now_athens.strftime("%Y-%m-%d at %H_%M_%S")
            _logfile_names[logfile_path] = f"{base}_{ts}{ext}"
        logfile = _logfile_names[logfile_path]
    else:
        # Use the provided path exactly, without altering its extension
        logfile = logfile_path

    # If message is multi-line, bypass collapsing and write as-is
    if "\n" in message:
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
