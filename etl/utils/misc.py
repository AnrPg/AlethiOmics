#!/usr/bin/env python3

import time

def print_and_log(message, add_timestamp=True, logfile_path="./main", also_show_to_screen=True) -> str:
    """Prints a message to the console and logs it to a timestamped file.

    Args:
        message (string): The message to print and log.
        logfile_prefix (string, optional): Path prefix for the log file (without extension). Defaults to './main'.
    Returns:
        str: The path to the log file where the message was written.
    """
    if not isinstance(message, str):
        message = str(message)

    if also_show_to_screen:
        # Print the message to the console
        print(message)

    # Generate timestamped logfile name
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    if add_timestamp:
        logfile_path = logfile_path.rstrip(".log")  # Remove any existing .log extension
        logfile = f"{logfile_path}_{timestamp}.log"
    else:
        logfile = logfile_path.rstrip("_")  # Remove trailing underscore if present

    # Write to log file in chunks of 20 lines per line
    lines = message.splitlines()
    with open(logfile, "a") as log_file:
        for i in range(0, len(lines), 20):
            group = lines[i:i+20]
            combined_line = " | ".join(group)
            log_file.write(combined_line + "\n")
    return logfile