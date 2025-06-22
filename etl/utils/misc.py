#!/usr/bin/env python3

import time

def print_and_log(message, logfile_prefix="./main", also_show_to_screen=True) -> str:
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
    logfile = f"{logfile_prefix}_{timestamp}.log"

    # Write to log file in chunks of 20 lines per line
    lines = message.splitlines()
    with open(logfile, "a") as log_file:
        for i in range(0, len(lines), 20):
            group = lines[i:i+20]
            combined_line = " | ".join(group)
            log_file.write(combined_line + "\n")
    return logfile