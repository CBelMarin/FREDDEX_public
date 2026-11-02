# -*- coding: utf-8 -*-
# FREDDEX - relays the pipeline's existing print() progress messages to a live GUI view
# Copyright (c) 2026 INSERM UMR 1112
# Author: Camille Beluffi-Marin
# Licensed under the MIT License
#
# Every processing step in FREDDEX already reports its progress with plain print() calls
# ("Processing of Diagnostics sheet...", "Warning the patient X has multiple diagnosis
# blocks...", "Process 1 finished.", etc.) -- but today those messages only ever reach the
# log file (once setup_logging() has redirected stdout there) or a bare console, never the
# waiting window the user is actually looking at, which just shows a spinning progress bar
# for however long the import takes.
#

import queue
import re
import sys


_PROGRESS_LINE = re.compile(r"^PROGRESS:([A-Za-z_]+):(\d+)/(\d+)$")


class ProgressRelay:
    """
    A stdout-like object (implements write()/flush()) that:
    1) forwards every write unchanged to the previous stdout, so existing behaviour
       (console output, or the log file once setup_logging() has redirected stdout) is
       preserved exactly as before;
    2) splits the incoming text into lines and pushes each non-empty, stripped line onto a
       thread-safe queue, for a GUI running on a different thread to read as a live log --
       EXCEPT for structured "PROGRESS:stage:current/total" markers, which are parsed and
       routed to a separate queue instead (drain_progress()) so they never show up as raw
       text in the human-readable log panel.
    """

    def __init__(self, previous_stdout):
        self._previous_stdout = previous_stdout
        self._buffer = ""
        self.queue = queue.Queue()
        self.progress_queue = queue.Queue()

    def write(self, text):
        self._previous_stdout.write(text)
        self._buffer += text
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            line = line.strip()
            if not line:
                continue
            match = _PROGRESS_LINE.match(line)
            if match:
                stage, current, total = match.groups()
                self.progress_queue.put((stage, int(current), int(total)))
            else:
                self.queue.put(line)
        return len(text)

    def flush(self):
        self._previous_stdout.flush()

    def drain(self):
        """Returns (and removes from the queue) every log line currently queued, without
        blocking. Never includes PROGRESS markers -- see drain_progress() for those."""
        lines = []
        while True:
            try:
                lines.append(self.queue.get_nowait())
            except queue.Empty:
                break
        return lines

    def drain_progress(self):
        """Returns (and removes from the queue) every (stage, current, total) progress update
        currently queued, without blocking, oldest first."""
        updates = []
        while True:
            try:
                updates.append(self.progress_queue.get_nowait())
            except queue.Empty:
                break
        return updates


def run_with_relay(fonction, *args, on_relay_ready=None, **kwargs):
    """
    Runs fonction(*args, **kwargs) with a ProgressRelay installed as sys.stdout for the
    duration of the call (restored afterwards, even if fonction raises), and returns
    (result, relay).

    on_relay_ready, if given, is called with the relay object immediately after it is
    created -- i.e. BEFORE fonction() starts running, not after it returns. This is what
    lets a caller poll relay.drain() for genuinely live progress: the (result, relay) tuple
    is only available once fonction() has already finished, which is too late for a caller
    on another thread that wants to display progress *while* fonction() is still running.
    """
    relay = ProgressRelay(sys.stdout)
    previous_stdout = sys.stdout
    sys.stdout = relay
    if on_relay_ready is not None:
        on_relay_ready(relay)
    try:
        result = fonction(*args, **kwargs)
    finally:
        sys.stdout = previous_stdout
    return result, relay
