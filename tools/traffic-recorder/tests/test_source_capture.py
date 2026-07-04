"""
Regression test for issue #312: SourceCapture used to assign its stop
Event to `self._stop`, which shadows threading.Thread's own private
`_stop()` method. Thread.join() calls that method internally once the
thread has finished, so joining a completed SourceCapture always raised
`TypeError: 'Event' object is not callable`.
"""

from __future__ import annotations

import importlib.util
import io
import os
import socket
import sys
import threading

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOL_DIR = os.path.dirname(_HERE)


def _load_main():
    spec = importlib.util.spec_from_file_location(
        "traffic_recorder_main",
        os.path.join(_TOOL_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["traffic_recorder_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()
SourceCapture = _mod.SourceCapture


class TestSourceCaptureJoin:
    def test_join_after_natural_exit_does_not_raise(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        host, port = server.getsockname()

        def _accept_then_close():
            conn, _ = server.accept()
            conn.close()

        acceptor = threading.Thread(target=_accept_then_close, daemon=True)
        acceptor.start()

        stop_event = threading.Event()
        capture = SourceCapture(
            host=host,
            port=port,
            source_tag="1090",
            output_lock=threading.Lock(),
            output_file=io.StringIO(),
            stop_event=stop_event,
        )
        capture.start()

        # Server closes the connection immediately -> recv() returns b"" ->
        # _capture_1090 breaks out -> run() finishes naturally, same as the
        # "Duration reached" shutdown path in the bug report.
        capture.join(timeout=5)
        acceptor.join(timeout=5)

        assert not capture.is_alive()
        assert capture.error is None

    def test_join_after_stop_event_set_does_not_raise(self):
        """Covers the actual reported scenario: main() sets stop_event, then
        joins every thread."""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        host, port = server.getsockname()

        def _accept_and_hold():
            conn, _ = server.accept()
            stop_event.wait(5)
            conn.close()

        stop_event = threading.Event()
        acceptor = threading.Thread(target=_accept_and_hold, daemon=True)
        acceptor.start()

        capture = SourceCapture(
            host=host,
            port=port,
            source_tag="1090",
            output_lock=threading.Lock(),
            output_file=io.StringIO(),
            stop_event=stop_event,
        )
        capture.start()

        stop_event.set()
        capture.join(timeout=5)
        acceptor.join(timeout=5)

        assert not capture.is_alive()
