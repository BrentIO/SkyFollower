"""
Unit tests for map/main.py's `_uvicorn_tls_kwargs()` -- the cert-present-vs-
absent branch that decides whether `uvicorn.run()` terminates TLS itself
(scripts/install.sh's --role map cert generation, or an operator's own
cert/key dropped in under the same filenames) or falls back to plain HTTP.
No live Redis needed -- this only touches the filesystem paths the function
checks, monkeypatched to a temp directory.
"""

from __future__ import annotations

import map.main as map_main


def test_tls_kwargs_present_when_both_files_exist(tmp_path, monkeypatch, caplog):
    cert = tmp_path / "cert.pem"
    key = tmp_path / "key.pem"
    cert.write_text("cert")
    key.write_text("key")
    monkeypatch.setattr(map_main, "_TLS_CERT_PATH", str(cert))
    monkeypatch.setattr(map_main, "_TLS_KEY_PATH", str(key))

    result = map_main._uvicorn_tls_kwargs()

    assert result == {"ssl_certfile": str(cert), "ssl_keyfile": str(key)}


def test_tls_kwargs_empty_and_warns_when_cert_missing(tmp_path, monkeypatch, caplog):
    cert = tmp_path / "cert.pem"
    key = tmp_path / "key.pem"
    key.write_text("key")  # Only the key exists -- still an incomplete pair.
    monkeypatch.setattr(map_main, "_TLS_CERT_PATH", str(cert))
    monkeypatch.setattr(map_main, "_TLS_KEY_PATH", str(key))

    with caplog.at_level("WARNING", logger="map"):
        result = map_main._uvicorn_tls_kwargs()

    assert result == {}
    assert "plain HTTP" in caplog.text


def test_tls_kwargs_empty_when_neither_file_exists(tmp_path, monkeypatch):
    monkeypatch.setattr(map_main, "_TLS_CERT_PATH", str(tmp_path / "cert.pem"))
    monkeypatch.setattr(map_main, "_TLS_KEY_PATH", str(tmp_path / "key.pem"))

    assert map_main._uvicorn_tls_kwargs() == {}
