"""Docs generation and the local docs server."""

import threading
import urllib.request

import pytest

from openneuro_pipeline.errors import TransformError
from openneuro_pipeline.extract import ExtractParams
from openneuro_pipeline.transform.docs_server import make_server


def test_generated_site_is_complete_and_served(pipeline, mocked_api):
    pipeline.run(ExtractParams(), transform=True)
    site = pipeline.dbt.generate_docs()

    server = make_server(site, port=0)  # port 0: any free port
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = server.server_address[1]
        for name in ("index.html", "manifest.json", "catalog.json"):
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/{name}") as resp:
                assert resp.status == 200
    finally:
        server.shutdown()
        server.server_close()


def test_incomplete_site_is_rejected(tmp_path):
    (tmp_path / "index.html").write_text("<html></html>")
    with pytest.raises(TransformError, match="catalog.json"):
        make_server(tmp_path, port=0)


def _complete_site(tmp_path):
    for name in ("index.html", "manifest.json", "catalog.json"):
        (tmp_path / name).write_text("{}")
    return tmp_path


def test_ctrl_c_stops_server_quietly(tmp_path, monkeypatch):
    from http.server import ThreadingHTTPServer

    from openneuro_pipeline.transform import docs_server

    opened = []
    monkeypatch.setattr(docs_server.webbrowser, "open", opened.append)

    def interrupted(self, *args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(ThreadingHTTPServer, "serve_forever", interrupted)

    docs_server.serve(_complete_site(tmp_path), port=0)  # returns, no traceback
    assert opened and opened[0].startswith("http://localhost:")


def test_port_in_use_is_reported(tmp_path):
    first = make_server(_complete_site(tmp_path), port=0)
    try:
        with pytest.raises(TransformError, match="Cannot serve docs on port"):
            make_server(tmp_path, port=first.server_address[1])
    finally:
        first.server_close()
