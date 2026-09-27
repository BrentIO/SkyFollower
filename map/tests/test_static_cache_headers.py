"""
Unit tests for map/main.py's _SPAStaticFiles.file_response() override
(issue #2054) -- mounts it standalone against a throwaway tmp_path
directory rather than the real frontend build, so no Redis/subprocess is
needed (contrast test_api.py's heavier integration style).
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

import map.main as map_main


def _client(dist_dir) -> TestClient:
    (dist_dir / "index.html").write_text("<html></html>")
    assets_dir = dist_dir / "assets"
    assets_dir.mkdir()
    (assets_dir / "index-D09Lu96.js").write_text("console.log('hashed')")
    (assets_dir / "maplibre-gl-worker.mjs").write_text("// worker")
    (assets_dir / "maplibre-gl-shared.mjs").write_text("// shared")

    app = FastAPI()
    app.mount("/map", map_main._SPAStaticFiles(directory=str(dist_dir), html=True), name="map-frontend")
    return TestClient(app)


def test_hashed_asset_gets_immutable_cache_control(tmp_path):
    response = _client(tmp_path).get("/map/assets/index-D09Lu96.js")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "public, max-age=31536000, immutable"


def test_index_html_gets_no_cache(tmp_path):
    response = _client(tmp_path).get("/map/index.html")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-cache"


def test_maplibre_worker_file_gets_no_cache_not_immutable(tmp_path):
    response = _client(tmp_path).get("/map/assets/maplibre-gl-worker.mjs")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-cache"


def test_maplibre_shared_file_gets_no_cache_not_immutable(tmp_path):
    response = _client(tmp_path).get("/map/assets/maplibre-gl-shared.mjs")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-cache"


def test_conditional_get_304_still_carries_cache_control(tmp_path):
    client = _client(tmp_path)
    first = client.get("/map/assets/index-D09Lu96.js")
    etag = first.headers["etag"]

    second = client.get("/map/assets/index-D09Lu96.js", headers={"if-none-match": etag})
    assert second.status_code == 304
    assert second.headers["cache-control"] == "public, max-age=31536000, immutable"
