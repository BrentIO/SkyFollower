"""
Tests for map/metar.py, GET /api/metar and the METAR MQTT statistics. No
live weather site and no live Redis: downloads and Redis are stand-ins.
"""

from __future__ import annotations

import gzip
import io
import json
import time
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import map.main as map_main
from map import metar
from shared.mqtt_presence import MqttPresence

_HOME = (39.3, -94.7)
_HEADER = (
    "raw_text,station_id,observation_time,latitude,longitude,temp_c,dewpoint_c,"
    "wind_dir_degrees,wind_speed_kt,wind_gust_kt,visibility_statute_mi,altim_in_hg,"
    "wx_string,sky_cover,cloud_base_ft_agl,sky_cover,cloud_base_ft_agl,flight_category"
)


def _iso(age_seconds=0):
    return datetime.fromtimestamp(time.time() - age_seconds, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _row(station="KMCI", lat=39.3, lon=-94.73, category="VFR", age=60, raw=None,
         wind="360", speed="12", gust="", vis="10+", wx="", clouds=("FEW", "7000", "BKN", "15000")):
    raw = raw or f"METAR {station} 121753Z 36012KT 10SM FEW070 BKN150 24/19 A2999"
    return ",".join([raw, station, _iso(age), str(lat), str(lon), "24.4", "19.4", wind, speed,
                     gust, vis, "29.99", wx, *clouds, category])


def _csv(*rows):
    return "\n".join([_HEADER, *rows]).encode()


def _parse(*rows):
    return metar.parse_reports(_csv(*rows), *_HOME)


_STATIONS = json.dumps([
    {"icaoId": "KMCI", "site": "Kansas City Intl", "state": "MO", "country": "US"},
    {"icaoId": None, "site": "Buoy"},
    {"icaoId": "EGLL", "site": "Heathrow", "state": "", "country": "GB"},
]).encode()


class _Redis:
    def __init__(self):
        self.values, self.expiry = {}, {}

    def set(self, key, value, ex=None):
        self.values[key], self.expiry[key] = value, ex

    def get(self, key):
        return self.values.get(key)


def _store(answers, home=_HOME):
    """answers: file name -> bytes or Exception; may be changed mid-test."""
    calls = []

    def fetch(url):
        name = url.rsplit("/", 1)[1]
        calls.append(name)
        if isinstance(answers[name], Exception):
            raise answers[name]
        return answers[name]

    return metar.MetarStore(_Redis(), *home, base_url="http://weather.invalid", fetch=fetch), calls


def _answers(**overrides):
    answers = {metar.REPORTS_FILE: _csv(_row()), metar.STATIONS_FILE: _STATIONS}
    answers.update({{"reports": metar.REPORTS_FILE, "stations": metar.STATIONS_FILE}[k]: v
                    for k, v in overrides.items()})
    return answers


class TestParseReports:
    def test_decodes_a_report_including_every_repeated_cloud_column(self):
        (report,) = _parse(_row(gust="20", wx="-RA BR"))
        assert report["station_id"] == "KMCI"
        assert report["flight_category"] == "VFR"
        assert report["clouds"] == [{"cover": "FEW", "base_ft": 7000}, {"cover": "BKN", "base_ft": 15000}]
        assert (report["wind_dir_deg"], report["wind_speed_kt"], report["wind_gust_kt"]) == (360, 12, 20)
        assert (report["visibility_sm"], report["weather"]) == ("10+", "-RA BR")
        assert (report["temperature_c"], report["altimeter_inhg"]) == (24.4, 29.99)

    def test_missing_values_are_omitted(self):
        (report,) = _parse(_row(wind="", speed="", vis="", clouds=("", "", "", ""), category=""))
        for field in ("flight_category", "wind_dir_deg", "wind_speed_kt", "wind_gust_kt",
                      "visibility_sm", "weather", "clouds", "sky_clear"):
            assert field not in report

    @pytest.mark.parametrize("category", ["null", "UNKNOWN", ""])
    def test_unrecognised_category_is_omitted(self, category):
        assert "flight_category" not in _parse(_row(category=category))[0]

    def test_variable_wind_has_no_direction(self):
        (report,) = _parse(_row(wind="0", speed="3", raw="METAR KMCI 121753Z VRB03KT 10SM CLR"))
        assert report["wind_variable"] is True
        assert "wind_dir_deg" not in report
        assert report["sky_clear"] == "CLR"

    def test_remarks_do_not_count_as_sky_or_wind(self):
        (report,) = _parse(_row(raw="METAR KMCI 121753Z 36012KT 10SM RMK CLR VRB02KT"))
        assert "sky_clear" not in report and "wind_variable" not in report

    def test_only_stations_within_250_nm(self):
        near = _row("KNEA", lat=_HOME[0] + 249 / 60, lon=_HOME[1])
        far = _row("KFAR", lat=_HOME[0] + 251 / 60, lon=_HOME[1])
        assert [r["station_id"] for r in _parse(near, far)] == ["KNEA"]

    def test_newest_report_per_station_wins(self):
        (report,) = _parse(_row(category="IFR", age=600), _row(category="VFR", age=60))
        assert report["flight_category"] == "VFR"

    def test_unusable_rows_are_skipped(self):
        rows = (_row("KBAD", lat=""), "short,row", _row())
        assert [r["station_id"] for r in _parse(*rows)] == ["KMCI"]

    @pytest.mark.parametrize("data", [b"", b"<html>blocked</html>", _HEADER.encode()])
    def test_unusable_file_raises(self, data):
        with pytest.raises(ValueError):
            metar.parse_reports(data, *_HOME)

    def test_a_file_with_no_nearby_station_is_not_an_error(self):
        assert _parse(_row("EGLL", lat=51.47, lon=-0.45)) == []


def test_download_sends_user_agent_and_decompresses():
    response = MagicMock()
    response.__enter__.return_value = io.BytesIO(gzip.compress(b"payload"))
    with patch("urllib.request.urlopen", return_value=response) as urlopen:
        assert metar.download("https://example.invalid/x.gz") == b"payload"
    assert urlopen.call_args.args[0].get_header("User-agent") == "P5Software SkyFollower"
    assert urlopen.call_args.kwargs["timeout"] == 30


def test_station_names():
    assert metar.parse_station_names(_STATIONS) == {
        "KMCI": "Kansas City Intl, MO, US", "EGLL": "Heathrow, GB",
    }


class TestMetarStore:
    def test_poll_stores_named_reports_with_a_ttl(self):
        store, _ = _store(_answers())
        store.poll()
        body = store.get()
        assert body["max_age_seconds"] == 5400
        assert body["stations"][0]["name"] == "Kansas City Intl, MO, US"
        assert store._redis.expiry[metar.METAR_KEY] == 5400
        assert store.consecutive_failures == 0 and store.last_success_at

    @pytest.mark.parametrize("failure", [OSError("down"), b"not a csv"])
    def test_failed_download_keeps_the_last_good_data(self, failure):
        answers = _answers()
        store, _ = _store(answers)
        store.poll()
        succeeded_at = store.last_success_at
        answers[metar.REPORTS_FILE] = failure
        store.poll()
        store.poll()
        assert [s["station_id"] for s in store.get()["stations"]] == ["KMCI"]
        assert store.consecutive_failures == 2
        assert store.last_success_at == succeeded_at
        answers[metar.REPORTS_FILE] = _csv(_row())
        store.poll()
        assert store.consecutive_failures == 0

    def test_reports_older_than_90_minutes_are_not_served(self):
        store, _ = _store(_answers(reports=_csv(
            _row("KNEW", age=5340), _row("KOLD", age=5460))))
        store.poll()
        assert [s["station_id"] for s in store.get()["stations"]] == ["KNEW"]

    def test_station_name_failure_does_not_block_reports_and_is_retried(self):
        answers = _answers(stations=OSError("down"))
        store, calls = _store(answers)
        store.poll()
        assert "name" not in store.get()["stations"][0]
        assert store.consecutive_failures == 0
        answers[metar.STATIONS_FILE] = _STATIONS
        store.poll()
        assert store.get()["stations"][0]["name"]
        assert calls.count(metar.STATIONS_FILE) == 2

    def test_station_names_are_downloaded_once_a_day(self):
        store, calls = _store(_answers())
        store.poll()
        store.poll()
        store._names_fetched_at -= 86401
        store.poll()
        assert calls.count(metar.STATIONS_FILE) == 2
        assert calls.count(metar.REPORTS_FILE) == 3

    @pytest.mark.parametrize("home", [(None, None), (39.3, None)])
    def test_no_center_fetches_and_serves_nothing(self, home):
        store, calls = _store(_answers(), home=home)
        store.poll()
        assert calls == []
        assert store.get()["stations"] == []


def test_api_endpoint_returns_the_store(monkeypatch):
    store, _ = _store(_answers())
    store.poll()
    monkeypatch.setattr(map_main, "_metar", store)
    assert map_main.get_metar()["stations"][0]["station_id"] == "KMCI"


class TestMqttStatistics:
    def _presence(self):
        client = MagicMock()
        with patch("shared.mqtt_presence.build_mqtt_client", return_value=client):
            presence = MqttPresence(
                {"host": "broker"}, component="map", device_identifier="SkyFollower_map",
                device_name="SkyFollower Map", device_model="Map",
                statistics=map_main._METAR_STATISTICS,
            )
            presence.start()
        return presence, client

    def _published(self, client):
        return {c.args[0]: c.args[1] for c in client.publish.call_args_list}

    def test_values_set_before_connecting_are_published_on_connect(self, monkeypatch):
        presence, client = self._presence()
        store, _ = _store(_answers())
        store.poll()
        monkeypatch.setattr(map_main, "_metar", store)
        monkeypatch.setattr(map_main, "_mqtt_presence", presence)
        map_main._publish_metar_statistics()
        client.publish.assert_not_called()
        presence._on_connect(client, None, None, 0, None)
        published = self._published(client)
        assert published["SkyFollower/map/statistic/metar_consecutive_failures"] == "0"
        assert published["SkyFollower/map/statistic/metar_last_download_at"].endswith("+00:00")
        for name in ("metar_last_download_at", "metar_consecutive_failures"):
            config = json.loads(published[f"homeassistant/sensor/SkyFollower_map_{name}/config"])
            assert config["state_topic"] == f"SkyFollower/map/statistic/{name}"
            assert config["availability_topic"] == "SkyFollower/map/status"

    def test_no_last_download_time_until_one_succeeds(self, monkeypatch):
        presence, client = self._presence()
        presence._on_connect(client, None, None, 0, None)
        store, _ = _store(_answers(reports=OSError("down")))
        store.poll()
        monkeypatch.setattr(map_main, "_metar", store)
        monkeypatch.setattr(map_main, "_mqtt_presence", presence)
        map_main._publish_metar_statistics()
        published = self._published(client)
        assert published["SkyFollower/map/statistic/metar_consecutive_failures"] == "1"
        assert "SkyFollower/map/statistic/metar_last_download_at" not in published
