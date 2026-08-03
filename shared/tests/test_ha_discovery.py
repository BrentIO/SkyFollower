import os
from unittest.mock import patch

from shared.ha_discovery import build_ha_device


class TestBuildHaDevice:
    def test_sets_all_fields(self):
        device = build_ha_device(
            identifier="SkyFollower_runner_us_faa_registry",
            name="SkyFollower US FAA Registry Runner",
            model="US FAA Registry Runner",
        )
        assert device["ids"] == "SkyFollower_runner_us_faa_registry"
        assert device["name"] == "SkyFollower US FAA Registry Runner"
        assert device["manufacturer"] == "P5Software, LLC"
        assert device["model"] == "US FAA Registry Runner"
        assert device["configuration_url"] == "https://github.com/BrentIO/SkyFollower"

    def test_configuration_url_override(self):
        device = build_ha_device(
            identifier="x",
            name="x",
            model="x",
            configuration_url="https://brentio.github.io/SkyFollower/runners/us-faa-registry.html",
        )
        assert device["configuration_url"] == "https://brentio.github.io/SkyFollower/runners/us-faa-registry.html"

    def test_sw_version_falls_back_to_dev_when_unset(self):
        with patch.dict(os.environ, {}, clear=True):
            device = build_ha_device(identifier="x", name="x", model="x")
            assert device["sw_version"] == "dev"

    def test_sw_version_reads_from_environment(self):
        with patch.dict(os.environ, {"VERSION": "2026.08.03"}):
            device = build_ha_device(identifier="x", name="x", model="x")
            assert device["sw_version"] == "2026.08.03"

    def test_sw_version_read_fresh_on_every_call(self):
        with patch.dict(os.environ, {"VERSION": "2026.08.01"}):
            first = build_ha_device(identifier="x", name="x", model="x")
        with patch.dict(os.environ, {"VERSION": "2026.08.02"}):
            second = build_ha_device(identifier="x", name="x", model="x")
        assert first["sw_version"] == "2026.08.01"
        assert second["sw_version"] == "2026.08.02"
