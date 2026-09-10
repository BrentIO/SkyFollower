import os
from unittest.mock import patch

from shared.ha_discovery import build_ha_device, build_ha_update_entity


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

    def test_sw_version_appends_commit_when_set(self):
        with patch.dict(
            os.environ, {"VERSION": "2026.08.03", "GIT_COMMIT": "abcdef01"}, clear=True
        ):
            device = build_ha_device(identifier="x", name="x", model="x")
            assert device["sw_version"] == "2026.08.03 (abcdef01)"

    def test_sw_version_omits_parens_when_commit_unknown(self):
        with patch.dict(
            os.environ, {"VERSION": "2026.08.03", "GIT_COMMIT": "unknown"}, clear=True
        ):
            device = build_ha_device(identifier="x", name="x", model="x")
            assert device["sw_version"] == "2026.08.03"


class TestBuildHaUpdateEntity:
    def _device(self):
        return build_ha_device(
            identifier="SkyFollower_receiver_pi_north",
            name="SkyFollower Receiver Pi North",
            model="Receiver",
        )

    def test_core_shape(self):
        device = self._device()
        entity = build_ha_update_entity(
            device=device,
            name="Update",
            state_topic="SkyFollower/core-health/statistic/receiver_pi_north_update",
        )
        assert entity["name"] == "Update"
        assert entity["state_topic"].endswith("receiver_pi_north_update")
        assert entity["entity_category"] == "diagnostic"

    def test_device_block_passed_through_unchanged(self):
        device = self._device()
        entity = build_ha_update_entity(
            device=device, name="Update", state_topic="t"
        )
        assert entity["device"] is device
        assert entity["device"]["ids"] == "SkyFollower_receiver_pi_north"

    def test_unique_id_and_object_id_derived_from_device_ids(self):
        entity = build_ha_update_entity(
            device=self._device(), name="Update", state_topic="t"
        )
        assert entity["unique_id"] == "SkyFollower_receiver_pi_north_update"
        assert entity["object_id"] == "SkyFollower_receiver_pi_north_update"

    def test_no_install_or_command_keys(self):
        entity = build_ha_update_entity(
            device=self._device(), name="Update", state_topic="t"
        )
        for forbidden in (
            "command_topic",
            "payload_install",
            "supported_features",
            "device_class",
        ):
            assert forbidden not in entity
        assert "INSTALL" not in str(entity)

    def test_no_value_templates_json_state_is_native(self):
        entity = build_ha_update_entity(
            device=self._device(), name="Update", state_topic="t"
        )
        assert "value_template" not in entity
        assert "latest_version_template" not in entity

    def test_availability_merged_when_given(self):
        availability = {
            "availability_topic": "SkyFollower/receiver/pi_north/status",
            "payload_available": "ONLINE",
            "payload_not_available": "OFFLINE",
        }
        entity = build_ha_update_entity(
            device=self._device(),
            name="Update",
            state_topic="t",
            availability=availability,
        )
        assert entity["availability_topic"] == "SkyFollower/receiver/pi_north/status"
        assert entity["payload_available"] == "ONLINE"
        assert entity["payload_not_available"] == "OFFLINE"

    def test_availability_absent_by_default(self):
        entity = build_ha_update_entity(
            device=self._device(), name="Update", state_topic="t"
        )
        assert "availability_topic" not in entity
