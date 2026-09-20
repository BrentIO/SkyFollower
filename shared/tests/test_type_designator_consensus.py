from __future__ import annotations

from unittest.mock import MagicMock

from shared.type_designator_consensus import (
    build_consensus_table,
    fetch_mictronics_type_designators,
    infer_type_designator,
    normalize_make_model_key,
    resolve_description_code,
)


class TestNormalizeMakeModelKey:
    def test_uppercases_and_strips_non_alphanumerics(self):
        assert normalize_make_model_key("Piper", "PA-28-181") == normalize_make_model_key("PIPER", "PA28181")

    def test_matches_regardless_of_punctuation_variance(self):
        assert normalize_make_model_key("CESSNA", "172-S") == normalize_make_model_key("cessna", "172s")

    def test_different_models_produce_different_keys(self):
        assert normalize_make_model_key("CESSNA", "172S") != normalize_make_model_key("CESSNA", "182T")

    def test_empty_manufacturer_returns_none(self):
        assert normalize_make_model_key("", "172S") is None

    def test_empty_model_returns_none(self):
        assert normalize_make_model_key("CESSNA", "") is None

    def test_none_values_return_none(self):
        assert normalize_make_model_key(None, None) is None


class TestBuildConsensusTable:
    def test_clear_majority_qualifies(self):
        examples = [("PIPER", "PA-28-181", "P28A")] * 10
        table = build_consensus_table(examples)
        key = normalize_make_model_key("PIPER", "PA-28-181")
        assert table[key] == "P28A"

    def test_below_threshold_agreement_abstains(self):
        # 5 examples, majority only holds 60% -- below the 90% bar.
        examples = (
            [("CIRRUS", "SR22", "SR22")] * 6
            + [("CIRRUS", "SR22", "S22T")] * 4
        )
        table = build_consensus_table(examples)
        key = normalize_make_model_key("CIRRUS", "SR22")
        assert key not in table

    def test_fewer_than_min_examples_abstains(self):
        # Only 2 labelled examples -- below the >= 3 minimum, even though
        # both agree (100% agreement).
        examples = [("BEECH", "35", "BE35"), ("BEECH", "35", "BE35")]
        table = build_consensus_table(examples)
        key = normalize_make_model_key("BEECH", "35")
        assert key not in table

    def test_no_labelled_examples_abstains(self):
        table = build_consensus_table([])
        assert table == {}

    def test_examples_with_no_designator_are_ignored(self):
        examples = [("PIPER", "PA-28-181", None), ("PIPER", "PA-28-181", "")]
        table = build_consensus_table(examples)
        key = normalize_make_model_key("PIPER", "PA-28-181")
        assert key not in table

    def test_custom_thresholds_are_respected(self):
        examples = (
            [("CIRRUS", "SR22", "SR22")] * 3
            + [("CIRRUS", "SR22", "S22T")] * 2
        )
        key = normalize_make_model_key("CIRRUS", "SR22")
        # 3/5 = 60% agreement, 5 examples: fails the default 90% bar...
        assert key not in build_consensus_table(examples)
        # ...but qualifies under a loosened 50% bar with the same min count.
        table = build_consensus_table(examples, min_agreement=0.5)
        assert table[key] == "SR22"


class TestInferTypeDesignator:
    def test_returns_designator_for_qualifying_key(self):
        table = {normalize_make_model_key("PIPER", "PA-28-181"): "P28A"}
        assert infer_type_designator(table, "PIPER", "PA-28-181") == "P28A"

    def test_returns_none_for_unknown_key(self):
        table = {normalize_make_model_key("PIPER", "PA-28-181"): "P28A"}
        assert infer_type_designator(table, "CESSNA", "172S") is None

    def test_returns_none_for_empty_manufacturer_or_model(self):
        table = {normalize_make_model_key("PIPER", "PA-28-181"): "P28A"}
        assert infer_type_designator(table, "", "PA-28-181") is None
        assert infer_type_designator(table, "PIPER", "") is None


class TestFetchMictronicsTypeDesignators:
    def test_extracts_designator_from_pipelined_docs(self):
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        pipe.execute.return_value = [
            {"aircraft": {"type_designator": "C172"}},
            None,
            {"aircraft": {}},
            {"icao_hex": "AAAAAA"},
        ]
        result = fetch_mictronics_type_designators(r, ["A1", "A2", "A3", "A4"])
        assert result == {"A1": "C172"}

    def test_batches_across_multiple_pipelines(self):
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        pipe.execute.side_effect = [
            [{"aircraft": {"type_designator": "C172"}}],
            [{"aircraft": {"type_designator": "P28A"}}],
        ]
        result = fetch_mictronics_type_designators(r, ["A1", "A2"], batch_size=1)
        assert result == {"A1": "C172", "A2": "P28A"}
        assert r.pipeline.call_count == 2

    def test_empty_input_returns_empty_dict(self):
        r = MagicMock()
        assert fetch_mictronics_type_designators(r, []) == {}
        r.pipeline.assert_not_called()


class TestResolveDescriptionCode:
    def test_returns_description_code_when_present(self):
        r = MagicMock()
        r.json.return_value.get.return_value = {"description_code": "L1P"}
        assert resolve_description_code(r, "P28A") == "L1P"

    def test_returns_none_when_type_not_found(self):
        r = MagicMock()
        r.json.return_value.get.return_value = None
        assert resolve_description_code(r, "ZZZZ") is None

    def test_returns_none_when_description_code_missing(self):
        r = MagicMock()
        r.json.return_value.get.return_value = {"manufacturer_model": "CESSNA 172"}
        assert resolve_description_code(r, "C172") is None

    def test_lookup_failure_returns_none(self):
        r = MagicMock()
        r.json.return_value.get.side_effect = Exception("boom")
        assert resolve_description_code(r, "C172") is None
