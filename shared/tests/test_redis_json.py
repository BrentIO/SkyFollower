import json
from unittest.mock import MagicMock

from redis.commands.json import JSON

from shared.redis_json import prune_none, set_json


class TestPruneNone:
    def test_drops_top_level_none(self):
        assert prune_none({"a": 1, "b": None}) == {"a": 1}

    def test_drops_nested_none(self):
        assert prune_none({"a": {"b": 1, "c": None}}) == {"a": {"b": 1}}

    def test_drops_dict_that_is_entirely_none_valued(self):
        assert prune_none({"a": {"b": None, "c": None}}) == {"a": {}}

    def test_top_level_none_field_removed_not_kept_as_null(self):
        assert prune_none({"a": 1, "aircraft": None}) == {"a": 1}
        assert "aircraft" not in prune_none({"a": 1, "aircraft": None})

    def test_preserves_falsy_but_non_none_values(self):
        record = {"count": 0, "enabled": False, "name": "", "items": []}
        assert prune_none(record) == record

    def test_walks_dicts_nested_in_lists(self):
        record = {"items": [{"a": 1, "b": None}, {"c": None}]}
        assert prune_none(record) == {"items": [{"a": 1}, {}]}

    def test_leaves_scalars_and_none_itself_unchanged(self):
        assert prune_none(5) == 5
        assert prune_none("x") == "x"
        assert prune_none(None) is None

    def test_does_not_mutate_input(self):
        record = {"a": 1, "b": None}
        prune_none(record)
        assert record == {"a": 1, "b": None}


class TestSetJson:
    def test_writes_pruned_object_to_json_set(self):
        client = MagicMock()
        set_json(client, "aircraft:registry:A8AE7F", {"a": 1, "b": None})
        client.json.return_value.set.assert_called_once_with(
            "aircraft:registry:A8AE7F", "$", {"a": 1}
        )

    def test_default_path_is_root(self):
        client = MagicMock()
        set_json(client, "key", {"a": 1})
        args = client.json.return_value.set.call_args
        assert args.args[1] == "$"

    def test_custom_path_is_passed_through(self):
        client = MagicMock()
        set_json(client, "key", {"a": 1}, path="$.aircraft")
        args = client.json.return_value.set.call_args
        assert args.args[1] == "$.aircraft"

    def test_works_with_a_pipeline_object(self):
        pipe = MagicMock()
        set_json(pipe, "key", {"a": 1, "b": None})
        pipe.json.return_value.set.assert_called_once_with("key", "$", {"a": 1})

    def test_uses_an_ascii_preserving_encoder(self):
        client = MagicMock()
        set_json(client, "key", {"a": 1})
        _, kwargs = client.json.call_args
        assert kwargs["encoder"].ensure_ascii is False


class TestSetJsonUtf8OnTheWire:
    """Exercises the real redis-py JSON client's encoder to prove non-ASCII
    characters reach the wire as-is, not as \\uXXXX escapes (issue #291)."""

    def _json_client(self):
        conn = MagicMock()
        return conn, JSON(client=conn, encoder=json.JSONEncoder(ensure_ascii=False))

    def test_accented_latin_characters_are_not_escaped(self):
        conn, json_ns = self._json_client()
        client = MagicMock()
        client.json.return_value = json_ns
        set_json(client, "aircraft:registry:C038AA", {"street": ["7373 de la Côte-Vertu Blvd."]})
        encoded = conn.execute_command.call_args.args[3]
        assert "Côte-Vertu" in encoded
        assert "\\u00" not in encoded

    def test_cyrillic_and_georgian_scripts_are_not_escaped(self):
        conn, json_ns = self._json_client()
        client = MagicMock()
        client.json.return_value = json_ns
        set_json(client, "operator:XYZ", {"name": "Авиакомпания", "owner": "ავიაკომპანია"})
        encoded = conn.execute_command.call_args.args[3]
        assert "Авиакомпания" in encoded
        assert "ავიაკომპანია" in encoded
        assert "\\u" not in encoded
