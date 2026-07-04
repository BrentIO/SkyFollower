from unittest.mock import MagicMock

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
