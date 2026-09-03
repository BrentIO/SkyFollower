"""
Anti-drift guard for specs/aws/cloudformation.yaml.

The template spans all three archive-facing components (archive-processor,
archive-compaction, management-ui), so no single component owns it and it
lives in shared/. It is the single source of truth for the live Glue
table, and CloudFormation performs ZERO validation of the Glue partition-
projection Parameters map -- a typo there deploys green and returns zero
rows forever. These assertions cover what AWS will not.

specs/data-dictionary.yaml's archive_parquet_index.fields is the enforced
source of truth for the 9 index columns, in order. shared/glue_projection.py
is the enforced source of truth for the partition-projection Parameters map
and the storage.location.template placeholders.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from shared.config import athena_config
from shared.glue_projection import (
    EXPECTED_PROJECTION_PARAMETERS,
    PARTITION_KEYS,
    STORAGE_LOCATION_TEMPLATE_SUFFIX,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_TEMPLATE_PATH = _REPO_ROOT / "specs" / "aws" / "cloudformation.yaml"
_DATA_DICTIONARY_PATH = _REPO_ROOT / "specs" / "data-dictionary.yaml"
_INSTALL_SH_PATH = _REPO_ROOT / "scripts" / "install.sh"

_RAW = _TEMPLATE_PATH.read_text()

# Glue column type <- data-dictionary field type. Explicit and reviewable
# on purpose: the dictionary calls the timestamp columns `datetime`, Glue
# calls them `timestamp`. A brand-new dictionary type (e.g. `float`) is
# absent here and fails test_every_dictionary_type_is_mapped with a clear
# message rather than a KeyError deep in another assertion.
_DICT_TYPE_TO_GLUE_TYPE = {
    "string": "string",
    "boolean": "boolean",
    "datetime": "timestamp",
}

# YAML short-form intrinsic tags (!Sub, !Ref, ...). yaml.safe_load() cannot
# parse these, and this file parses the template without a CloudFormation-
# aware loader, so they are banned outright.
_SHORT_FORM_RE = re.compile(
    r"""(?<![\w"'])!(Sub|Ref|GetAtt|Join|If|Equals|Not|And|Or|Select|Split|"""
    r"""Base64|Cidr|FindInMap|ImportValue|GetAZs|Transform|Condition)\b"""
)

try:
    TEMPLATE = yaml.safe_load(_RAW)
except yaml.YAMLError:
    TEMPLATE = None


def _require_template() -> dict:
    assert TEMPLATE is not None, (
        "cloudformation.yaml did not parse with yaml.safe_load -- almost "
        "certainly a short-form intrinsic; see test_no_short_form_intrinsics."
    )
    return TEMPLATE


def _glue_table_input() -> dict:
    resources = _require_template()["Resources"]
    return resources["GlueArchiveFlightsTable"]["Properties"]["TableInput"]


def _dictionary_index_fields() -> dict:
    data = yaml.safe_load(_DATA_DICTIONARY_PATH.read_text())
    return data["records"]["archive_parquet_index"]["fields"]


# ---------------------------------------------------------------------------
# 1. No short-form intrinsics (ordered first).
# ---------------------------------------------------------------------------

def _strip_comment(line: str) -> str:
    if line.lstrip().startswith("#"):
        return ""
    return line.split(" #", 1)[0]


def test_no_short_form_intrinsics():
    offenders = [
        f"  line {i}: {line.strip()}"
        for i, line in enumerate(_RAW.splitlines(), start=1)
        if _SHORT_FORM_RE.search(_strip_comment(line))
    ]
    assert not offenders, (
        "specs/aws/cloudformation.yaml must use full-form intrinsics only "
        "(Fn::Sub, Ref, Fn::GetAtt, Fn::If, Fn::Join). Found short-form:\n"
        + "\n".join(offenders)
    )


# ---------------------------------------------------------------------------
# 2. Glue columns match the data dictionary, as an ordered list.
#    Order is load-bearing: management-ui's _row_from_athena_result_row
#    destructures Athena's result rows positionally.
# ---------------------------------------------------------------------------

def test_glue_columns_match_data_dictionary_in_order():
    fields = _dictionary_index_fields()
    expected = [
        {"Name": name, "Type": _DICT_TYPE_TO_GLUE_TYPE[spec["type"]]}
        for name, spec in fields.items()
    ]
    actual = _glue_table_input()["StorageDescriptor"]["Columns"]
    assert actual == expected


# ---------------------------------------------------------------------------
# 3. Every data-dictionary type appears in the Glue type map.
# ---------------------------------------------------------------------------

def test_every_dictionary_type_is_mapped():
    used = {spec["type"] for spec in _dictionary_index_fields().values()}
    unmapped = used - set(_DICT_TYPE_TO_GLUE_TYPE)
    assert not unmapped, (
        f"archive_parquet_index uses type(s) {sorted(unmapped)} with no entry "
        "in _DICT_TYPE_TO_GLUE_TYPE -- add the Glue-type mapping here and a "
        "matching column to the template."
    )


# ---------------------------------------------------------------------------
# 4. Partition keys are exactly [year, month, day], all string.
# ---------------------------------------------------------------------------

def test_partition_keys():
    keys = _glue_table_input()["PartitionKeys"]
    assert keys == [{"Name": name, "Type": "string"} for name in PARTITION_KEYS]


# ---------------------------------------------------------------------------
# 5. Partition-projection properties present and correct -- the guard
#    against the typo class AWS silently accepts.
# ---------------------------------------------------------------------------

def test_projection_properties():
    params = _glue_table_input()["Parameters"]
    expected = EXPECTED_PROJECTION_PARAMETERS
    # Compare the full projection.* key SET, not just each expected key's
    # value in isolation -- a misspelled key (e.g. "projection.enbaled")
    # must surface as an unexpected extra rather than passing silently
    # because the correctly-spelled key was simply never looked up.
    actual = {k: v for k, v in params.items() if k.startswith("projection.")}
    missing = set(expected) - set(actual)
    unexpected = set(actual) - set(expected)
    assert not missing and not unexpected, (
        f"projection.* keys drifted from shared/glue_projection.py.\n"
        f"  missing: {sorted(missing)}\n"
        f"  unexpected: {sorted(unexpected)}"
    )
    assert actual == expected


# ---------------------------------------------------------------------------
# 6. storage.location.template is an Fn::Join (never Fn::Sub -- it carries
#    ${year}/${month}/${day} Athena placeholders) ending with the exact
#    Hive path build_index_s3_key and the data dictionary's s3_path use.
# ---------------------------------------------------------------------------

def test_storage_location_template():
    value = _glue_table_input()["Parameters"]["storage.location.template"]
    assert isinstance(value, dict) and "Fn::Join" in value, (
        "storage.location.template must be built with Fn::Join, not Fn::Sub "
        "or a bare string."
    )
    assert "Fn::Sub" not in str(value)
    delimiter, parts = value["Fn::Join"]
    assert delimiter == ""
    assert parts[-1] == STORAGE_LOCATION_TEMPLATE_SUFFIX
    # The dictionary's s3_path uses the same Hive layout.
    data = yaml.safe_load(_DATA_DICTIONARY_PATH.read_text())
    s3_path = data["records"]["archive_parquet_index"]["s3_path"]
    assert s3_path.startswith("index/year={YYYY}/month={MM}/day={DD}/")


# ---------------------------------------------------------------------------
# 7. Template parameter defaults match shared/config.py's athena_config().
# ---------------------------------------------------------------------------

def test_parameter_defaults_match_athena_config():
    params = _require_template()["Parameters"]
    cfg = athena_config()
    assert params["AthenaWorkGroupName"]["Default"] == cfg["workgroup"]
    assert params["GlueDatabaseName"]["Default"] == cfg["database"]
    assert params["GlueTableName"]["Default"] == cfg["table"]


# ---------------------------------------------------------------------------
# 8. install.sh's hardcoded Athena literals match those same defaults --
#    a third copy nothing else guards.
# ---------------------------------------------------------------------------

def test_install_sh_athena_literals_match_defaults():
    text = _INSTALL_SH_PATH.read_text()
    cfg = athena_config()

    def literal(key: str) -> str:
        match = re.search(rf"^{key}=(.+)$", text, re.MULTILINE)
        assert match, f"{key}= not found in scripts/install.sh"
        return match.group(1).strip()

    assert literal("ATHENA_WORKGROUP") == cfg["workgroup"]
    assert literal("ATHENA_DATABASE") == cfg["database"]
    assert literal("ATHENA_TABLE") == cfg["table"]
