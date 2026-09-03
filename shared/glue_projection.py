"""
Canonical Glue partition-projection configuration for the archive Parquet
index table (`specs/aws/cloudformation.yaml`'s `GlueArchiveFlightsTable`).

CloudFormation performs ZERO validation of a Glue table's `Parameters` map
-- it is a free-form string-to-string map, so a typo (`projection.enbaled`,
`projection.enabled: "ture"`, a `digits` value that no longer matches the
zero-padding archive-processor/archive-compaction actually write) deploys
cleanly and yields a table that silently returns zero rows for every query,
forever. This module is the single source of truth both sides check
against:

- `shared/tests/test_cloudformation_template.py` asserts the template's
  `Parameters` map matches `EXPECTED_PROJECTION_PARAMETERS` exactly.
- Any predicate generator that builds Athena partition predicates (e.g.
  management-ui's archive search) must zero-pad month/day to `MONTH_DIGITS`
  / `DAY_DIGITS` characters or partition projection will not match a row.

Values here must stay in lockstep with the template by hand -- nothing
regenerates the template from this module.
"""

from __future__ import annotations

YEAR_RANGE = (2022, 2100)
MONTH_RANGE = (1, 12)
DAY_RANGE = (1, 31)

MONTH_DIGITS = 2
DAY_DIGITS = 2

PARTITION_KEYS = ("year", "month", "day")

# The exact `Parameters` map CloudFormation must set on GlueArchiveFlightsTable.
EXPECTED_PROJECTION_PARAMETERS: dict[str, str] = {
    "projection.enabled": "true",
    "projection.year.type": "integer",
    "projection.year.range": f"{YEAR_RANGE[0]},{YEAR_RANGE[1]}",
    "projection.month.type": "integer",
    "projection.month.range": f"{MONTH_RANGE[0]},{MONTH_RANGE[1]}",
    "projection.month.digits": str(MONTH_DIGITS),
    "projection.day.type": "integer",
    "projection.day.range": f"{DAY_RANGE[0]},{DAY_RANGE[1]}",
    "projection.day.digits": str(DAY_DIGITS),
}

# The Athena projection placeholders `storage.location.template` must end
# with. Built via Fn::Join in the template (never Fn::Sub, which would try
# to resolve ${year}/${month}/${day} as CloudFormation variables).
STORAGE_LOCATION_TEMPLATE_SUFFIX = "/index/year=${year}/month=${month}/day=${day}/"
