"""
Rules engine for SkyFollower message processor.

Loads rules and areas from Redis (config:rules / config:areas keys), evaluates
all enabled rules against a flight's current state, and returns the list of
rules that matched.  A rule fires at most once per flight per identifier.

Configuration is hot-reloaded whenever the version hash keys change.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import date, datetime, time, timezone
from typing import Any, Optional

from shapely.geometry import Point, Polygon

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Condition priorities — lower number evaluated first (cheap before expensive)
# ---------------------------------------------------------------------------
_PRIORITY = {
    "military": 50,
    "aircraft_powerplant_count": 50,
    "altitude": 100,
    "velocity": 110,
    "heading": 150,
    "vertical_speed": 155,
    "date": 200,
    "aircraft_icao_hex": 300,
    "ident": 305,
    "aircraft_registration": 310,
    "squawk": 310,
    "operator_airline_designator": 360,
    "aircraft_type_designator": 355,
    "wake_turbulence_category": 400,
    "matched_rules": 360,
    "area": 1000,  # shapely geometry — most expensive
}

_VALID_CONDITION_TYPES = frozenset(_PRIORITY.keys())
_VALID_OPERATORS = frozenset({"equals", "minimum", "maximum", "in_list", "not_in_list"})

_WAKE_TURBULENCE_CATEGORIES = frozenset({
    "light", "medium", "medium 1", "medium 2",
    "high vortex aircraft", "heavy", "super", "rotorcraft", "high performance",
})


# ---------------------------------------------------------------------------
# Internal exceptions (used only within loadRules / loadAreas for clean flow)
# ---------------------------------------------------------------------------
class _RuleError(Exception):
    pass


class _ConditionError(Exception):
    pass


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

class RulesEngine:
    """
    Evaluates SkyFollower notification rules against live flight state.

    Usage::

        engine = RulesEngine(redis_client)
        # In a background thread every 5 seconds:
        engine.reload_if_changed()
        # On every decoded message:
        matched = engine.evaluate(flight)
    """

    def __init__(self, redis_client) -> None:
        self._redis = redis_client
        self._rules: list[dict] = []
        self._areas: list[dict] = []
        self._removed_rules: list[dict] = []
        self._rules_version: Optional[str] = None
        self._areas_version: Optional[str] = None

    # ------------------------------------------------------------------
    # Config reload
    # ------------------------------------------------------------------

    def reload_if_changed(self) -> bool:
        """
        Compare cached version hashes against Redis.  Reload whichever
        config has changed.  Returns True if anything was reloaded.
        """
        reloaded = False
        try:
            rv = self._redis.get("config:rules:version")
            if rv != self._rules_version:
                raw = self._redis.get("config:rules")
                if raw and self._load_rules(raw):
                    self._rules_version = rv
                    reloaded = True

            av = self._redis.get("config:areas:version")
            if av != self._areas_version:
                raw = self._redis.get("config:areas")
                if raw and self._load_areas(raw):
                    self._areas_version = av
                    reloaded = True
        except Exception as exc:
            logger.error("Error polling config from Redis: %s", exc)
        return reloaded

    def load_rules_json(self, json_str: str) -> bool:
        """Load rules directly from a JSON string (used by UI backend for validation)."""
        ok = self._load_rules(json_str)
        if ok:
            self._rules_version = hashlib.sha256(json_str.encode()).hexdigest()
        return ok

    def load_areas_json(self, json_str: str) -> bool:
        """Load areas directly from a JSON string."""
        ok = self._load_areas(json_str)
        if ok:
            self._areas_version = hashlib.sha256(json_str.encode()).hexdigest()
        return ok

    # ------------------------------------------------------------------
    # Evaluation
    # ------------------------------------------------------------------

    def evaluate(self, flight) -> list[dict]:
        """
        Evaluate all enabled rules against *flight*.

        Returns a list of matched rule dicts (keys: name, description,
        identifier).  Skips rules whose identifier is already in
        flight.matched_rules (each rule fires at most once per flight).
        """
        matched: list[dict] = []
        for rule in self._rules:
            identifier = rule["identifier"]
            if identifier in flight.matched_rules:
                continue
            if self._evaluate_rule(rule, flight):
                matched.append(rule)
        return matched

    # ------------------------------------------------------------------
    # Rule loading
    # ------------------------------------------------------------------

    def _load_rules(self, json_str: str) -> bool:
        """Parse, validate and stage rules.  Only replaces the active set on success."""
        try:
            rules = json.loads(json_str)
        except json.JSONDecodeError:
            logger.critical("Rules file contains invalid JSON — keeping previous ruleset.")
            return False

        if not isinstance(rules, list):
            logger.critical("Rules must be a JSON array — keeping previous ruleset.")
            return False

        staged: list[dict] = []
        seen_identifiers: set[str] = set()

        for idx, rule in enumerate(rules):
            try:
                staged_rule = self._parse_rule(rule, idx, seen_identifiers)
                if staged_rule is None:
                    continue  # disabled rule — skip
                seen_identifiers.add(staged_rule["identifier"])
                staged.append(staged_rule)
            except _RuleError as exc:
                logger.critical("Rule #%d invalid: %s — keeping previous ruleset.", idx, exc)
                return False

        removed = [r for r in self._rules if r not in staged]
        self._removed_rules = removed
        self._rules = staged
        logger.info("Rules loaded: %d active.", len(self._rules))
        return True

    def _parse_rule(self, rule: dict, idx: int, seen: set[str]) -> Optional[dict]:
        if "enabled" not in rule:
            raise _RuleError("missing 'enabled' field")
        if not isinstance(rule["enabled"], bool):
            raise _RuleError("'enabled' must be a boolean")
        if not rule["enabled"]:
            return None

        if "identifier" not in rule:
            raise _RuleError("missing 'identifier' field")
        identifier = str(rule["identifier"])
        if identifier in seen:
            raise _RuleError(f"duplicate identifier '{identifier}'")

        if "conditions" not in rule or not isinstance(rule["conditions"], list):
            raise _RuleError(f"rule '{identifier}' missing 'conditions' array")
        if len(rule["conditions"]) == 0:
            raise _RuleError(f"rule '{identifier}' has no conditions")

        staged_conditions: list[dict] = []
        for cidx, cond in enumerate(rule["conditions"]):
            try:
                staged_conditions.append(self._parse_condition(cond, identifier, cidx))
            except _ConditionError as exc:
                raise _RuleError(f"rule '{identifier}' condition #{cidx}: {exc}") from exc

        staged_conditions.sort(key=lambda c: c["_priority"])

        return {
            "name": str(rule.get("name", "")),
            "description": str(rule.get("description", "")),
            "identifier": identifier,
            "conditions": staged_conditions,
        }

    def _parse_condition(self, cond: dict, rule_id: str, cidx: int) -> dict:
        for field in ("type", "value", "operator"):
            if field not in cond:
                raise _ConditionError(f"missing '{field}'")

        ctype = str(cond["type"]).strip().lower()
        operator = str(cond["operator"]).strip().lower()
        value = cond["value"]

        if ctype not in _VALID_CONDITION_TYPES:
            raise _ConditionError(f"unknown type '{ctype}'")
        if operator not in _VALID_OPERATORS:
            raise _ConditionError(f"unknown operator '{operator}'")

        # Delegate to per-type validators which return the normalised condition
        staged = {"type": ctype, "operator": operator, "value": value,
                  "_priority": _PRIORITY[ctype]}
        validator = getattr(self, f"_validate_{ctype}", None)
        if validator:
            staged = validator(staged)
        return staged

    # ------------------------------------------------------------------
    # Area loading
    # ------------------------------------------------------------------

    def _load_areas(self, json_str: str) -> bool:
        try:
            geo = json.loads(json_str)
        except json.JSONDecodeError:
            logger.critical("Areas file contains invalid JSON — keeping previous areas.")
            return False

        if geo.get("type", "").lower() != "featurecollection":
            logger.critical("Areas must be a GeoJSON FeatureCollection — keeping previous areas.")
            return False

        staged: list[dict] = []
        for feature in geo.get("features", []):
            if feature.get("type") != "Feature":
                continue
            name = str(feature.get("properties", {}).get("name", "")).strip()
            if not name:
                continue
            geometry = feature.get("geometry", {})
            if geometry.get("type") != "Polygon":
                logger.debug("Area '%s' is not a Polygon — skipping.", name)
                continue
            coords = geometry.get("coordinates", [])
            if len(coords) != 1:
                logger.warning("Area '%s' has unexpected coordinate structure — skipping.", name)
                continue
            try:
                poly = Polygon([tuple(c) for c in coords[0]])
                if not poly.is_valid:
                    logger.warning("Area '%s' is not a valid polygon — skipping.", name)
                    continue
                staged.append({
                    "name": name,
                    "geometry": poly,
                    "boundary": poly.bounds,  # (minx, miny, maxx, maxy)
                })
            except Exception as exc:
                logger.warning("Area '%s' could not be parsed: %s — skipping.", name, exc)
                continue

        self._areas = staged
        logger.info("Areas loaded: %d polygons.", len(self._areas))
        return True

    # ------------------------------------------------------------------
    # Condition validators (normalise value + enforce operator constraints)
    # ------------------------------------------------------------------

    def _validate_altitude(self, c: dict) -> dict:
        if c["operator"] == "equals":
            raise _ConditionError("altitude does not support 'equals'")
        c["value"] = self._require_non_negative_int(c["value"], "altitude")
        return c

    def _validate_velocity(self, c: dict) -> dict:
        if c["operator"] == "equals":
            raise _ConditionError("velocity does not support 'equals'")
        c["value"] = self._require_non_negative_int(c["value"], "velocity")
        return c

    def _validate_vertical_speed(self, c: dict) -> dict:
        if c["operator"] == "equals":
            raise _ConditionError("vertical_speed does not support 'equals'")
        try:
            c["value"] = int(float(c["value"]))
        except (TypeError, ValueError):
            raise _ConditionError("vertical_speed value must be a number")
        return c

    def _validate_heading(self, c: dict) -> dict:
        if c["operator"] != "equals":
            raise _ConditionError("heading only supports 'equals'")
        try:
            parts = tuple(float(x) for x in str(c["value"]).split(","))
        except (TypeError, ValueError):
            raise _ConditionError("heading value must be two comma-separated numbers")
        if len(parts) != 2:
            raise _ConditionError("heading value must be exactly two values (min,max)")
        for v in parts:
            if not (0 <= v <= 359):
                raise _ConditionError(f"heading value {v} must be between 0 and 359")
        c["value"] = parts
        return c

    def _validate_date(self, c: dict) -> dict:
        raw = str(c["value"]).strip()
        if "T" in raw:
            # YYYY-MM-DDTHH:MMZ
            try:
                parsed = datetime.fromisoformat(raw)
                if parsed.tzinfo is None:
                    raise _ConditionError(
                        "datetime value must include a timezone designator (e.g. 'Z')"
                    )
                c["_date_format"] = "datetime"
                c["value"] = parsed
            except ValueError:
                raise _ConditionError(
                    f"invalid datetime value '{raw}' — expected YYYY-MM-DDTHH:MMZ"
                )
        else:
            # YYYY-MM-DD
            try:
                c["value"] = date.fromisoformat(raw)
                c["_date_format"] = "date"
            except ValueError:
                raise _ConditionError(
                    f"invalid date value '{raw}' — expected YYYY-MM-DD or YYYY-MM-DDTHH:MMZ"
                )
        return c

    def _validate_ident(self, c: dict) -> dict:
        if c["operator"] != "equals":
            raise _ConditionError("ident only supports 'equals'")
        c["value"] = str(c["value"]).strip().upper()
        return c

    def _validate_squawk(self, c: dict) -> dict:
        if c["operator"] != "equals":
            raise _ConditionError("squawk only supports 'equals'")
        val = str(c["value"]).strip()
        if not val.isnumeric() or len(val) != 4:
            raise _ConditionError("squawk must be a 4-digit numeric string")
        c["value"] = val
        return c

    def _validate_military(self, c: dict) -> dict:
        if c["operator"] != "equals":
            raise _ConditionError("military only supports 'equals'")
        c["value"] = str(c["value"]).strip().lower() == "true"
        return c

    def _validate_operator_airline_designator(self, c: dict) -> dict:
        if c["operator"] != "equals":
            raise _ConditionError("operator_airline_designator only supports 'equals'")
        val = str(c["value"]).strip().upper()
        if len(val) != 3:
            raise _ConditionError("operator_airline_designator must be exactly 3 characters")
        c["value"] = val
        return c

    def _validate_aircraft_type_designator(self, c: dict) -> dict:
        if c["operator"] != "equals":
            raise _ConditionError("aircraft_type_designator only supports 'equals'")
        val = str(c["value"]).strip().upper()
        if len(val) != 4:
            raise _ConditionError("aircraft_type_designator must be exactly 4 characters")
        c["value"] = val
        return c

    def _validate_aircraft_registration(self, c: dict) -> dict:
        if c["operator"] != "equals":
            raise _ConditionError("aircraft_registration only supports 'equals'")
        val = str(c["value"]).strip().upper()
        if len(val) < 2:
            raise _ConditionError("aircraft_registration must be at least 2 characters")
        c["value"] = val
        return c

    def _validate_aircraft_icao_hex(self, c: dict) -> dict:
        if c["operator"] != "equals":
            raise _ConditionError("aircraft_icao_hex only supports 'equals'")
        val = str(c["value"]).strip().upper()
        if len(val) != 6:
            raise _ConditionError("aircraft_icao_hex must be exactly 6 characters")
        c["value"] = val
        return c

    def _validate_aircraft_powerplant_count(self, c: dict) -> dict:
        c["value"] = self._require_non_negative_int(c["value"], "aircraft_powerplant_count")
        return c

    def _validate_wake_turbulence_category(self, c: dict) -> dict:
        if c["operator"] != "equals":
            raise _ConditionError("wake_turbulence_category only supports 'equals'")
        val = str(c["value"]).strip().lower()
        if val not in _WAKE_TURBULENCE_CATEGORIES:
            raise _ConditionError(
                f"unknown wake_turbulence_category '{val}'; "
                f"valid values: {sorted(_WAKE_TURBULENCE_CATEGORIES)}"
            )
        c["value"] = val
        return c

    def _validate_matched_rules(self, c: dict) -> dict:
        if c["operator"] not in ("in_list", "not_in_list"):
            raise _ConditionError("matched_rules only supports 'in_list' or 'not_in_list'")
        if not isinstance(c["value"], list):
            raise _ConditionError("matched_rules value must be a list")
        if len(c["value"]) < 1:
            raise _ConditionError("matched_rules list must have at least one element")
        return c

    def _validate_area(self, c: dict) -> dict:
        if c["operator"] != "equals":
            raise _ConditionError("area only supports 'equals'")
        name = str(c["value"]).strip().upper()
        if not any(a["name"].upper() == name for a in self._areas):
            raise _ConditionError(f"area '{c['value']}' not found in areas config")
        c["value"] = name
        return c

    # ------------------------------------------------------------------
    # Condition data evaluators
    # ------------------------------------------------------------------

    def _evaluate_rule(self, rule: dict, flight) -> bool:
        for cond in rule["conditions"]:
            if not self._evaluate_condition(cond, flight):
                return False
        return True

    def _evaluate_condition(self, cond: dict, flight) -> bool:
        ctype = cond["type"]
        evaluator = getattr(self, f"_eval_{ctype}", None)
        if evaluator is None:
            logger.warning("No evaluator for condition type '%s'", ctype)
            return False
        return evaluator(cond, flight)

    def _eval_altitude(self, c: dict, flight) -> bool:
        if not flight.positions:
            return False
        alt = flight.positions[-1].altitude
        if alt is None:
            return False
        return self._compare_numeric(alt, c["operator"], c["value"])

    def _eval_velocity(self, c: dict, flight) -> bool:
        if not flight.velocities:
            return False
        v = flight.velocities[-1].velocity
        if v is None:
            return False
        return self._compare_numeric(v, c["operator"], c["value"])

    def _eval_vertical_speed(self, c: dict, flight) -> bool:
        if not flight.velocities:
            return False
        vs = flight.velocities[-1].vertical_speed
        if vs is None:
            return False
        threshold = c["value"]
        if threshold < 0:
            # Descending condition
            if c["operator"] == "minimum":
                return vs < 0 and vs <= threshold
            if c["operator"] == "maximum":
                return vs < 0 and vs >= threshold
        else:
            # Climbing or level condition
            if c["operator"] == "minimum":
                return vs >= 0 and vs >= threshold
            if c["operator"] == "maximum":
                return vs >= 0 and vs <= threshold
        return False

    def _eval_heading(self, c: dict, flight) -> bool:
        if not flight.velocities:
            return False
        h = flight.velocities[-1].heading
        if h is None:
            return False
        lo, hi = c["value"]
        if lo > hi:
            # Wraps around 0° (e.g. northbound: 340–020)
            return h >= lo or h <= hi
        return lo <= h <= hi

    def _eval_date(self, c: dict, flight) -> bool:
        now_utc = datetime.now(timezone.utc)
        fmt = c.get("_date_format", "date")
        if fmt == "date":
            current = now_utc.date()
        else:
            current = now_utc.replace(second=0, microsecond=0)
        return self._compare_ordered(current, c["operator"], c["value"])

    def _eval_area(self, c: dict, flight) -> bool:
        if not flight.positions:
            return False
        pos = flight.positions[-1]
        if pos.latitude is None or pos.longitude is None:
            return False
        name = c["value"]
        for area in self._areas:
            if area["name"].upper() != name:
                continue
            minx, miny, maxx, maxy = area["boundary"]
            if not (minx <= pos.longitude <= maxx and miny <= pos.latitude <= maxy):
                return False  # outside bounding box
            return Point(pos.longitude, pos.latitude).within(area["geometry"])
        return False

    def _eval_ident(self, c: dict, flight) -> bool:
        return str(flight.ident).strip().upper() == c["value"]

    def _eval_squawk(self, c: dict, flight) -> bool:
        return str(flight.squawk).strip() == c["value"]

    def _eval_military(self, c: dict, flight) -> bool:
        return flight.aircraft.get("military") == c["value"]

    def _eval_operator_airline_designator(self, c: dict, flight) -> bool:
        return flight.operator.get("airline_designator", "").upper() == c["value"]

    def _eval_aircraft_type_designator(self, c: dict, flight) -> bool:
        return flight.aircraft.get("type_designator", "").upper() == c["value"]

    def _eval_aircraft_registration(self, c: dict, flight) -> bool:
        return flight.aircraft.get("registration", "").upper() == c["value"]

    def _eval_aircraft_icao_hex(self, c: dict, flight) -> bool:
        return flight.icao_hex.upper() == c["value"]

    def _eval_aircraft_powerplant_count(self, c: dict, flight) -> bool:
        count = flight.aircraft.get("powerplant", {})
        if isinstance(count, dict):
            count = count.get("count")
        if count is None:
            return False
        return self._compare_numeric(count, c["operator"], c["value"])

    def _eval_wake_turbulence_category(self, c: dict, flight) -> bool:
        wtc = flight.aircraft.get("wake_turbulence_category", "")
        return str(wtc).strip().lower() == c["value"]

    def _eval_matched_rules(self, c: dict, flight) -> bool:
        if c["operator"] == "in_list":
            return any(r in flight.matched_rules for r in c["value"])
        if c["operator"] == "not_in_list":
            return not any(r in flight.matched_rules for r in c["value"])
        return False

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _compare_numeric(current: float, operator: str, threshold: float) -> bool:
        if operator == "equals":
            return current == threshold
        if operator == "minimum":
            return current >= threshold
        if operator == "maximum":
            return current <= threshold
        return False

    @staticmethod
    def _compare_ordered(current: Any, operator: str, threshold: Any) -> bool:
        if operator == "equals":
            return current == threshold
        if operator == "minimum":
            return current >= threshold
        if operator == "maximum":
            return current <= threshold
        return False

    @staticmethod
    def _require_non_negative_int(value: Any, field: str) -> int:
        try:
            v = int(float(value))
        except (TypeError, ValueError):
            raise _ConditionError(f"{field} value must be a number")
        if v < 0:
            raise _ConditionError(f"{field} value must be non-negative")
        return v
