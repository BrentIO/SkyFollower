"""
aws-setup: one-shot container that provisions the SkyFollower archive's AWS
infrastructure by deploying the CloudFormation stack in
specs/aws/cloudformation.yaml, then prints the resulting credentials and
resource names for scripts/install.sh to write into a host's .env.

Run with `docker run --rm`, never as a Compose service -- it belongs to no
running stack, and both the archive and management-ui hosts need it.

Contract:
  * stdout carries ONLY `KEY=value` lines (the stack outputs). Every
    progress line, change-set summary, stack event, and error goes to
    stderr. That separation is what lets install.sh capture a clean
    payload while the operator watches progress live.
  * Exit non-zero on any terminal failure, after printing the first
    CREATE_FAILED / UPDATE_FAILED event's reason -- that string is almost
    always the actual diagnosis.

Modes:
  (default)          build a change set, print its summary, execute it
                     (pausing for confirmation only if it contains a
                     resource Replacement), wait for a terminal state,
                     print outputs.
  --yes             apply a replacement-containing change set without
                     prompting (non-interactive runs).
  --outputs-only    skip provisioning; just read an existing stack's
                     outputs (used on the second host).
  --delete          delete_stack + wait. Documented as a bare docker run;
                     never offered by install.sh.

Elevated (provisioning) credentials come from boto3's own environment
variables (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN /
AWS_DEFAULT_REGION). They are the operator's own temporary session
credentials, used once, in a container that is immediately destroyed --
this component never writes them anywhere.
"""

from __future__ import annotations

import argparse
import os
import sys
import time

import boto3
from botocore.exceptions import ClientError, WaiterError

# Baked into the image at /app/specs/aws/cloudformation.yaml by the
# Dockerfile's `COPY specs/aws/`. The fallback path is for running this
# module straight from a source checkout (tests, local debugging).
_TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "..", "specs", "aws", "cloudformation.yaml")
if not os.path.exists(_TEMPLATE_PATH):
    _TEMPLATE_PATH = "/app/specs/aws/cloudformation.yaml"

_DEFAULT_STACK_NAME = "skyfollower"

# Env var -> CloudFormation parameter key. ARCHIVE_BUCKET_NAME and
# CREATE_ARCHIVE_BUCKET are the two install.sh actually sets; the rest are
# escape hatches for a bare `docker run` and are only passed through when
# present, so the template's own defaults stay authoritative otherwise.
_REQUIRED_PARAM_ENV = {
    "ARCHIVE_BUCKET_NAME": "ArchiveBucketName",
}
_OPTIONAL_PARAM_ENV = {
    "CREATE_ARCHIVE_BUCKET": "CreateArchiveBucket",
    "GLUE_DATABASE_NAME": "GlueDatabaseName",
    "GLUE_TABLE_NAME": "GlueTableName",
    "ATHENA_WORKGROUP_NAME": "AthenaWorkGroupName",
    "ATHENA_RESULTS_EXPIRATION_DAYS": "AthenaResultsExpirationDays",
    "RESOURCE_NAME_PREFIX": "ResourceNamePrefix",
    "ACCESS_KEY_SERIAL": "AccessKeySerial",
    "ATHENA_BYTES_SCANNED_CUTOFF_BYTES": "AthenaBytesScannedCutoffBytes",
}

_CHANGE_SET_POLL_SECONDS = 3
_CHANGE_SET_DEADLINE_SECONDS = 300


def log(message: str = "") -> None:
    """Everything the operator reads goes to stderr, keeping stdout a clean
    KEY=value payload for install.sh."""
    print(message, file=sys.stderr, flush=True)


def emit_output(key: str, value: str) -> None:
    print(f"{key}={value}", flush=True)


def build_parameters(env: dict) -> list[dict]:
    params: list[dict] = []
    missing: list[str] = []
    for env_key, param_key in _REQUIRED_PARAM_ENV.items():
        value = env.get(env_key, "").strip()
        if not value:
            missing.append(env_key)
            continue
        params.append({"ParameterKey": param_key, "ParameterValue": value})
    if missing:
        raise SystemExit(
            "Missing required environment variable(s): " + ", ".join(missing)
        )
    for env_key, param_key in _OPTIONAL_PARAM_ENV.items():
        value = env.get(env_key, "").strip()
        if value:
            params.append({"ParameterKey": param_key, "ParameterValue": value})
    return params


def stack_exists(cf, stack_name: str) -> bool:
    try:
        resp = cf.describe_stacks(StackName=stack_name)
    except ClientError as exc:
        if "does not exist" in str(exc):
            return False
        raise
    for stack in resp.get("Stacks", []):
        status = stack.get("StackStatus", "")
        # A stack wedged at REVIEW_IN_PROGRESS never had a change set
        # executed against it -- treat it as not-yet-created so a fresh
        # CREATE change set is what gets built.
        if status == "REVIEW_IN_PROGRESS":
            return False
        return True
    return False


def read_outputs(cf, stack_name: str) -> dict:
    resp = cf.describe_stacks(StackName=stack_name)
    stacks = resp.get("Stacks", [])
    if not stacks:
        raise SystemExit(f"Stack '{stack_name}' not found.")
    return {o["OutputKey"]: o["OutputValue"] for o in stacks[0].get("Outputs", [])}


def emit_outputs(outputs: dict) -> None:
    for key in sorted(outputs):
        emit_output(key, outputs[key])


def _wait_for_change_set(cf, stack_name: str, change_set_name: str) -> dict:
    """Poll until the change set reaches a terminal state. Returns the
    describe_change_set response on success; raises SystemExit on a real
    failure. The "no changes" outcome is returned like success so the
    caller can treat it as a clean no-op."""
    deadline = time.monotonic() + _CHANGE_SET_DEADLINE_SECONDS
    while True:
        resp = cf.describe_change_set(ChangeSetName=change_set_name, StackName=stack_name)
        status = resp.get("Status", "")
        if status in ("CREATE_COMPLETE", "FAILED"):
            return resp
        if time.monotonic() > deadline:
            raise SystemExit(
                f"Change set '{change_set_name}' did not finish creating within "
                f"{_CHANGE_SET_DEADLINE_SECONDS}s (last status: {status})."
            )
        time.sleep(_CHANGE_SET_POLL_SECONDS)


def _change_set_is_empty(resp: dict) -> bool:
    if resp.get("Status") != "FAILED":
        return False
    reason = (resp.get("StatusReason") or "").lower()
    return (
        "didn't contain changes" in reason
        or "no updates are to be performed" in reason
        or "no changes" in reason
    )


def summarize_changes(changes: list[dict]) -> str:
    lines = []
    for change in changes:
        rc = change.get("ResourceChange", {})
        action = rc.get("Action", "?")
        replacement = rc.get("Replacement")
        suffix = f" (Replacement: {replacement})" if replacement and replacement != "False" else ""
        lines.append(
            f"  {action:<8} {rc.get('LogicalResourceId', '?'):<28} "
            f"{rc.get('ResourceType', '?')}{suffix}"
        )
    return "\n".join(lines) if lines else "  (no resource changes)"


def has_replacement(changes: list[dict]) -> bool:
    # "True" is a definite replacement; "Conditional" means AWS can't tell
    # ahead of time and it might replace -- both stop for confirmation.
    for change in changes:
        replacement = change.get("ResourceChange", {}).get("Replacement")
        if replacement in ("True", "Conditional"):
            return True
    return False


def _first_failure_reason(cf, stack_name: str) -> str:
    try:
        events = cf.describe_stack_events(StackName=stack_name).get("StackEvents", [])
    except ClientError:
        return ""
    # describe_stack_events returns newest-first; the earliest failure is
    # the last matching one and is almost always the root cause.
    reason = ""
    for event in events:
        if event.get("ResourceStatus", "") in (
            "CREATE_FAILED",
            "UPDATE_FAILED",
            "DELETE_FAILED",
        ):
            detail = event.get("ResourceStatusReason", "")
            if detail and "cancelled" not in detail.lower():
                reason = f"{event.get('LogicalResourceId', '?')}: {detail}"
    return reason


def provision(cf, stack_name: str, template_body: str, parameters: list[dict],
              assume_yes: bool, interactive: bool) -> dict:
    exists = stack_exists(cf, stack_name)
    change_set_type = "UPDATE" if exists else "CREATE"
    change_set_name = f"skyfollower-{int(time.time())}"

    log(f"→ Building CloudFormation change set for stack '{stack_name}' ({change_set_type})...")
    cf.create_change_set(
        StackName=stack_name,
        ChangeSetName=change_set_name,
        ChangeSetType=change_set_type,
        TemplateBody=template_body,
        Parameters=parameters,
        Capabilities=["CAPABILITY_NAMED_IAM"],
        Description="SkyFollower archive infrastructure (aws-setup container)",
    )

    result = _wait_for_change_set(cf, stack_name, change_set_name)

    if _change_set_is_empty(result):
        log("✓ No changes -- stack already matches the template.")
        _safe_delete_change_set(cf, stack_name, change_set_name)
        if not exists:
            # A CREATE change set that came back empty means nothing was
            # ever created; there is no stack to read outputs from.
            raise SystemExit(
                f"Stack '{stack_name}' does not exist and the template produced "
                "no resources to create."
            )
        return read_outputs(cf, stack_name)

    if result.get("Status") == "FAILED":
        raise SystemExit(
            f"Change set creation failed: {result.get('StatusReason', 'unknown reason')}"
        )

    changes = result.get("Changes", [])
    log("Change set summary:")
    log(summarize_changes(changes))

    if has_replacement(changes):
        if not assume_yes:
            if interactive:
                log("")
                answer = input(
                    "  This change set REPLACES one or more resources "
                    "(recreating them, which rotates any affected access keys).\n"
                    "  Apply it? [y/N]: "
                )
                if not answer.strip().lower().startswith("y"):
                    _safe_delete_change_set(cf, stack_name, change_set_name)
                    raise SystemExit("Aborted -- change set not executed.")
            else:
                _safe_delete_change_set(cf, stack_name, change_set_name)
                raise SystemExit(
                    "This change set replaces one or more resources. Re-run with "
                    "--yes to apply it deliberately."
                )

    log(f"→ Executing change set on '{stack_name}'...")
    cf.execute_change_set(ChangeSetName=change_set_name, StackName=stack_name)

    waiter_name = "stack_create_complete" if not exists else "stack_update_complete"
    log("→ Waiting for the stack to reach a terminal state (this can take a few minutes)...")
    try:
        cf.get_waiter(waiter_name).wait(StackName=stack_name)
    except WaiterError as exc:
        reason = _first_failure_reason(cf, stack_name)
        log(f"✗ Stack deployment failed: {reason or exc}")
        raise SystemExit(1)

    log("✓ Stack deployed.")
    return read_outputs(cf, stack_name)


def _safe_delete_change_set(cf, stack_name: str, change_set_name: str) -> None:
    try:
        cf.delete_change_set(ChangeSetName=change_set_name, StackName=stack_name)
    except ClientError:
        pass


def delete_stack(cf, stack_name: str) -> None:
    if not stack_exists(cf, stack_name):
        log(f"Stack '{stack_name}' does not exist -- nothing to delete.")
        return
    log(f"→ Deleting stack '{stack_name}'...")
    cf.delete_stack(StackName=stack_name)
    try:
        cf.get_waiter("stack_delete_complete").wait(StackName=stack_name)
    except WaiterError as exc:
        reason = _first_failure_reason(cf, stack_name)
        log(f"✗ Stack deletion failed: {reason or exc}")
        raise SystemExit(1)
    log("✓ Stack deleted. Both S3 buckets were retained; no flight data was lost.")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="aws-setup", description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--yes", action="store_true",
                       help="apply a replacement-containing change set without prompting")
    group.add_argument("--outputs-only", action="store_true",
                       help="skip provisioning; just read an existing stack's outputs")
    group.add_argument("--delete", action="store_true",
                       help="delete the stack and wait (both buckets are retained)")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    env = os.environ
    stack_name = env.get("STACK_NAME", "").strip() or _DEFAULT_STACK_NAME

    cf = boto3.client("cloudformation")

    if args.delete:
        delete_stack(cf, stack_name)
        return 0

    if args.outputs_only:
        log(f"→ Reading outputs from stack '{stack_name}'...")
        emit_outputs(read_outputs(cf, stack_name))
        return 0

    with open(_TEMPLATE_PATH, "r") as handle:
        template_body = handle.read()

    parameters = build_parameters(env)
    interactive = sys.stdin.isatty()
    outputs = provision(cf, stack_name, template_body, parameters,
                        assume_yes=args.yes, interactive=interactive)
    emit_outputs(outputs)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit as exc:
        # argparse and our own raise SystemExit("message") both land here;
        # a string code prints to stderr and exits 1, an int passes through.
        if isinstance(exc.code, str):
            log(exc.code)
            sys.exit(1)
        raise
