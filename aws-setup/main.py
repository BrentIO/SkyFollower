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
  --print-bootstrap-policy
                    render the least-privilege IAM policy the provisioning
                    credential needs, fully substituted from the same
                    parameters this tool already accepts (no AWS call, no
                    credentials required). For an operator who does not
                    already hold an SSO/access-portal session and wants to
                    create a one-time IAM user for provisioning.
  --delete-bootstrap-user NAME
                    delete that IAM user's access keys, inline policies,
                    and the user itself, using the current credentials --
                    the self-destruct step for the one-time user created
                    from --print-bootstrap-policy. On any failure prints
                    exactly what is left and the manual console steps.

Elevated (provisioning) credentials come from boto3's own environment
variables (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN /
AWS_DEFAULT_REGION). They are either the operator's own temporary session
credentials or a one-time IAM user's access key (see the two bootstrap
modes above), used once, in a container that is immediately destroyed --
this component never writes them anywhere.
"""

from __future__ import annotations

import argparse
import json
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


# ---------------------------------------------------------------------------
# Bootstrap IAM user: policy rendering + self-destruct
#
# CloudFormation here has no service role, so it runs every underlying
# resource action with the caller's own credentials. The policy below is
# therefore the union of the CloudFormation control-plane actions this
# module calls AND every resource action the template in
# specs/aws/cloudformation.yaml creates, updates, or deletes -- scoped by
# ARN wherever the template's naming parameters allow. Derived by reading
# the template and this file, NOT from a live deploy: CloudFormation's own
# resource-tagging can require IAM actions (iam:TagUser and friends) that
# are not obvious from the template. It must be confirmed end-to-end
# (create, update, delete) against a real account and iterated on any
# AccessDenied. See docs/aws-configuration.md.
# ---------------------------------------------------------------------------

def _bootstrap_context(env: dict) -> dict:
    prefix = env.get("RESOURCE_NAME_PREFIX", "").strip() or "skyfollower"
    archive_bucket = env.get("ARCHIVE_BUCKET_NAME", "").strip()
    if not archive_bucket:
        raise SystemExit(
            "Missing required environment variable(s): ARCHIVE_BUCKET_NAME"
        )
    # account / region are only needed to tighten the Glue and Athena ARNs.
    # When unknown (the policy is usually printed before any credential
    # exists) they fall back to "*" -- still scoped to the specific
    # workgroup / database / table name, just not to one account.
    return {
        "partition": env.get("AWS_PARTITION", "").strip() or "aws",
        "region": env.get("AWS_DEFAULT_REGION", "").strip() or "*",
        "account": env.get("AWS_ACCOUNT_ID", "").strip() or "*",
        "stack_name": env.get("STACK_NAME", "").strip() or _DEFAULT_STACK_NAME,
        "prefix": prefix,
        "archive_bucket": archive_bucket,
        "glue_database": env.get("GLUE_DATABASE_NAME", "").strip() or "skyfollower",
        "glue_table": env.get("GLUE_TABLE_NAME", "").strip() or "archive_flights",
        "athena_workgroup": env.get("ATHENA_WORKGROUP_NAME", "").strip() or "skyfollower",
        "bootstrap_user": env.get("BOOTSTRAP_USER_NAME", "").strip() or f"{prefix}-bootstrap",
    }


def build_bootstrap_policy(env: dict) -> dict:
    c = _bootstrap_context(env)
    p, region, account = c["partition"], c["region"], c["account"]
    stack = c["stack_name"]
    bucket = c["archive_bucket"]
    db, table, wg = c["glue_database"], c["glue_table"], c["athena_workgroup"]
    user_arns = [
        f"arn:{p}:iam::{account}:user/{c['prefix']}-archive-processor",
        f"arn:{p}:iam::{account}:user/{c['prefix']}-archive-compaction",
        f"arn:{p}:iam::{account}:user/{c['prefix']}-management-ui",
    ]
    bootstrap_user_arn = f"arn:{p}:iam::{account}:user/{c['bootstrap_user']}"

    # S3 config-read actions CloudFormation issues while creating, drift-
    # checking, or deleting an AWS::S3::Bucket. Broad on purpose: a missing
    # one surfaces only as a mid-deploy AccessDenied.
    s3_bucket_read = [
        "s3:GetBucketPublicAccessBlock",
        "s3:GetEncryptionConfiguration",
        "s3:GetBucketTagging",
        "s3:GetBucketPolicy",
        "s3:GetBucketAcl",
        "s3:GetBucketCORS",
        "s3:GetBucketWebsite",
        "s3:GetBucketVersioning",
        "s3:GetBucketLogging",
        "s3:GetLifecycleConfiguration",
        "s3:GetReplicationConfiguration",
        "s3:GetBucketObjectLockConfiguration",
        "s3:GetBucketNotification",
        "s3:GetAccelerateConfiguration",
        "s3:GetBucketRequestPayment",
        "s3:GetBucketLocation",
        "s3:ListBucket",
    ]
    s3_bucket_write = [
        "s3:CreateBucket",
        "s3:PutBucketPublicAccessBlock",
        "s3:PutEncryptionConfiguration",
        "s3:PutBucketTagging",
    ]

    statements = [
        {
            "Sid": "CloudFormationControlPlane",
            "Effect": "Allow",
            "Action": [
                "cloudformation:DescribeStacks",
                "cloudformation:DescribeStackEvents",
                "cloudformation:CreateChangeSet",
                "cloudformation:DescribeChangeSet",
                "cloudformation:ExecuteChangeSet",
                "cloudformation:DeleteChangeSet",
                "cloudformation:ListChangeSets",
                "cloudformation:DeleteStack",
                "cloudformation:GetTemplateSummary",
            ],
            "Resource": [
                f"arn:{p}:cloudformation:{region}:{account}:stack/{stack}/*",
                f"arn:{p}:cloudformation:{region}:{account}:changeSet/{stack}-*/*",
            ],
        },
        {
            "Sid": "ArchiveBucketCreateAndConfigure",
            "Effect": "Allow",
            "Action": s3_bucket_write + s3_bucket_read,
            # Archive bucket carries DeletionPolicy: Retain -- no
            # s3:DeleteBucket for it, deliberately.
            "Resource": f"arn:{p}:s3:::{bucket}",
        },
        {
            "Sid": "AthenaResultsBucketCreateConfigureAndDelete",
            "Effect": "Allow",
            "Action": s3_bucket_write + s3_bucket_read + [
                "s3:DeleteBucket",
                "s3:PutLifecycleConfiguration",
            ],
            # CloudFormation generates this bucket's name as
            # "{stack-name}-athenaresultsbucket-{random}", lowercased.
            "Resource": f"arn:{p}:s3:::{stack}-*",
        },
        {
            "Sid": "GlueDatabaseAndTable",
            "Effect": "Allow",
            "Action": [
                "glue:CreateDatabase",
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:UpdateDatabase",
                "glue:DeleteDatabase",
                "glue:CreateTable",
                "glue:GetTable",
                "glue:GetTables",
                "glue:UpdateTable",
                "glue:DeleteTable",
            ],
            "Resource": [
                f"arn:{p}:glue:{region}:{account}:catalog",
                f"arn:{p}:glue:{region}:{account}:database/{db}",
                f"arn:{p}:glue:{region}:{account}:table/{db}/*",
            ],
        },
        {
            "Sid": "AthenaWorkGroup",
            "Effect": "Allow",
            "Action": [
                "athena:CreateWorkGroup",
                "athena:GetWorkGroup",
                "athena:UpdateWorkGroup",
                "athena:DeleteWorkGroup",
                "athena:TagResource",
                "athena:UntagResource",
                "athena:ListTagsForResource",
            ],
            "Resource": f"arn:{p}:athena:{region}:{account}:workgroup/{wg}",
        },
        {
            "Sid": "ProvisionedIamUsers",
            "Effect": "Allow",
            "Action": [
                "iam:CreateUser",
                "iam:GetUser",
                "iam:DeleteUser",
                "iam:PutUserPolicy",
                "iam:GetUserPolicy",
                "iam:DeleteUserPolicy",
                "iam:ListUserPolicies",
                "iam:ListAttachedUserPolicies",
                "iam:CreateAccessKey",
                "iam:DeleteAccessKey",
                "iam:ListAccessKeys",
                "iam:GetAccessKeyLastUsed",
                "iam:TagUser",
                "iam:UntagUser",
                "iam:ListUserTags",
            ],
            "Resource": user_arns,
        },
        {
            # The policy that grants elevated access is also the policy that
            # lets it clean itself up -- nothing broader. Scoped to exactly
            # the one bootstrap user, never a wildcard.
            "Sid": "BootstrapUserSelfCleanup",
            "Effect": "Allow",
            "Action": [
                "iam:ListAccessKeys",
                "iam:DeleteAccessKey",
                "iam:ListUserPolicies",
                "iam:DeleteUserPolicy",
                "iam:GetUser",
                "iam:DeleteUser",
            ],
            "Resource": bootstrap_user_arn,
        },
    ]
    return {"Version": "2012-10-17", "Statement": statements}


def delete_bootstrap_user(iam, name: str) -> None:
    """Delete the one-time provisioning user: its access keys, its inline
    policies, then the user. Best-effort -- every step's failure is
    collected so the operator gets one exact list of what is left rather
    than an abort on the first error."""
    remaining: list[str] = []

    def _swallow_missing(exc: ClientError) -> bool:
        return exc.response.get("Error", {}).get("Code") == "NoSuchEntity"

    try:
        keys = iam.list_access_keys(UserName=name).get("AccessKeyMetadata", [])
    except ClientError as exc:
        if _swallow_missing(exc):
            log(f"User '{name}' does not exist -- nothing to delete.")
            return
        keys = []
        remaining.append(f"could not list access keys ({exc})")
    for key in keys:
        key_id = key.get("AccessKeyId", "?")
        try:
            iam.delete_access_key(UserName=name, AccessKeyId=key_id)
        except ClientError as exc:
            remaining.append(f"access key {key_id} ({exc})")

    try:
        policies = iam.list_user_policies(UserName=name).get("PolicyNames", [])
    except ClientError as exc:
        policies = []
        if not _swallow_missing(exc):
            remaining.append(f"could not list inline policies ({exc})")
    for policy_name in policies:
        try:
            iam.delete_user_policy(UserName=name, PolicyName=policy_name)
        except ClientError as exc:
            remaining.append(f"inline policy {policy_name} ({exc})")

    try:
        attached = iam.list_attached_user_policies(UserName=name).get("AttachedPolicies", [])
    except ClientError:
        attached = []
    for policy in attached:
        arn = policy.get("PolicyArn", "?")
        try:
            iam.detach_user_policy(UserName=name, PolicyArn=arn)
        except ClientError as exc:
            remaining.append(f"attached policy {arn} ({exc})")

    try:
        iam.delete_user(UserName=name)
    except ClientError as exc:
        if not _swallow_missing(exc):
            remaining.append(f"user {name} ({exc})")

    if remaining:
        log(f"✗ Could not fully remove the bootstrap identity '{name}'. Still present:")
        for item in remaining:
            log(f"    - {item}")
        log("")
        log("  Remove what is left by hand in the IAM console")
        log("  (https://console.aws.amazon.com/iam/home#/users):")
        log(f"    1. Open the user '{name}'.")
        log("    2. Security credentials tab -> delete every access key listed.")
        log("    3. Permissions tab -> delete every inline policy and detach every")
        log("       attached policy.")
        log("    4. Delete the user.")
        raise SystemExit(1)

    log(f"✓ Bootstrap user '{name}' -- its access keys and inline policy included -- is deleted.")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="aws-setup", description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--yes", action="store_true",
                       help="apply a replacement-containing change set without prompting")
    group.add_argument("--outputs-only", action="store_true",
                       help="skip provisioning; just read an existing stack's outputs")
    group.add_argument("--delete", action="store_true",
                       help="delete the stack and wait (both buckets are retained)")
    group.add_argument("--print-bootstrap-policy", action="store_true",
                       help="render the least-privilege provisioning IAM policy "
                            "(fully substituted from the same parameters; no AWS call)")
    group.add_argument("--delete-bootstrap-user", metavar="NAME", default=None,
                       help="delete NAME's access keys, inline policies, and the user "
                            "itself, using the current credentials (bootstrap self-destruct)")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    env = os.environ
    stack_name = env.get("STACK_NAME", "").strip() or _DEFAULT_STACK_NAME

    # Bootstrap modes first: --print-bootstrap-policy makes no AWS call at
    # all, and --delete-bootstrap-user talks to IAM, not CloudFormation.
    if args.print_bootstrap_policy:
        print(json.dumps(build_bootstrap_policy(env), indent=2), flush=True)
        return 0

    if args.delete_bootstrap_user:
        delete_bootstrap_user(boto3.client("iam"), args.delete_bootstrap_user)
        return 0

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
