# AWS Setup

| | |
|---|---|
| **Purpose** | One-shot container that provisions the archive's AWS infrastructure — both S3 buckets, the Glue database/table (with partition projection), the Athena workgroup, and three scoped IAM identities — by deploying a CloudFormation stack |
| **Run frequency** | Once per install, then again for each schema/policy upgrade. Never runs as part of the deployed stack |
| **Reads/writes** | AWS CloudFormation, and (via the stack) S3, Glue, Athena, IAM |

## How it works

The one authoritative description of every AWS resource the archive needs
lives in [`specs/aws/cloudformation.yaml`](../specs/aws/cloudformation.yaml),
baked into this image. `aws-setup` drives CloudFormation with it rather
than calling the Glue/IAM/Athena APIs directly:

- **The execution role.** Before every deploy, `aws-setup` idempotently
  creates the `<prefix>-cloudformation-execution` IAM role — trusted only
  by `cloudformation.amazonaws.com` — and attaches an inline permissions
  policy holding every resource action the template needs (`create_role` /
  `put_role_policy` via boto3, using the caller credentials; an unchanged
  re-run is a no-op, a changed policy is written in place). The deploy
  then passes `RoleARN=<that role>` to `create_change_set` and
  `execute_change_set`, so CloudFormation runs the underlying
  S3/Glue/Athena/IAM actions **as the role, not as the caller**.
- **Create or update** is a single `create_change_set` →
  `execute_change_set`. CloudFormation diffs desired state against deployed
  state and applies only the delta, rolling back on failure. Re-running an
  unchanged template is a clean no-op that still returns outputs.
- **Change preview**: every run builds a change set and prints its summary
  to stderr. It executes automatically **unless the change set contains a
  resource `Replacement`** (e.g. the archive bucket or an IAM user being
  recreated) — then it stops and requires `--yes`. This makes the two
  documented foot-guns (changing `ArchiveBucketName` or `ResourceNamePrefix`
  on an existing stack) impossible to trigger by accident.
- **Teardown** is `--delete`: `delete_stack` + wait, then a best-effort
  teardown of the execution role and its inline policy (a leftover is
  reported, not fatal). Both S3 buckets carry `DeletionPolicy: Retain`, so
  a delete never destroys flight data or the query-results bucket's
  contents.

**stdout carries only `KEY=value` lines** (the stack outputs). Every
progress line, change-set summary, stack event, and error goes to stderr.
That separation is what lets `scripts/install.sh` capture a clean payload
while the operator watches progress live. Any terminal failure exits
non-zero after printing the first `CREATE_FAILED` / `UPDATE_FAILED` event's
reason.

## Credentials

The credential `aws-setup` runs with is a **caller** credential — far
smaller than the union of what the template touches, because CloudFormation
executes the template as the execution role (above), not as the caller.
The exact least-privilege policy is documented in
[docs/aws-configuration.md](../docs/aws-configuration.md) and rendered,
fully substituted, by `--print-bootstrap-policy`. Two ways to supply it:

- **An existing temporary session** — access key + secret + session token,
  as copied from the AWS access portal / SSO.
- **A one-time IAM user**: run `--print-bootstrap-policy`, create a user
  with that inline policy in the console, use its plain access key (no
  session token), then run `--delete-bootstrap-user <name>` to remove it.
  `scripts/install.sh` walks through this end-to-end.

Either way the credential is passed to the container as environment for a
single `--rm` run. This component never writes it anywhere.

The property this preserves: **no SkyFollower component ever holds a
credential that can create, modify, or delete an AWS resource.** The three
identities the stack issues are data-plane only — they cannot even read the
CloudFormation control plane, which is why the management-ui host runs
`--outputs-only` with its own caller credential rather than reading its
keys back through anything the stack gave it.

## Invocation

| Flags | Behaviour |
|---|---|
| *(none)* | Ensure the execution role exists, then build a change set, print its summary, execute it (pausing for confirmation only on a `Replacement`), wait for a terminal state, print outputs. `RoleARN` is passed on both change-set calls |
| `--yes` | Apply a replacement-containing change set without prompting (non-interactive runs) |
| `--outputs-only` | Skip provisioning; just read an existing stack's outputs (the management-ui host) |
| `--delete` | `delete_stack` + wait, then tear down the execution role (best-effort). Both buckets are retained |
| `--print-bootstrap-policy` | Render the least-privilege caller IAM policy as JSON, fully substituted from the same parameters (`RESOURCE_NAME_PREFIX`, `STACK_NAME`, `AWS_DEFAULT_REGION`, and optional `AWS_ACCOUNT_ID` / `BOOTSTRAP_USER_NAME` refine the ARNs). Makes no AWS call and needs no credentials |
| `--delete-bootstrap-user NAME` | Delete `NAME`'s access keys, inline policies, then the user itself, using the credentials passed in. On any failed step, prints exactly what is left and the manual console steps, then exits non-zero |

`scripts/install.sh` runs this for you (the `archive` and `management-ui`
roles). To run it directly:

```sh
docker run --rm \
  -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... -e AWS_SESSION_TOKEN=... \
  -e AWS_DEFAULT_REGION=us-east-1 \
  -e ARCHIVE_BUCKET_NAME=skyfollower-archive-example \
  -e CREATE_ARCHIVE_BUCKET=Yes \
  ghcr.io/brentio/skyfollower-aws-setup:latest
```

Omit `AWS_SESSION_TOKEN` when the credential is a plain IAM user's key
rather than an SSO / access-portal session — boto3 rejects an empty-string
token rather than ignoring it.

## Configuration

Every value is an environment variable. `ARCHIVE_BUCKET_NAME` is the only
one that is required; the rest are escape hatches for a bare `docker run`
and, when unset, leave the template's own defaults authoritative.

| Variable | Required | Default | Description |
|---|---|---|---|
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | ✅ | — | The caller provisioning credentials. boto3's own variable names |
| `AWS_SESSION_TOKEN` | ✅ for an SSO / access-portal session; omit for a plain IAM user's key | — | boto3's own variable name |
| `AWS_DEFAULT_REGION` | ✅ | — | Region to deploy the stack in / read it from |
| `ARCHIVE_BUCKET_NAME` | ✅ (not for `--outputs-only` / `--delete` / `--print-bootstrap-policy`) | — | Name of the S3 bucket holding `flights/`, `index/`, `_compaction_state/` |
| `CREATE_ARCHIVE_BUCKET` | ❌ | `Yes` | `Yes` to create it, `No` to adopt one that already exists |
| `AWS_ACCOUNT_ID` | ❌ | `*` | `--print-bootstrap-policy` only: tightens the Glue/Athena/IAM ARNs to one account |
| `BOOTSTRAP_USER_NAME` | ❌ | `{RESOURCE_NAME_PREFIX}-bootstrap` | `--print-bootstrap-policy` only: the user the self-cleanup statement is scoped to |
| `STACK_NAME` | ❌ | `skyfollower` | CloudFormation stack name. An escape hatch (e.g. a test stack beside the real one); `install.sh` never sets it |
| `GLUE_DATABASE_NAME` | ❌ | `skyfollower` | |
| `GLUE_TABLE_NAME` | ❌ | `archive_flights` | |
| `ATHENA_WORKGROUP_NAME` | ❌ | `skyfollower` | |
| `ATHENA_RESULTS_EXPIRATION_DAYS` | ❌ | `8` | Whole-bucket expiry on the query-results bucket. One day longer than management-ui's Redis pointer TTL, by design |
| `RESOURCE_NAME_PREFIX` | ❌ | `skyfollower` | Prefix for the three IAM user names **and** the `<prefix>-cloudformation-execution` role. Changing it on an existing stack replaces every user (rotating every key) |
| `ACCESS_KEY_SERIAL` | ❌ | `1` | Bump to rotate all three access-key pairs |
| `ATHENA_BYTES_SCANNED_CUTOFF_BYTES` | ❌ | `0` | Per-query scanned-bytes ceiling (cost guardrail). `0` disables it; any non-zero value is floored at 10 MB by AWS |

## Outputs

Printed to stdout as `KEY=value`, one per line:

`ArchiveBucketName`, `AthenaResultsBucketName`, `AwsRegion`,
`GlueDatabaseName`, `GlueTableName`, `AthenaWorkGroupName`, and the three
access-key pairs (`ArchiveProcessorAccessKeyId` /
`ArchiveProcessorSecretAccessKey`, and the `ArchiveCompaction*` /
`ManagementUi*` equivalents).

## Upgrades and rotation

- **A normal update does not rotate credentials.** `AWS::IAM::AccessKey` is
  replaced only when its `Serial` or `UserName` changes, so schema, policy,
  and expiry changes leave every key untouched — no `.env` edit, no
  container restart. Inline policies update in place within seconds.
- **Key rotation** is deliberate: bump `ACCESS_KEY_SERIAL` (a real downtime
  window between `UPDATE_COMPLETE` and re-running `install.sh`, during which
  `archive-processor` falls back to `s3.db` with no data loss), or change
  `RESOURCE_NAME_PREFIX`, or delete the stack.
- **Adding an index column** is an append-only, 5-file change — see
  [docs/aws-configuration.md](../docs/aws-configuration.md). Never reorder or rename: Athena
  results are read positionally by management-ui.
- **Never change `ARCHIVE_BUCKET_NAME` on an existing stack** — in create
  mode CloudFormation replaces the bucket, and the archive silently starts
  writing to a new empty one. `UpdateReplacePolicy: Retain` saves the data
  but not the situation.

See [docs/aws-configuration.md](../docs/aws-configuration.md) for the full operator guide,
including recovering a wedged stack (`ROLLBACK_COMPLETE`, `DELETE_FAILED`).
