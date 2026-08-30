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
- **Teardown** is `--delete`. Both S3 buckets carry `DeletionPolicy:
  Retain`, so a delete never destroys flight data or the query-results
  bucket's contents.

**stdout carries only `KEY=value` lines** (the stack outputs). Every
progress line, change-set summary, stack event, and error goes to stderr.
That separation is what lets `scripts/install.sh` capture a clean payload
while the operator watches progress live. Any terminal failure exits
non-zero after printing the first `CREATE_FAILED` / `UPDATE_FAILED` event's
reason.

## Credentials

`aws-setup` needs credentials far broader than the three scoped identities
it creates — full Glue/IAM/Athena/S3 provisioning rights. Those are **the
operator's own temporary session credentials** (access key + secret +
session token, as copied from the AWS access portal), passed to the
container as environment for a single `--rm` run. This component never
writes them anywhere; they expire on their own.

The property this preserves: **no SkyFollower component ever holds a
credential that can create, modify, or delete an AWS resource.** The three
identities the stack issues are data-plane only — they cannot even read the
CloudFormation control plane, which is why the management-ui host runs
`--outputs-only` with its own temporary credentials rather than reading its
keys back through anything the stack gave it.

## Invocation

| Flags | Behaviour |
|---|---|
| *(none)* | Build a change set, print its summary, execute it (pausing for confirmation only on a `Replacement`), wait for a terminal state, print outputs |
| `--yes` | Apply a replacement-containing change set without prompting (non-interactive runs) |
| `--outputs-only` | Skip provisioning; just read an existing stack's outputs (the management-ui host) |
| `--delete` | `delete_stack` + wait. Both buckets are retained |

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

## Configuration

Every value is an environment variable. `ARCHIVE_BUCKET_NAME` is the only
one that is required; the rest are escape hatches for a bare `docker run`
and, when unset, leave the template's own defaults authoritative.

| Variable | Required | Default | Description |
|---|---|---|---|
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` | ✅ | — | The operator's temporary provisioning credentials. boto3's own variable names |
| `AWS_DEFAULT_REGION` | ✅ | — | Region to deploy the stack in / read it from |
| `ARCHIVE_BUCKET_NAME` | ✅ (not for `--outputs-only` / `--delete`) | — | Name of the S3 bucket holding `flights/`, `index/`, `_compaction_state/` |
| `CREATE_ARCHIVE_BUCKET` | ❌ | `Yes` | `Yes` to create it, `No` to adopt one that already exists |
| `STACK_NAME` | ❌ | `skyfollower` | CloudFormation stack name. An escape hatch (e.g. a test stack beside the real one); `install.sh` never sets it |
| `GLUE_DATABASE_NAME` | ❌ | `skyfollower` | |
| `GLUE_TABLE_NAME` | ❌ | `archive_flights` | |
| `ATHENA_WORKGROUP_NAME` | ❌ | `skyfollower` | |
| `ATHENA_RESULTS_EXPIRATION_DAYS` | ❌ | `8` | Whole-bucket expiry on the query-results bucket. One day longer than management-ui's Redis pointer TTL, by design |
| `RESOURCE_NAME_PREFIX` | ❌ | `skyfollower` | Prefix for the three IAM user names. Changing it on an existing stack replaces every user (rotating every key) |
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
  [docs/aws-setup.md](../docs/aws-setup.md). Never reorder or rename: Athena
  results are read positionally by management-ui.
- **Never change `ARCHIVE_BUCKET_NAME` on an existing stack** — in create
  mode CloudFormation replaces the bucket, and the archive silently starts
  writing to a new empty one. `UpdateReplacePolicy: Retain` saves the data
  but not the situation.

See [docs/aws-setup.md](../docs/aws-setup.md) for the full operator guide,
including recovering a wedged stack (`ROLLBACK_COMPLETE`, `DELETE_FAILED`).
