# AWS Configuration

**Which roles need AWS:** only `archive` and `management-ui`. The `core`
role never touches AWS — it runs no S3, Athena, or Glue client and has no
AWS prompt in `scripts/install.sh`. If AWS prompts appear during a `core`
install, it's because that host also runs `management-ui` (the common
core-host topology), and the prompts belong to that role.

**There is no separate manual step.** `scripts/install.sh` runs the
`aws-setup` container for you as part of the `archive` and `management-ui`
prompts and writes the resulting bucket/region/credentials straight into
that host's `.env`. You only run `aws-setup` by hand if you're
provisioning outside the installer. Have your AWS access-portal session
credentials ready before starting an `archive` or `management-ui` install.

**None of the AWS resources on this page exist in a fresh SkyFollower
deployment.** The archive's Parquet index ([Archive
Processor](/components/archive-processor)'s [Parquet
Index](/components/archive-processor#parquet-index) section) is written to
S3 either way, but nothing can query it — via AWS Athena, against a Glue
Catalog table, using partition projection rather than a Glue Crawler —
until the infrastructure below is provisioned.

Provisioning is done by a **one-shot container**, `aws-setup`, that you run
once and throw away. It deploys a CloudFormation stack from
[`specs/aws/cloudformation.yaml`](https://github.com/BrentIO/SkyFollower/blob/main/specs/aws/cloudformation.yaml)
and prints the resulting bucket name, region, and credentials.
`scripts/install.sh` runs it for you and writes those values straight into
the host `.env`. Re-running it **is** the upgrade path: a schema or policy
change ships an updated template, and CloudFormation applies only the delta.

## The security property this preserves

**No SkyFollower component ever holds a credential that can create, modify,
or delete an AWS resource.** The three IAM identities the stack issues are
data-plane only — they can read and write specific S3 prefixes and run
Athena queries, and nothing else. They cannot even read the CloudFormation
control plane.

Provisioning needs far broader rights (Glue, IAM, Athena, S3 admin). Those
are **your own temporary session credentials** — access key, secret, and
session token, as copied from the AWS access portal — passed to the
`aws-setup` container as environment for a single `--rm` run. Nothing
writes them to `.env` or any file, and they expire on their own.

## What the stack creates

| Resource | Notes |
|---|---|
| **Archive S3 bucket** | Holds `flights/`, `index/`, `_compaction_state/`. Optional — set `CreateArchiveBucket=No` to adopt one that already exists. `DeletionPolicy: Retain` and `UpdateReplacePolicy: Retain`, so a stack delete or a replacing change can never destroy flight data. No versioning, no lifecycle rule — deliberate (see below) |
| **Athena query-results bucket** | Always created, dedicated, unnamed (CloudFormation generates the name). Whole-bucket expiry rule (`AthenaResultsExpirationDays`, default 8) plus an `AbortIncompleteMultipartUpload` rule. Safe to expire the whole bucket *because* it is dedicated |
| **Glue database + table** | The table over `s3://{archive-bucket}/index/`, Parquet, with year/month/day **partition projection** — no crawler. All 9 index columns, in the order the data dictionary defines them |
| **Athena workgroup** | `EnforceWorkGroupConfiguration: true`, supplying the results `OutputLocation`. management-ui never passes a `ResultConfiguration` of its own, so it is structurally impossible for a query to redirect results into the flight-data bucket |
| **3 IAM users**, each with an inline policy and one access key | `archive-processor` (Get/Put on `flights/*` and `index/*`), `archive-compaction` (Get/Put/**Delete** on `index/*`, plus `_compaction_state/*` and bucket-level `List`), `management-ui` (read-only Athena/Glue **scoped to this workgroup/database/table**, `GetObject` on `index/*` and `flights/*`, and full access to the results bucket) |

Inline policies rather than managed policies: 1:1 lifecycle with the user,
and no "AWS keeps only 5 managed-policy versions" ceiling to prune on
repeated upgrades.

### Why `CreateArchiveBucket` defaults to `Yes`

The failure modes are asymmetric:

- **Wrong-way `Yes`** (bucket already exists): `BucketAlreadyOwnedByYou`,
  `CREATE_FAILED` within seconds, clean rollback, existing data untouched,
  obvious fix.
- **Wrong-way `No`** (bucket absent): the stack goes **green**. Glue
  accepts a `Location` pointing at a nonexistent bucket; IAM accepts ARNs
  resolving to nothing. The failure surfaces hours later, on a different
  machine, as `archive-processor` silently spooling to its `s3.db`
  fallback.

### Why no versioning or lifecycle rule on the archive bucket

`archive-compaction` deletes every per-flight `index/*.parquet` after
merging it into the day's compacted file. With versioning on, each
compaction run would leave those deletes as permanently-billed noncurrent
versions. There is deliberately **no lifecycle rule on the archive bucket,
ever** — flight data is kept indefinitely.

## Running it through `scripts/install.sh`

For the `archive` and `management-ui` roles, the installer offers to
provision before it asks for AWS values, so the stack outputs become the
prompt defaults — you press Enter through them.

```
$ ./scripts/install.sh --role archive
-- skyfollower-archive (archive) --

  This role needs AWS infrastructure (Glue table, Athena workgroup, IAM identities).
  Create or update it now? [Y/n] y

  Paste temporary AWS credentials with permission to create these resources
  (access key + secret + session token, as copied from the AWS access portal).
  These are used for this one step only and are never saved.
  AWS access key ID: ...
  AWS secret access key: (hidden)
  AWS session token: (hidden)
  AWS region [us-east-1]: us-east-1
  S3 archive bucket name: skyfollower-archive-example
  Create this bucket? [Y/n] y

  → Deploying CloudFormation stack 'skyfollower' (this can take a few minutes)...
  ✓ Stack deployed.
  ✓ AWS values captured -- the prompts below are pre-filled; press Enter to accept.
```

**Declining provisioning falls through to manual AWS prompts, unchanged** —
for anyone who already has infrastructure, or wants to create it another
way.

### The management-ui host prompts for temporary credentials too

Reading a stack's outputs needs `cloudformation:DescribeStacks`, which
**none of the three scoped identities has**. So the management-ui host
can't read its own credentials back out of the stack using anything the
stack issued it. It runs the same prompt flow, invoking the container with
`--outputs-only`: paste temporary session credentials, read outputs,
auto-fill. The elevated credential is needed twice across the two hosts,
but it is short-lived and never stored either time.

Finding a stack requires knowing its region, so the installer prompts for
the region **before** the outputs lookup rather than taking it from the
stack's own `AwsRegion` output.

## Running the container directly

`install.sh` covers the normal path. The container is also a plain
`docker run`:

```sh
# Create or update
docker run --rm \
  -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... -e AWS_SESSION_TOKEN=... \
  -e AWS_DEFAULT_REGION=us-east-1 \
  -e ARCHIVE_BUCKET_NAME=skyfollower-archive-example \
  -e CREATE_ARCHIVE_BUCKET=Yes \
  ghcr.io/brentio/skyfollower-aws-setup:latest

# Read an existing stack's outputs
docker run --rm \
  -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... -e AWS_SESSION_TOKEN=... \
  -e AWS_DEFAULT_REGION=us-east-1 \
  ghcr.io/brentio/skyfollower-aws-setup:latest --outputs-only
```

stdout is only `KEY=value` lines; every progress line and error goes to
stderr.

See the [aws-setup component page](/components/aws-setup) for the full
environment-variable table.

### Applying a change that replaces a resource

Every run builds a CloudFormation **change set** and prints its summary. It
executes automatically **unless the change set contains a resource
`Replacement`** — then it stops. Re-run with `--yes` to apply it
deliberately:

```sh
docker run --rm -e AWS_... ghcr.io/brentio/skyfollower-aws-setup:latest --yes
```

The two changes that manifest as replacements — altering
`ResourceNamePrefix`, or `ArchiveBucketName` on an existing stack — are
therefore impossible to trigger by accident.

### Tearing down

`--delete` runs `delete_stack` and waits. `install.sh` never offers this;
it is a deliberate bare `docker run`:

```sh
docker run --rm \
  -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... -e AWS_SESSION_TOKEN=... \
  -e AWS_DEFAULT_REGION=us-east-1 \
  ghcr.io/brentio/skyfollower-aws-setup:latest --delete
```

Both S3 buckets are retained, so no flight data and no query-result files
are lost.

## Upgrades and rotation

**A normal stack update does not rotate credentials.** `AWS::IAM::AccessKey`
is replaced only when its `Serial` or `UserName` changes, so schema
changes, policy changes, and expiry changes leave every key untouched — no
`.env` edit, no container restart. Inline policies update in place within
seconds.

**Three things rotate keys**: bumping `AccessKeySerial` (the intended
lever), changing `ResourceNamePrefix`, and deleting the stack. Bumping
`AccessKeySerial` opens a real downtime window between `UPDATE_COMPLETE`
and re-running `install.sh` to pick up the new keys; during it
`archive-processor` falls back to `s3.db` with no data loss.

**Adding an index column** is an append-only change across five places:

1. `specs/data-dictionary.yaml` — `archive_parquet_index.fields`
2. `archive-processor/main.py` — `_PARQUET_INDEX_SCHEMA`
3. `specs/aws/cloudformation.yaml` — the Glue table's `Columns`
4. `management-ui/backend/main.py` — `_SEARCH_SELECT_COLUMNS`
5. `management-ui/backend/main.py` — `_row_from_csv_fields`

**Rule: only ever append at the end; never reorder or rename.** Parquet
resolves columns by name, so old files backfill as `NULL`; but
`_row_from_csv_fields` resolves Athena's result CSV *by position* and will
silently shift every value if the order changes. The anti-drift test
`shared/tests/test_cloudformation_template.py` enforces that the template's
columns match the data dictionary, in order.

**Never change `ArchiveBucketName` on an existing stack.** In create mode
CloudFormation *replaces* the bucket; `UpdateReplacePolicy: Retain` keeps
the old data, but the archive silently starts writing to a new empty
bucket. A genuinely independent second deployment in the same account needs
a different `STACK_NAME` **and** a different `ResourceNamePrefix` **and** a
different `ArchiveBucketName`.

## Recovering a wedged stack

CloudFormation can leave a stack in a state that blocks further updates:

- **`ROLLBACK_COMPLETE`** — a *first* `CREATE` failed and rolled back. The
  stack can only be deleted, not updated. Run `aws-setup --delete` (or
  delete it in the console), fix the cause, and run provisioning again.
  Common cause: `CreateArchiveBucket=Yes` against a bucket name that
  already exists.
- **`UPDATE_ROLLBACK_COMPLETE`** — an update failed and rolled back
  cleanly. Just fix the cause and re-run; no teardown needed.
- **`UPDATE_ROLLBACK_FAILED` / `DELETE_FAILED`** — rare, usually an IAM or
  S3 resource that couldn't be rolled back or removed. Resolve it from the
  CloudFormation console (continue rollback, or skip the stuck resource),
  then re-run.

Because both buckets are `Retain`, none of these recovery paths risk
flight data.

## An obsolete local file to remove by hand

Earlier versions of SkyFollower had each archive-facing component write
resolved IAM/Glue reference files into a `data/<component>/aws-setup/`
directory for an operator to copy into the console by hand. That workflow
is gone. A `management-ui` host deployed before this change may still have
a stale `data/management-ui/aws-setup/` directory — it is obsolete and safe
to delete. `install.sh` deliberately does not remove it for you (an
installer shouldn't delete operator-visible files from a bind mount
unasked).
