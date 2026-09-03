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
provisioning outside the installer.

The installer needs one elevated AWS credential for that provisioning
step. It offers two ways to supply one, once per run:

- **An existing temporary session** (access key + secret + session token,
  as copied from the AWS access portal / SSO).
- **A one-time IAM user** the installer prints a
  [least-privilege policy](#the-caller-and-bootstrap-user-policy) for and
  walks you through creating — then offers to delete again once
  provisioning succeeds.

Either way the credential is used for a single `--rm` container run and is
never written to any file.

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

Provisioning needs far broader rights — but *how much* broader is knowable
exactly, not "S3 admin, roughly", and it is split across two tiers so that
nothing admin-adjacent persists.

## The two-tier credential model

`aws-setup` deploys the CloudFormation stack with a **service role**
(`RoleARN` on `create_change_set`, bound once when the change set is
created and carried automatically through `execute_change_set`), mirroring
FireFly-Cloud's `firefly-cloudformation-execution` + `firefly-github-actions`
split.

| Tier | Identity | Trusted by | Holds |
|---|---|---|---|
| **Execution role** | `skyfollower-cloudformation-execution` (an IAM role, created by `aws-setup`, **not** in the template) | `cloudformation.amazonaws.com` only — unassumable by any user | Every resource permission the template needs: S3 bucket create/configure/delete, Glue database + table CRUD, Athena workgroup CRUD, and user + inline-policy + access-key management for the three runtime identities |
| **Caller credential** | your pasted temporary session, or the one-time `skyfollower-bootstrap` user | you | Only the CloudFormation control plane on `stack/skyfollower/*`, `iam:PassRole` + role management on the one execution role, and (bootstrap user only) its own self-delete |

`aws-setup` creates the execution role idempotently (`create_role` /
`put_role_policy` via boto3, using the caller credentials) before every
deploy: an unchanged re-run is a no-op, a changed permissions policy is
written in place. `aws-setup --delete` tears the role down again after the
stack (a leftover is reported, not fatal).

### Honest note on the trust model

With `iam:PassRole` plus the CloudFormation control plane, the caller
credential can still deploy an *arbitrary* template through the execution
role. This is inherent to the `PassRole` pattern, and FireFly-Cloud
accepts the same trade-off — the caller policy below is **not** a hard
privilege ceiling. What the split buys is that nothing admin-adjacent
**persists**: the execution role is unassumable except by CloudFormation,
and the one-time bootstrap user self-deletes once provisioning succeeds. A
pasted session expires on its own.

## The caller and bootstrap-user policy

This is the complete policy the pasted session or the one-time bootstrap
user needs. `scripts/install.sh` renders it for you with every value
filled in; `aws-setup --print-bootstrap-policy` is the same rendering
(no AWS call, no credentials required).

Replace `YOUR_REGION`, `YOUR_ACCOUNT_ID` (and, if you overrode them, the
`skyfollower` / `skyfollower-bootstrap` names) with your values.

```json
{
  "Version": "2012-10-17",
  "Statement": [
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
        "cloudformation:GetTemplateSummary"
      ],
      "Resource": [
        "arn:aws:cloudformation:YOUR_REGION:YOUR_ACCOUNT_ID:stack/skyfollower/*",
        "arn:aws:cloudformation:YOUR_REGION:YOUR_ACCOUNT_ID:changeSet/skyfollower-*/*"
      ]
    },
    {
      "Sid": "ManageAndPassExecutionRole",
      "Effect": "Allow",
      "Action": [
        "iam:CreateRole",
        "iam:GetRole",
        "iam:GetRolePolicy",
        "iam:PutRolePolicy",
        "iam:DeleteRolePolicy",
        "iam:DeleteRole",
        "iam:TagRole",
        "iam:UpdateAssumeRolePolicy",
        "iam:PassRole"
      ],
      "Resource": "arn:aws:iam::YOUR_ACCOUNT_ID:role/skyfollower-cloudformation-execution"
    },
    {
      "Sid": "BootstrapUserSelfCleanup",
      "Effect": "Allow",
      "Action": [
        "iam:ListAccessKeys",
        "iam:DeleteAccessKey",
        "iam:ListUserPolicies",
        "iam:DeleteUserPolicy",
        "iam:GetUser",
        "iam:DeleteUser"
      ],
      "Resource": "arn:aws:iam::YOUR_ACCOUNT_ID:user/skyfollower-bootstrap"
    }
  ]
}
```

| Statement | Why it's there |
|---|---|
| `CloudFormationControlPlane` | The change-set create/describe/execute/delete calls `aws-setup` makes, plus the stack/event reads it uses to preview and diagnose. Scoped to `stack/skyfollower/*` and its change sets |
| `ManageAndPassExecutionRole` | `aws-setup` creates the execution role with these calls, then `iam:PassRole` hands it to CloudFormation as the service role. Scoped to exactly the one role name — the caller can create, update, replace, and delete that role and nothing else |
| `BootstrapUserSelfCleanup` | Only relevant when you provision via a one-time IAM user: lets that user delete its own access key, inline policy, and itself. Scoped to exactly that one user, never a wildcard |

## The `skyfollower-cloudformation-execution` role

`aws-setup` creates this role with the trust policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "cloudformation.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

and attaches this inline permissions policy — the full resource list the
template performs on create, update, and delete, ARN-scoped wherever the
template's naming parameters (`ArchiveBucketName`, `ResourceNamePrefix`,
`GlueDatabaseName` / `GlueTableName`, `AthenaWorkGroupName`, and the stack
name) make it possible:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ArchiveBucketCreateAndConfigure",
      "Effect": "Allow",
      "Action": [
        "s3:CreateBucket",
        "s3:PutBucketPublicAccessBlock",
        "s3:PutEncryptionConfiguration",
        "s3:PutBucketTagging",
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
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::YOUR_ARCHIVE_BUCKET"
    },
    {
      "Sid": "AthenaResultsBucketCreateConfigureAndDelete",
      "Effect": "Allow",
      "Action": [
        "s3:CreateBucket",
        "s3:PutBucketPublicAccessBlock",
        "s3:PutEncryptionConfiguration",
        "s3:PutBucketTagging",
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
        "s3:DeleteBucket",
        "s3:PutLifecycleConfiguration"
      ],
      "Resource": "arn:aws:s3:::skyfollower-*"
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
        "glue:DeleteTable"
      ],
      "Resource": [
        "arn:aws:glue:YOUR_REGION:YOUR_ACCOUNT_ID:catalog",
        "arn:aws:glue:YOUR_REGION:YOUR_ACCOUNT_ID:database/skyfollower",
        "arn:aws:glue:YOUR_REGION:YOUR_ACCOUNT_ID:table/skyfollower/*"
      ]
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
        "athena:ListTagsForResource"
      ],
      "Resource": "arn:aws:athena:YOUR_REGION:YOUR_ACCOUNT_ID:workgroup/skyfollower"
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
        "iam:ListUserTags"
      ],
      "Resource": [
        "arn:aws:iam::YOUR_ACCOUNT_ID:user/skyfollower-archive-processor",
        "arn:aws:iam::YOUR_ACCOUNT_ID:user/skyfollower-archive-compaction",
        "arn:aws:iam::YOUR_ACCOUNT_ID:user/skyfollower-management-ui"
      ]
    }
  ]
}
```

| Statement | Template resources | Notes |
|---|---|---|
| `ArchiveBucketCreateAndConfigure` | `ArchiveBucket` | Create + public-access-block + SSE. **No `s3:DeleteBucket`** — the archive bucket is `DeletionPolicy: Retain`. The long `s3:Get*` list is the bucket sub-resources CloudFormation reads on every create / drift-check / delete |
| `AthenaResultsBucketCreateConfigureAndDelete` | `AthenaResultsBucket` | Same, **plus** `s3:DeleteBucket` and `s3:PutLifecycleConfiguration` — this bucket has no `Retain` override and carries the results-expiry lifecycle rule. CloudFormation names it `{stack}-athenaresultsbucket-{random}`, hence the `skyfollower-*` ARN |
| `GlueDatabaseAndTable` | `GlueDatabase`, `GlueArchiveFlightsTable` | Create / read / update (schema change on re-run) / delete |
| `AthenaWorkGroup` | `AthenaWorkGroup` | Create / read / update / delete plus the tag actions CloudFormation applies to created resources |
| `ProvisionedIamUsers` | the three `AWS::IAM::User` + `AWS::IAM::AccessKey` resources | User + inline-policy + access-key lifecycle. An `AccessKeySerial` bump is delete-old-key + create-new, so both are needed. `iam:TagUser` / `iam:UntagUser` / `iam:ListUserTags` cover CloudFormation's own resource tagging |

> **The execution-role policy has not yet been verified end-to-end against
> a live AWS account.** It is derived by reading the template and
> `aws-setup`'s code. CloudFormation's own resource-tagging behaviour, in
> particular, can require actions that are not obvious from the template
> alone. Before relying on it, deploy a fresh stack, update it, and delete
> it end-to-end with only the caller policy attached to a real bootstrap
> user, and iterate on any `AccessDenied` — folding each fix back into
> both `aws-setup`'s builder and the JSON above (they are generated from
> the same function, so they cannot drift).

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
prompt defaults — you press Enter through them. It asks for the elevated
provisioning credential **exactly once per run**: if you install both
`archive` and `management-ui` on the same host, the stack outputs captured
during `archive` (which include the `management-ui` access key) are reused,
and the roles are always installed `archive`-before-`management-ui`
regardless of the order you selected them.

After "Create or update it now?" it asks **how** you want to supply the
credential:

```
  This role needs AWS infrastructure (Glue table, Athena workgroup, IAM identities).
  Create or update it now? [Y/n] y

  Provisioning needs an elevated AWS credential (CloudFormation, S3, Glue, Athena, IAM).
  How do you want to supply it?
    1) Paste an existing temporary session (AWS access portal / SSO).
    2) Create a one-time IAM user now -- the installer prints a
       least-privilege policy and the console steps, and offers to
       delete the user again once provisioning succeeds.
  Choose [1/2]:
```

### The existing-session path (choice 1)

```
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

### The one-time IAM user path (choice 2)

For an operator who doesn't already hold a suitable SSO / access-portal
session. The installer asks region, resource-name prefix, and bucket name
**first** — so the policy it prints has no placeholders left in it — then
prints the fully-substituted
[caller policy](#the-caller-and-bootstrap-user-policy) plus a numbered
console walkthrough, and reads a **plain access key ID + secret** (no
session token). After a successful deploy:

```
  The one-time IAM user 'skyfollower-bootstrap' has served its purpose.
  Delete it now (key, inline policy, and user)? [Y/n] y
  ✓ One-time IAM user removed.
```

If the self-delete can't finish (an `AccessDenied`, a race), the installer
prints **exactly** what's left and the console steps to remove it by
hand — you're never left unsure whether a stray elevated identity still
exists. If provisioning itself fails, the user is left in place so you can
retry, with the `--delete-bootstrap-user` command to clean up later.

**Declining provisioning falls through to manual AWS prompts, unchanged** —
for anyone who already has infrastructure, or wants to create it another
way.

### The management-ui host needs an elevated credential too

Reading a stack's outputs needs `cloudformation:DescribeStacks`, which
**none of the three scoped identities has**. So a `management-ui`-only
host can't read its own credentials back out of the stack using anything
the stack issued it — it runs the same prompt flow, invoking the container
with `--outputs-only`. (When `archive` and `management-ui` are installed
together, the `archive` run already captured everything and this step is
skipped.)

Finding a stack requires knowing its region, so the installer prompts for
the region **before** the outputs lookup rather than taking it from the
stack's own `AwsRegion` output.

## Running the container directly

`install.sh` covers the normal path. The container is also a plain
`docker run`:

```sh
# Create or update. Omit AWS_SESSION_TOKEN if the credential is a plain
# IAM user's key rather than an SSO/access-portal session -- boto3 rejects
# an empty-string token rather than ignoring it. This run also ensures the
# skyfollower-cloudformation-execution role exists before deploying.
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

# Render the caller policy (no AWS call, no credentials needed)
docker run --rm \
  -e AWS_DEFAULT_REGION=us-east-1 \
  -e RESOURCE_NAME_PREFIX=skyfollower \
  ghcr.io/brentio/skyfollower-aws-setup:latest --print-bootstrap-policy

# Delete the one-time bootstrap user once done (its keys, inline policies,
# then the user). Uses the credentials you pass in.
docker run --rm \
  -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... \
  -e AWS_DEFAULT_REGION=us-east-1 \
  ghcr.io/brentio/skyfollower-aws-setup:latest --delete-bootstrap-user skyfollower-bootstrap
```

stdout is only `KEY=value` lines (or, for `--print-bootstrap-policy`, the
policy JSON); every progress line and error goes to stderr.

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

`--delete` runs `delete_stack` and waits, then tears down the
`skyfollower-cloudformation-execution` role and its inline policy
(best-effort — a leftover is reported, not fatal). `install.sh` never
offers this; it is a deliberate bare `docker run`:

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

**Adding an index column** is an append-only change across six places:

1. `specs/data-dictionary.yaml` — `archive_parquet_index.fields`
2. `archive-processor/main.py` — `_PARQUET_INDEX_SCHEMA`
3. `specs/aws/cloudformation.yaml` — the Glue table's `Columns`
4. `management-ui/backend/main.py` — `_SEARCH_SELECT_COLUMNS`
5. `management-ui/backend/main.py` — `_row_from_athena_result_row`
6. `management-ui/backend/main.py` — `_DOWNLOAD_SELECT_COLUMNS` (the
   download endpoint's own SELECT list, everything in #4 except `s3_key`)

**Rule: only ever append at the end; never reorder or rename.** Parquet
resolves columns by name, so old files backfill as `NULL`; but
`_row_from_athena_result_row` resolves each Athena result row *by
position* and will silently shift every value if the order changes. The
anti-drift test `shared/tests/test_cloudformation_template.py` enforces
that the template's columns match the data dictionary, in order.

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
