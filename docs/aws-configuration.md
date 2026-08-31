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

The installer needs an elevated AWS credential for that one provisioning
step. It offers two ways to supply one: paste an existing temporary
session (AWS access portal / SSO), or have the installer print a
[least-privilege policy](#the-provisioning-credential-exact-permissions)
and walk you through creating a **one-time IAM user** — which it then
offers to delete again once provisioning succeeds. Either way the
credential is used for a single `--rm` container run and is never written
to any file.

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

Provisioning needs far broader rights than that — but *how much* broader
is knowable exactly, not "S3 admin, roughly". The next section spells out
the complete policy.

## The provisioning credential: exact permissions

`aws-setup` has no CloudFormation service role, so CloudFormation executes
every underlying resource action **using the caller's own credentials**.
The provisioning credential therefore needs the five CloudFormation
control-plane actions `aws-setup` calls (`DescribeStacks`,
`CreateChangeSet` with `CAPABILITY_NAMED_IAM`, `DescribeChangeSet`,
`ExecuteChangeSet`, `DeleteStack`) **plus** a direct permission for every
resource action the template in
[`specs/aws/cloudformation.yaml`](https://github.com/BrentIO/SkyFollower/blob/main/specs/aws/cloudformation.yaml)
performs on create, update, and delete.

The policy below is that list, scoped by ARN wherever the template's own
naming parameters (`ArchiveBucketName`, `ResourceNamePrefix`,
`GlueDatabaseName` / `GlueTableName`, `AthenaWorkGroupName`, and the stack
name) make that possible. It answers "is this effectively
`AdministratorAccess`?" — no; here is exactly what it can touch and why.

Replace `YOUR_REGION`, `YOUR_ACCOUNT_ID`, and `YOUR_ARCHIVE_BUCKET` with
your values; keep the resource names (`skyfollower`, `archive_flights`,
`skyfollower-bootstrap`) unless you are also overriding the matching
template parameters. `scripts/install.sh` renders this for you with every
value already filled in — `aws-setup --print-bootstrap-policy` (see below)
is the same rendering.

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

Why each statement is there:

| Statement | Template resources | Notes |
|---|---|---|
| `CloudFormationControlPlane` | the stack itself | The five calls `aws-setup` makes, plus the change-set/event reads it uses to preview and diagnose |
| `ArchiveBucketCreateAndConfigure` | `ArchiveBucket` | Create + public-access-block + SSE. **No `s3:DeleteBucket`** — the archive bucket is `DeletionPolicy: Retain`. The long `s3:Get*` list is the bucket sub-resources CloudFormation reads on every create/drift/delete |
| `AthenaResultsBucketCreateConfigureAndDelete` | `AthenaResultsBucket` | Same, **plus** `s3:DeleteBucket` and `s3:PutLifecycleConfiguration` — this bucket has no `Retain` override and carries the results-expiry lifecycle rule. CloudFormation names it `{stack}-athenaresultsbucket-{random}`, hence the `skyfollower-*` ARN |
| `GlueDatabaseAndTable` | `GlueDatabase`, `GlueArchiveFlightsTable` | Create/read/update (schema change on re-run) / delete |
| `AthenaWorkGroup` | `AthenaWorkGroup` | Create/read/update/delete + the tag actions CloudFormation applies to created resources |
| `ProvisionedIamUsers` | the three `AWS::IAM::User` + `AWS::IAM::AccessKey` resources | User + inline-policy + access-key lifecycle. A `Serial` bump is delete-old-key + create-new, so both are needed. `iam:TagUser` / `iam:UntagUser` / `iam:ListUserTags` cover CloudFormation's resource-tagging |
| `BootstrapUserSelfCleanup` | — | Only present when you provision via a one-time IAM user: lets that user delete its own access key, inline policy, and itself. Scoped to exactly that one user, never a wildcard |

> **This policy has not yet been verified end-to-end against a live AWS
> account.** It is derived by reading the template and `aws-setup`'s code.
> CloudFormation's own resource-tagging behaviour, in particular, can
> require IAM actions (`iam:TagUser` and similar) that are not obvious
> from the template alone. Before relying on it, deploy a fresh stack,
> update it, and delete it with **only** this policy attached, and iterate
> on any `AccessDenied` until all three lifecycle operations succeed
> cleanly.

The elevated credential — whether an SSO session or a one-time IAM user's
access key — is passed to the `aws-setup` container as environment for a
single `--rm` run. Nothing writes it to `.env` or any file. A session
expires on its own; a one-time user is deleted by
[`aws-setup --delete-bootstrap-user`](#the-one-time-bootstrap-user-modes).

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
prompt defaults — you press Enter through them. After "Create or update it
now?" it asks **how** you want to supply the elevated credential:

```
  This role needs AWS infrastructure (Glue table, Athena workgroup, IAM identities).
  Create or update it now? [Y/n] y

  Provisioning needs elevated AWS rights (CloudFormation, S3, Glue, Athena, IAM).
  How do you want to supply them?
    1) Paste an existing temporary session (AWS access portal / SSO).
    2) Create a one-time IAM user now -- the installer prints a
       least-privilege policy and the console steps, and offers to
       delete the user again once provisioning succeeds.
  Choose [1/2]:
```

### The existing-session path (choice 1)

Unchanged from before:

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

For an operator who doesn't already hold a suitable SSO/access-portal
session. The installer asks region, resource-name prefix, and bucket name
**first** — so the policy it prints has no placeholders — then:

```
  AWS region [us-east-1]: us-east-1
  Resource name prefix (for the stack's IAM user names) [skyfollower]:
  S3 archive bucket name: skyfollower-archive-example
  Create this bucket? [Y/n] y

  ----------------------------------------------------------------------
  One-time setup in the AWS console (https://console.aws.amazon.com/iam):

    1. Users -> Create user. Name it exactly: skyfollower-bootstrap
       Do NOT enable console access.
    2. On 'Set permissions' pick 'Attach policies directly', then
       'Create inline policy' -> JSON tab, and paste this verbatim:

         { ...the fully-substituted policy from the section above... }

       Name it (e.g. skyfollower-bootstrap-policy) and finish creating the user.
    3. Open skyfollower-bootstrap -> Security credentials -> Create access key
       -> 'Application running outside AWS'. Copy the key ID and secret.
    4. Paste them below. A plain IAM user's key needs no session token.
  ----------------------------------------------------------------------

  AWS access key ID: ...
  AWS secret access key: (hidden)

  → Deploying CloudFormation stack 'skyfollower' (this can take a few minutes)...
  ✓ Stack deployed.

  The one-time IAM user 'skyfollower-bootstrap' has served its purpose.
  Delete it now (key, inline policy, and user)? [Y/n] y
  ✓ One-time IAM user removed.
```

If the self-delete can't finish (an `AccessDenied`, a race), the installer
prints **exactly** what's left and the console steps to remove it by hand —
you're never left unsure whether a stray elevated identity still exists. If
provisioning itself fails, the user is left in place so you can retry, with
the `--delete-bootstrap-user` command to clean up later.

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

### The one-time bootstrap-user modes

Two modes back the installer's [one-time IAM user
path](#the-one-time-iam-user-path-choice-2), and are usable directly:

```sh
# Render the least-privilege provisioning policy, fully substituted.
# No AWS call, no credentials needed.
docker run --rm \
  -e AWS_DEFAULT_REGION=us-east-1 \
  -e ARCHIVE_BUCKET_NAME=skyfollower-archive-example \
  -e RESOURCE_NAME_PREFIX=skyfollower \
  -e BOOTSTRAP_USER_NAME=skyfollower-bootstrap \
  ghcr.io/brentio/skyfollower-aws-setup:latest --print-bootstrap-policy

# Delete that user once done: its access keys, inline policies, then the
# user. Uses the credentials you pass in (the bootstrap user's own).
docker run --rm \
  -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... \
  -e AWS_DEFAULT_REGION=us-east-1 \
  ghcr.io/brentio/skyfollower-aws-setup:latest --delete-bootstrap-user skyfollower-bootstrap
```

`--print-bootstrap-policy` reads the same parameters the create/update mode
does (`ARCHIVE_BUCKET_NAME` is required; `RESOURCE_NAME_PREFIX`,
`GLUE_DATABASE_NAME`, `GLUE_TABLE_NAME`, `ATHENA_WORKGROUP_NAME`,
`STACK_NAME`, `AWS_DEFAULT_REGION` refine the ARNs; `AWS_ACCOUNT_ID`, if
set, tightens the Glue/Athena/IAM ARNs from `*` to your account). The
policy JSON goes to stdout.

`--delete-bootstrap-user` prints, on any failed step, exactly what is left
and the console steps to remove it, then exits non-zero.

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
