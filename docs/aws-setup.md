# AWS Setup

**None of the AWS resources on this page exist in a fresh SkyFollower
deployment.** The archive's Parquet index ([Archive
Processor](/components/archive-processor)'s [Parquet
Index](/components/archive-processor#parquet-index) section) is written to
S3 either way, but nothing can query it — via AWS Athena, against a Glue
Catalog table, using partition projection rather than a Glue Crawler —
until an operator manually creates that Glue database/table, an Athena
workgroup, and two IAM identities by hand, following this page. There is
also no UI or tool in this repo yet that actually *runs* an Athena query
against the index once it exists — this page only covers provisioning the
AWS-side resources a future consumer of the index will need.

No component in this project ever calls a Glue, IAM, or Athena
provisioning API — table, database, workgroup, and identity creation are
one-time (or rare) admin actions a human performs directly in the AWS
console, using whatever access they already have. This sidesteps having to
define, ship, or secure a new AWS credential just for provisioning.

## What each component writes instead

`archive-processor` and `archive-compaction` each ship template JSON files
(`specs/aws/`) baked into their own image, containing a literal
`__BUCKET_NAME__` placeholder wherever their configured `s3.bucket` belongs.
On every startup (`archive-processor`) or every run (`archive-compaction`),
each resolves its own templates against its own config and writes the
result to `{data_dir}/aws-setup/` — pure local string substitution, no AWS
API calls, so this needs zero AWS permissions of its own. Re-running
(restarting) is always safe and always reflects current config: if
`s3.bucket` ever changes, or an image upgrade ships an updated template, the
next restart's output changes to match.

| File | Written by | Destination |
|------|-----------|-------------|
| `specs/aws/glue-table-definition.json` | `archive-processor` (it owns the `index/` schema) | `{data_dir}/aws-setup/glue-table-definition.json` |
| `specs/aws/iam-policies/archive-processor.json` | `archive-processor` | `{data_dir}/aws-setup/iam-policy.json` |
| `specs/aws/iam-policies/archive-compaction.json` | `archive-compaction` | `{data_dir}/aws-setup/iam-policy.json` |

These resolved files are what you copy exact values from in the steps
below — no risk of a typo in your own bucket name or a partition-projection
property.

## AWS resources this sets up

| Resource | Notes |
|---|---|
| S3 bucket | Assumed to already exist — holds `flights/*`, `index/*`, `_compaction_state/*` |
| Glue database | Namespace for the table |
| Glue table, with partition projection | Points at `s3://{bucket}/index/`, Parquet, year/month/day projection — no crawler |
| Athena workgroup + query-results S3 location | Every Athena query needs somewhere to write results |
| S3 lifecycle rule on the query-results prefix | Auto-expires old result files |
| IAM identity for `archive-processor` | Scoped to its own S3 access only |
| IAM identity for `archive-compaction` | A separate, narrower identity — see its README |

## Setup steps (console click-path — no CLI, no CloudShell)

Every resource below has a genuine point-and-click console path.

1. **Confirm the S3 bucket exists.** This setup assumes it already does.

2. **Create the Glue database** (Glue console → Databases → Add database).
   Match the `DatabaseName` in your resolved `glue-table-definition.json`
   (default template value: `skyfollower`).

3. **Create the Glue table** (Glue console → Tables → Add table, "manually
   add" path — not the crawler-based wizard). Manually add each of the 9
   columns and the partition-projection table properties as key/value
   pairs, using your resolved `glue-table-definition.json` (written by
   `archive-processor` to `{data_dir}/aws-setup/`) as the exact source for
   every value — table name, S3 location, each column name/type, and the
   `projection.*`/`storage.location.template` properties. This step is
   tedious (the console wizard has no JSON-paste option for tables, unlike
   IAM in step 6) but fully supported.

4. **Create/configure the Athena workgroup's query-results output
   location** (Athena console → Workgroups). A plain S3-path text field —
   no JSON, no file needed. Pick a dedicated prefix, e.g.
   `s3://{bucket}/athena-results/`.

5. **Set an S3 lifecycle rule on the query-results prefix** (S3 console →
   your bucket → Management → Lifecycle rules) to auto-expire old result
   files, e.g. after 7 days. This is a genuinely separate step from (4) —
   Athena's workgroup config and S3's lifecycle configuration are different
   services with no combined API — but still just one more one-time,
   console-only step.

6. **Create an IAM identity for `archive-processor`** and paste its
   resolved `iam-policy.json` (written to `{data_dir}/aws-setup/` on that
   component's host) directly into the console's JSON policy editor. Raw
   JSON paste works natively for IAM policies, unlike Glue tables — the
   fast step.

7. **Create an IAM identity for `archive-compaction`**, the same way, using
   *its own* resolved `iam-policy.json` from *its own* host's
   `{data_dir}/aws-setup/`. Deliberately a separate identity from
   `archive-processor`'s, not a shared or widened one — see
   `archive-compaction`'s own README (`archive-compaction/README.md`)'s AWS
   Setup section for why.

No IAM policy needs to be authored for steps 1–5 — those are performed
using the human operator's own existing AWS access, not a new credential
this project defines.

## Updating a policy later

AWS managed policies support versioning (the console's "Edit policy" flow,
or `create-policy-version` via the CLI if you prefer). Since the resolved
file a component writes is always the *complete* policy — not a diff — a
future update is just "replace the whole policy with the new full version,"
no manual merging required. AWS only retains 5 versions of a managed
policy, so if you update one repeatedly over time, you'll eventually need
to prune an old version before AWS allows a new one.

Updating the *table* definition works the same way in spirit: a future
image upgrade that changes the schema or partitioning ships an updated
`glue-table-definition.json` template, the next restart writes the new
resolved file to `{data_dir}/aws-setup/`, and you manually apply the change
to the live Glue table via the console (Edit table) — propagating a
changed definition into AWS is always a manual step, by design; no
component here is ever given write access to apply it automatically.
