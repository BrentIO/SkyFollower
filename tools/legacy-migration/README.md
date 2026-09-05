# Legacy Migration

One-time tool that copies the legacy MongoDB-tracked flight archive
([SkyFollower-legacy](https://github.com/BrentIO/SkyFollower-legacy)) into
this repo's S3/Parquet archive format. Not a standing component: it isn't
started by `scripts/install.sh`, isn't part of any host's normal
`docker-compose.*.yaml` stack, and isn't published to GHCR. The operator
builds and runs it by hand, once, wherever Mongo/S3/RabbitMQ reachability
is most convenient.

**This can only run once legacy MongoDB has stopped taking production
writes.** Querying it concurrently with live writes risks reading a
document mid-update, and running migration reads against the same
instance serving production traffic is a real load concern independent of
query count. That is an operational precondition, not something this tool
can detect or wait out on its own.

## What it does

- **Producer** (run once per pass): walks every calendar day in
  `--start-date`/`--end-date` (both inclusive) and publishes one message
  per day to the `legacy-migration` queue. Also runs a one-time sweep for
  any document whose `first_message` falls outside that range -- such a
  document would never match any day's query and would otherwise be
  silently skipped forever.
- **Worker** (long-lived, scale with `--scale worker=N`): consumes one day
  at a time. For every flight in that day: runs data-quality guards
  (zero messages, `last_message` before `first_message`, missing
  `aircraft.icao_hex`) -- a failure sends the flight to the
  `legacy-migration-dlq` queue and moves on. Otherwise, copies the
  flight's S3 object from the legacy bucket's flat `{_id}.gz` key to the
  new bucket's dated `flights/{YYYY}/{MM}/{DD}/{uuid}.json.gz` key (via
  `shared/archive_index.py`'s `build_s3_key()` -- the exact function the
  live archive processor uses, so there is only one implementation of the
  key format). Skips the copy if the destination object already exists
  (idempotent against redelivery and a deliberately overlapping second
  pass). Once the day's flights are all processed, uploads one compacted
  Parquet index file per day to `index/year={YYYY}/month={MM}/day={DD}/legacy-migration.parquet`.
- **Verify**: run once, right before deleting the legacy bucket by hand.
  Reconciles per-day Mongo counts against the destination bucket's object
  counts, and confirms every copied object is byte-identical to its
  legacy original via an ETag (MD5) comparison. Read-only.

**This tool never deletes anything, in either bucket.** Every copy is a
cross-bucket `CopyObject`; the legacy bucket is deleted by hand, once, by
the operator, only after `verify` reports clean.

## Two-pass execution

Legacy Mongo only ever offloads a flight's `positions`/`velocities` to S3
in the background, on a delay -- some recent flights won't have a
`migrated` timestamp yet at cutover time. This tool only ever considers
documents where `migrated` exists; a document without one has no `.gz`
object to copy and is entirely out of this tool's scope.

1. **Pass 1 -- the bulk history.** `--start-date` defaults to
   `2022-07-11` (legacy history's earliest flight); `--end-date` is the
   cutover date. This is the only pass that should ever run the producer's
   catch-all sweep (see "Catch-all sweep" below) -- it's the only run
   whose range is the full recorded history.
2. **Operator step.** Drive the remaining un-`migrated` tail to
   `migrated` using the legacy system's own offload tool.
3. **Pass 2 -- the tail.** Re-run with `--start-date` at (or before) the
   start of that tail (and `--end-date` left at today). Days already fully
   processed in pass 1 are cheap no-ops (see Idempotency below), so an
   overlapping range is safe and is the recommended way to run it. The
   producer auto-detects that this narrower range isn't the full history
   and skips the sweep -- see below.

Both passes are the same binary, just different date bounds -- there is
no separate "top-up" mode.

## Catch-all sweep

Before the day-walk, the producer can additionally sweep for `migrated`
documents whose `first_message` falls entirely outside
`[--start-date, --end-date]` -- genuinely anomalous data (clock skew,
corruption) that no day-walk range would ever reach, and send them to the
DLQ for manual review.

This sweep is **only meaningful across the full recorded history**. For
any narrower range -- including pass 2's recommended overlapping tail
range above -- its "outside the range" predicate instead matches the
entire bulk of already-migrated history, flooding the DLQ with millions of
harmless entries. The producer auto-detects this: it only runs the sweep
when `--start-date` is at or before the earliest recorded flight and
`--end-date` is today or later, and otherwise logs that it's skipping the
sweep. An operator can still force it either way with `--sweep`/
`--no-sweep`.

## Idempotency

Two layers:

- **Per-flight**: a `HeadObject` on the computed destination key before
  copying; skipped if it already exists.
- **Per-day**: the RabbitMQ ack *is* the completion signal, not a separate
  marker. If a worker dies mid-day, the message is redelivered and
  reprocessed; the per-flight check above makes that safe. Re-running a
  day rewrites that day's compacted Parquet file wholesale from Mongo's
  current contents -- intended, since the index is derived state.

Re-running an overlapping day-walk range is always safe regardless of the
sweep (above) -- the two are independent. The sweep is a one-time scan for
data outside every range this tool will ever day-walk; the day-walk itself
has no memory of prior runs at all.

## Configure

`docker-compose.legacy-migration.yaml` is pure `${VAR}` interpolation --
it needs an env file to read those variables from. Use a **dedicated
`tools/legacy-migration/.env`**, not the repo-root `.env` a deployment
host already has from `scripts/install.sh`: that file carries the shared
`skyfollower` application user's broker credentials and no Mongo/S3
variables at all, and merging this tool's dedicated
`skyfollower-migration` credentials into it is error-prone -- easy to
end up running this tool as the application user by mistake.

```bash
cp tools/legacy-migration/.env.example tools/legacy-migration/.env
# edit tools/legacy-migration/.env, filling in every value -- see the
# table below and the Mongo credential/RabbitMQ setup/IAM sections
```

Every `docker compose` command in this README then needs
`--env-file tools/legacy-migration/.env` alongside `-f
docker-compose.legacy-migration.yaml`, run from the repo root (the build
context stays repo-root `.`; only the interpolation source moves):

```bash
docker compose -f docker-compose.legacy-migration.yaml \
  --env-file tools/legacy-migration/.env \
  run --rm producer --start-date 2022-07-11 --end-date 2026-09-01
```

`.env.example` is tracked in git (the repo-root `.gitignore`'s blanket
`.env` rule already excludes the real `tools/legacy-migration/.env` from
being committed by accident); `tools/legacy-migration/.env` itself never is.

**Split-host note:** the migration containers can run anywhere with
reachability to Mongo/S3/the broker -- they don't need to run on any
particular host in the topology. `RABBITMQ_HOST` must point at the core
host running `docker-compose.core.yaml`, and that host must allow inbound
5672 from wherever this tool runs, if that's a different host.

## Configuration

Environment variables (see `shared/config.py`'s `mongo`,
`legacy_migration_s3`, and `rabbitmq` blocks):

| Variable | Required | Notes |
|---|---|---|
| `MONGO_URI` | yes | Read-only credential -- see below |
| `MONGO_DATABASE` | no | Default `SkyFollower` |
| `MONGO_COLLECTION` | no | Default `flights` |
| `SOURCE_S3_BUCKET` | yes | Legacy bucket (e.g. `com.skyfollower.datastore`) |
| `DEST_S3_BUCKET` | yes | New archive bucket, provisioned via the `aws-setup` CloudFormation stack |
| `AWS_DEFAULT_REGION` / `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | yes | Temporary migration identity -- see IAM below |
| `RABBITMQ_HOST` / `RABBITMQ_PORT` / `RABBITMQ_USERNAME` / `RABBITMQ_PASSWORD` | yes | Dedicated `skyfollower-migration` user -- see below, never the shared application user |
| `LOG_LEVEL` | no | Default `info`. `debug` logs every migrated flight's `_id` and destination key -- ~8.6M lines across the full history, opt in deliberately |

## Mongo credential

These run against the external legacy MongoDB directly (via `mongosh` or
equivalent) -- it isn't a container anywhere in this repo, so unlike
RabbitMQ setup above, there's no `docker compose exec` involved here.

A read-only role scoped to the one collection this tool reads. Example
(adjust database/collection names to match `MONGO_DATABASE`/
`MONGO_COLLECTION`):

```javascript
db.createRole({
  role: "skyfollowerMigrationReadOnly",
  privileges: [
    { resource: { db: "SkyFollower", collection: "flights" }, actions: ["find"] },
  ],
  roles: [],
})
db.createUser({
  user: "skyfollower-migration",
  pwd: "<generated-password>",
  roles: [{ role: "skyfollowerMigrationReadOnly", db: "SkyFollower" }],
})
```

Requires a partial index on `first_message`, scoped to `migrated: {$exists:
true}` -- every query this tool issues is covered by it directly. Confirm
it exists before running with any real concurrency:

```javascript
db.flights.getIndexes()
// expect: { key: { first_message: 1 }, name: "first_message_migrated_partial",
//           partialFilterExpression: { migrated: { $exists: true } } }
```

**Never issue a collection-wide count against this collection.** None of
`{migrated: {$exists: false}}`, `total_messages`, or `aircraft.icao_hex`
predicates are covered by the index above -- each is a full scan over
millions of documents. This tool's own queries never do this; if you need
to check something ad hoc, scope it by an indexed `first_message` range
first.

## RabbitMQ setup

A dedicated `skyfollower-migration` user, not the shared `skyfollower`
application user -- this tool is a separate process with a separate
lifetime, and `shared/rabbitmq_topology.py`'s
`SKYFOLLOWER_RABBITMQ_RESOURCE_PATTERN` is deliberately left unmodified.

The broker runs as the `rabbitmq` service in `docker-compose.core.yaml` --
there is no host `rabbitmqctl` binary. Every command below runs as
`docker compose exec` into that container, **on the core host, from its
deployment directory** (matching `scripts/install.sh`'s own convention for
provisioning RabbitMQ users):

```bash
docker compose -f docker-compose.core.yaml exec -T rabbitmq \
  rabbitmqctl add_user skyfollower-migration '<generated-password>'
docker compose -f docker-compose.core.yaml exec -T rabbitmq \
  rabbitmqctl set_permissions -p / skyfollower-migration \
    '^legacy-migration(-dlq)?$' \
    '^(legacy-migration(-dlq)?|amq\.default)$' \
    '^legacy-migration(-dlq)?$'
```

(configure / write / read, in that order -- `amq.default` must be in the
**write** pattern since publishing via the default exchange authorizes
against that exchange resource.)

RabbitMQ's default `consumer_timeout` (30 minutes) is broker-side and
would forcibly close a worker's channel and redeliver its message if a
day with several thousand flights plus S3 retry backoff runs long. Scope a
longer timeout to just this queue, rather than the broker-wide default
(which would also affect the live message-processor queues sharing the
broker):

```bash
docker compose -f docker-compose.core.yaml exec -T rabbitmq \
  rabbitmqctl set_policy consumer-timeout-migration '^legacy-migration$' \
    '{"consumer-timeout":3600000}' --apply-to queues
```

Teardown once the migration is complete and `verify` reports clean, and
the DLQ has been reviewed:

```bash
docker compose -f docker-compose.core.yaml exec -T rabbitmq rabbitmqctl delete_user skyfollower-migration
docker compose -f docker-compose.core.yaml exec -T rabbitmq rabbitmqctl clear_policy consumer-timeout-migration
docker compose -f docker-compose.core.yaml exec -T rabbitmq rabbitmqctl delete_queue legacy-migration
docker compose -f docker-compose.core.yaml exec -T rabbitmq rabbitmqctl delete_queue legacy-migration-dlq
```

## IAM

`iam-policy-example.json` -- copy-only, no `s3:DeleteObject` anywhere, on
either bucket. Create a temporary identity for this run only and
revoke/detach it immediately afterward -- never a standing identity.

```bash
sed -e 's|__SOURCE_BUCKET_NAME__|com.skyfollower.datastore|g' \
    -e 's|__DEST_BUCKET_NAME__|NEW_ARCHIVE_BUCKET|g' \
    tools/legacy-migration/iam-policy-example.json > /tmp/legacy-migration-policy.json

aws iam create-user --user-name skyfollower-legacy-migration
aws iam put-user-policy --user-name skyfollower-legacy-migration \
  --policy-name legacy-migration --policy-document file:///tmp/legacy-migration-policy.json
aws iam create-access-key --user-name skyfollower-legacy-migration
#   -> put the AccessKeyId/SecretAccessKey into tools/legacy-migration/.env
#      as AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY
```

Teardown, after `verify` reports clean and the legacy bucket is deleted by hand:

```bash
aws iam delete-access-key --user-name skyfollower-legacy-migration --access-key-id KEY_ID
aws iam delete-user-policy --user-name skyfollower-legacy-migration --policy-name legacy-migration
aws iam delete-user --user-name skyfollower-legacy-migration
```

## Running it

```bash
COMPOSE="docker compose -f docker-compose.legacy-migration.yaml --env-file tools/legacy-migration/.env"

# Pass 1: publish every day from legacy history's start through cutover.
$COMPOSE run --rm producer --start-date 2022-07-11 --end-date 2026-09-01

# Scale workers to taste; long-lived, drains the queue and exits nothing
# on its own -- stop with Ctrl+C / `docker compose down` once idle.
$COMPOSE up --build --scale worker=8

# ... operator drives the remaining un-migrated tail via the legacy
# offload tool, then re-run producer for pass 2 with an overlapping range ...

# Before deleting the legacy bucket by hand:
$COMPOSE run --rm verify --start-date 2022-07-11 --end-date 2026-09-01
```

Logs go to stdout and to `./logs/<container-hostname>.log` (bind-mounted,
one file per worker container since `--scale` produces one hostname each).

## Dead-letter queue

`legacy-migration-dlq` holds one message per flagged flight:
`{"_id": "<uuid>", "reason": "<plain text>"}`. Publish-and-forget -- no
retry semantics, since this is a dead end for human review, not a
transient failure. Expected to hold a small number of documents (measured
at design time: ~6, out of ~8.75M). Drain and review it manually; treat
this as a required step before considering the migration complete,
alongside a clean `verify` run.
