# pinax-redis-kafka-connect

[![CI](https://github.com/pinax-network/pinax-redis-kafka-connect/actions/workflows/ci.yml/badge.svg)](https://github.com/pinax-network/pinax-redis-kafka-connect/actions/workflows/ci.yml)

A [Kafka Connect](https://kafka.apache.org/documentation/#connect) **sink** connector that
accumulates per-team usage credits in Redis and emails teams as they approach their
monthly usage allowance.

For each Kafka record it:

1. **Increments** a Redis counter (`INCRBYFLOAT`) keyed by the record key, by the
   record's `billed_credits`, and refreshes the key's expiry (`EXPIREAT`).
2. **Notifies** the team by email (via Mandrill / Mailchimp Transactional) the first
   time their accumulated usage crosses a milestone of their included allowance.

Each batch is written as one Redis [transaction](https://redis.io/docs/latest/develop/interact/transactions/)
(`MULTI`/`EXEC`), which also stores the Kafka offset of the last record applied per
partition. A record is therefore counted exactly once, and a Redis snapshot knows where
in the topic it ends.

---

## Requirements

|                   |                                                                               |
|-------------------|-------------------------------------------------------------------------------|
| **Java**          | 17+ to run (the connector and its `connect-api` dependency are compiled for Java 17); JDK 21+ to build |
| **Kafka Connect** | 4.x runtime                                                                   |
| **Redis**         | Reached via **Redis Sentinel** (the connector uses `JedisSentinelPool`)       |
| **Docker**        | Only for running the integration tests locally                                |

## Build

```bash
mvn clean package
```

This produces a shaded, dependency-bundled plugin jar:

```
target/pinax-redis-kafka-connect.jar
```

## Install

Drop the jar into a directory on your Connect worker's `plugin.path`, e.g.:

```
plugin.path=/opt/kafka/connect-plugins
```

```
/opt/kafka/connect-plugins/pinax-redis-kafka-connect/pinax-redis-kafka-connect.jar
```

and restart the worker.

## Configuration

| Property          | Required | Default              | Description                                                     |
|-------------------|----------|----------------------|-----------------------------------------------------------------|
| `master`          | no       | `mymaster`           | Redis Sentinel master name.                                     |
| `hosts`           | no       | `localhost:6379`     | Comma-separated list of Sentinel `host:port` addresses.         |
| `redis_username`  | no       | —                    | User to authenticate as on the Redis nodes. Empty with a `redis_password` set: the `default` user. |
| `redis_password`  | no       | —                    | Password for the Redis nodes. Empty: no authentication.         |
| `redis_sentinel_username` | no | —                  | User to authenticate as on the Sentinels. Empty with a `redis_sentinel_password` set: the `default` user. |
| `redis_sentinel_password` | no | —                    | Password for the Sentinels. They authenticate separately from the Redis nodes, so set it also when it is the same password. Empty: no authentication. |
| `offsetsKey`      | no       | `kafka-connect:redis-sink:offsets` | Prefix of the hashes holding the applied offsets, one per topic (`<offsetsKey>:<topic>`). |
| `retryBackoffMs`  | no       | `5000`               | Milliseconds before a batch is retried after Redis was unreachable or not accepting writes. |
| `conflictRetryBackoffMs` | no | `1000`              | Milliseconds before a batch is retried after its transaction was refused because the stored offsets changed. |
| `maxRetries`      | no       | `12`                 | How often in a row a batch is retried before the task fails. About a minute with the default backoff. |
| `from`            | no       | `info@pinax.network` | `From` address for notification emails (must be a valid email). |
| `mailchimpApiKey` | **yes**  | —                    | Mandrill / Mailchimp Transactional API key.                     |
| `templateSlug`    | **yes**  | —                    | Mandrill template name used to render the usage email.          |

Needs a Kafka Connect worker of version 3.6 or later.

Plus the standard Kafka Connect sink properties (`topics`, converters, `errors.*`, …).
`tasks.max` must be `1`: a config with a higher value is rejected, see "A single task"
below.

### Example connector configuration

```json
{
  "name": "pinax-redis-usage-sink",
  "config": {
    "connector.class": "com.pinax.kafka.redis.connect.sink.RedisSinkConnector",
    "tasks.max": "1",
    "topics": "team-usage",
    "master": "mymaster",
    "hosts": "sentinel-a:26379,sentinel-b:26379,sentinel-c:26379",
    "from": "billing@pinax.network",
    "mailchimpApiKey": "${file:/secrets/mandrill.properties:apiKey}",
    "templateSlug": "monthly-usage-update",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter"
  }
}
```

## Record format

- **Key** — the Redis key to accumulate into (e.g. a team identifier).
- **Value** — a JSON object:

| Field                | Type    | Used for                                                                                                                     |
|----------------------|---------|------------------------------------------------------------------------------------------------------------------------------|
| `billed_credits`     | number  | Amount added to the key via `INCRBYFLOAT` (credits are stored as integer cents, i.e. a 100× multiplier).                     |
| `expiration`         | number  | Unix epoch **seconds**; applied to the key via `EXPIREAT`.                                                                   |
| `included_credits`   | integer | The team's included allowance (cents). Notifications are evaluated whenever this is `> 0`.                                   |
| `credit_cutoff`      | integer | Ignored. Previously gated notifications; no longer read by the sink.                                                          |
| `team_billing_email` | string  | Recipient of the usage email.                                                                                                |
| `team_name`          | string  | Rendered into the email template.                                                                                            |
| `team_plan`          | string  | Rendered into the email template.                                                                                            |

Example value:

```json
{
  "billed_credits": 1250,
  "expiration": 1735689600,
  "included_credits": 100000,
  "credit_cutoff": 0,
  "team_billing_email": "team@acme.example",
  "team_name": "Acme",
  "team_plan": "Pro"
}
```

### Notification milestones

When `included_credits > 0`, an email is sent when accumulated usage crosses one of these
multiples of the allowance. If a single batch leaps past several at once, only the
**highest** milestone crossed in that batch is notified:

```
50%   75%   90%   100%   150%   200%
```

## Behaviour & delivery semantics

- **Counters are exactly-once.** Every batch is one `MULTI`/`EXEC` transaction that
  applies the `INCRBYFLOAT` increments and stores, in the hash `<offsetsKey>:<topic>`,
  the offset of the last record applied per partition (field = partition). Before a
  batch is written, records at or below the stored offset are dropped: a batch that
  Connect delivers again after a failure, a restart or a rewind of the consumer group
  is not counted twice, and its notifications are not sent twice.
- **Notifications are at-most-once.** A milestone is detected from the result of the
  transaction and its email is sent right after it. When the transaction is applied but
  the task does not get to send (the reply from Redis is lost, the worker stops, or the
  mail API call fails), the batch is skipped as already counted when it arrives again
  and that email is never sent. The team is notified again at the next milestone.
- **Redis decides where a partition continues.** When a partition is assigned, the
  task moves the consumer to the record after the stored offset, whatever the consumer
  group has committed. It does so on the next `put()`, which Connect also calls without
  records, so a partition nothing is produced to is covered too. A partition whose
  batch in hand already continues right after the stored offset is left where it is;
  otherwise its records in hand are left out and arrive again from the new position. A
  partition without a stored offset stays where the consumer group has it.
  The same is done for all assigned partitions whenever the task reconnects to Redis:
  after a Sentinel failover the new master may lack the last writes, and it then
  continues from the offsets it holds.
- **Transformations.** Offsets are stored and compared by the topic, partition and
  offset a record was read from, so a transformation that renames the topic does not
  affect them. The hash is named after the source topic.
- **Restoring Redis.** Because counters and offsets are written together, a snapshot
  (RDB or AOF) is consistent by itself. Stop the connector, restore the snapshot, start
  the connector: it continues at the offsets in the snapshot and re-applies what came
  after it. Things to know:
  - The topic must still hold those records. If retention has removed them, the
    consumer continues at the earliest record left and the removed ones stay uncounted.
  - Re-applied records that cross a milestone send their notification again.
  - A Redis without the offset hash (empty, or last written by a version before 0.4.0)
    follows the consumer group as before. To rebuild the counters from the topic, delete
    them and reset the consumer group's offsets.
  - A partition gets its entry in the hash with the first record applied by 0.4.0 or
    later. A snapshot taken before that restores the partition to wherever the consumer
    group is, and its records since the snapshot are not applied again. Check with
    `HGETALL <offsetsKey>:<topic>` that every partition is listed; the task warns when
    it is assigned a partition that is missing from an existing hash.
  - **Rolling back.** A version before 0.4.0 counts records without updating the offset
    hash. After running one, delete `<offsetsKey>:<topic>` before starting 0.4.0 or later
    again. Otherwise the task goes back to the stale offsets and everything the old
    version counted is counted a second time. The task cannot tell this from a restored
    snapshot; it logs a warning with the number of records whenever it moves a partition
    back behind the batch in hand.
  - The offsets belong to one Kafka cluster. When the connector is pointed at another
    cluster or a re-created topic, delete `<offsetsKey>:<topic>` first. The task fails
    when it notices: it moves a partition to a stored offset the topic does not have,
    and Kafka starts the consumer over. A stored offset the new topic does have cannot
    be told apart and is applied to it.
- **A single task.** The offsets of all partitions of a topic are in one hash, and every
  transaction `WATCH`es it, so tasks running side by side would refuse each other's
  transactions. The connector therefore always creates one task. A config with
  `tasks.max` higher than 1 is rejected when it is submitted; one stored before that is
  brought down to one task, with a warning in the log. The `WATCH` remains for a task that was replaced but has
  not noticed yet: when it stored offsets in between, the transaction is refused,
  nothing is applied and the batch is retried after `conflictRetryBackoffMs`.
- **Retries.** A batch that failed for a passing reason is delivered again by Connect
  after `retryBackoffMs` (Redis unreachable, or not accepting writes: `LOADING`, `BUSY`,
  `READONLY`, `MISCONF`, `OOM`) or
  `conflictRetryBackoffMs` (transaction refused). Connect would do so forever, with the
  task `RUNNING` while nothing is counted. After `maxRetries` retries in a row without a
  batch going through, the task fails instead, which shows in its status and leaves it
  to whatever restarts failed tasks.
- **Not rolled back.** Redis does not undo a transaction when it rejects one of its
  commands: the rest of the batch and the offsets are applied.
  - A rejected increment or expiration (`WRONGTYPE`, "not a valid float", "invalid expire
    time") concerns a single record. It is logged as an error with the key, topic,
    partition and offset. The notifications of the other records are sent, then the
    task fails with a `DataException`. After a restart it continues behind that batch;
    the rejected increment or expiration is lost.
  - Offsets that could not be stored fail the task with a `DataException`: carrying on
    would count the batch again after the next restart.
- **Invalid records fail the task.** A record whose value is not JSON or lacks
  `billed_credits` or `expiration` fails the task with a `DataException`. The records
  before it in its partition, and those of other partitions, are applied first, so the
  stored offset ends up right before it. The task fails on the same record again after
  a restart, until the partition is moved to the record by hand (below): records at or
  below the stored offset are not parsed, and nothing else is skipped.
- **Moving a partition by hand.** Resetting the consumer group has no effect on a
  partition with a stored offset. To skip or repeat records, stop the connector, set the
  offset of the last record to treat as applied with
  `HSET <offsetsKey>:<topic> <partition> <offset>`, and start it again. Stop it, do not
  pause it: a task that keeps running reads the stored offsets only for records it is
  delivered, does not move its consumer, and overwrites the offset with its next batch.
- **Error classification** (`put()` maps Redis failures to the right Connect signal):

  | Failure | Mapped to | Effect |
  |---|---|---|
  | Connection loss, pool exhaustion, sentinel failover (`JedisException`) | `RetriableException` | Connect retries the batch; the task fails after `maxRetries` in a row |
  | Offsets changed by another task between `WATCH` and `EXEC` | `RetriableException` | Connect retries the batch; the task fails after `maxRetries` in a row |
  | Auth / ACL error (`JedisAccessControlException`) | `ConnectException` | Task fails fast (permanent misconfig) |
  | Redis temporarily not accepting writes — `LOADING`, `BUSY`, `READONLY`, `MISCONF`, `OOM`, `NOREPLICAS`, also as the reason of an `EXECABORT` | `RetriableException` | Nothing was applied; Connect retries the batch on a new connection; the task fails after `maxRetries` in a row |
  | Kafka does not have the offset stored in Redis | `ConnectException` | Task fails before anything is written; the offset hash belongs to another topic or cluster |
  | Increment or expiration of one record rejected inside the transaction | `DataException` | Logged; the rest of the batch is applied and notified, then the task fails |
  | Offsets rejected inside the transaction, or another permanent error on the offset hash (`JedisDataException`) | `DataException` | Non-retriable (retrying can't help) |
  | Record value that cannot be parsed (`JSONException`) | `DataException` | Non-retriable; the records before it are applied, then the task fails |

- **Connection lifecycle.** A failed batch drops and rebuilds the Redis connection on
  the next `put()`; a transient outage at startup is tolerated and retried lazily.
- **Notifications are best-effort and non-fatal.** A record with a missing/malformed
  notification field is logged and skipped — it never fails an already-committed write.

## Testing

```bash
# Unit tests only (fast, no Docker)
mvn test

# Unit + integration tests (integration tests need a running Docker daemon)
mvn verify
```

- **Unit tests** (`*Test`, Surefire) — JUnit 6 + Mockito. Cover config validation,
  the connector wiring, mail-content rendering, and `RedisSinkTask`'s error
  classification and notification logic (Redis and the mailer are mocked).
- **Integration tests** (`*IT`, Failsafe) — JUnit 6 + [Testcontainers](https://testcontainers.com/).
  Start a real Redis container and verify the transaction semantics the task relies on
  (accumulation, TTL, stored offsets, a refused transaction after a concurrent offset
  change, and that per-command errors surface at `Response.get()`). They
  **self-skip** when no Docker daemon is available, except in CI (`CI` set in the
  environment), where a missing daemon fails the build.

## Static analysis

CI (and local runs) enforce:

```bash
mvn checkstyle:check   # import hygiene + high-value correctness checks (checkstyle.xml)
mvn spotbugs:check     # bug detection (excludes in spotbugs-exclude.xml)
```

## Continuous integration

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs on every push to `main`
and on every pull request: build, unit + integration tests, Checkstyle, and SpotBugs,
and uploads the plugin jar as a build artifact.

[`.github/workflows/upload-jar-on-release.yml`](.github/workflows/upload-jar-on-release.yml)
builds the jar on a GitHub Release and attaches it to the release.
