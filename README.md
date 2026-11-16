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
| **Java**          | 17+ (the connector and its `connect-api` dependency are compiled for Java 17) |
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
| `offsetsKey`      | no       | `kafka-connect:redis-sink:offsets` | Prefix of the hashes holding the applied offsets, one per topic (`<offsetsKey>:<topic>`). |
| `from`            | no       | `info@pinax.network` | `From` address for notification emails (must be a valid email). |
| `mailchimpApiKey` | **yes**  | —                    | Mandrill / Mailchimp Transactional API key.                     |
| `templateSlug`    | **yes**  | —                    | Mandrill template name used to render the usage email.          |

Plus the standard Kafka Connect sink properties (`topics`, `tasks.max`, converters,
`errors.*`, …).

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
- **Redis decides where a partition continues.** On the first batch after a partition
  is assigned, the task compares it with the stored offset. If the batch starts beyond
  the record that follows it, the task rewinds the consumer to that record. Offsets
  have gaps (transaction markers), so this also happens once per assignment without
  anything being missing; the records then simply arrive again.
- **Restoring Redis.** Because counters and offsets are written together, a snapshot
  (RDB or AOF) is consistent by itself. Stop the connector, restore the snapshot, start
  the connector: it rewinds to the offsets in the snapshot and re-applies what came
  after it. Things to know:
  - The topic must still hold those records. If retention has removed them, the
    consumer continues at the earliest record left and the removed ones stay uncounted.
  - Re-applied records that cross a milestone send their notification again.
  - A Redis without the offset hash (empty, or last written by a version before 0.4.0)
    follows the consumer group as before. To rebuild the counters from the topic, delete
    them and reset the consumer group's offsets.
  - The offsets belong to one Kafka cluster. When the connector is pointed at another
    cluster or a re-created topic, delete `<offsetsKey>:<topic>` first, or the stored
    offsets are applied to the new topic.
- **Two tasks, one partition.** The offset hashes are `WATCH`ed, so a transaction is
  refused when another task stored offsets in between (a task that lost its partitions
  in a rebalance but has not noticed yet). Nothing is applied and the batch is retried.
- **Not rolled back.** Redis does not undo a transaction when one command fails
  (`WRONGTYPE`, "not a valid float"): the other increments and the offsets are applied,
  and the task fails with a `DataException`. After a restart it continues behind that
  batch; the rejected increment is lost.
- **Error classification** (`put()` maps Redis failures to the right Connect signal):

  | Failure | Mapped to | Effect |
  |---|---|---|
  | Connection loss, pool exhaustion, sentinel failover (`JedisException`) | `RetriableException` | Connect retries the batch |
  | Offsets changed by another task between `WATCH` and `EXEC` | `RetriableException` | Connect retries the batch |
  | Auth / ACL error (`JedisAccessControlException`) | `ConnectException` | Task fails fast (permanent misconfig) |
  | Command/data error — `WRONGTYPE`, "not a valid float" (`JedisDataException`) | `DataException` | Non-retriable (retrying can't help) |
  | Malformed record payload (`JSONException`) | `DataException` | Non-retriable |

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

- **Unit tests** (`*Test`, Surefire) — JUnit 5 + Mockito. Cover config validation,
  the connector wiring, mail-content rendering, and `RedisSinkTask`'s error
  classification and notification logic (Redis and the mailer are mocked).
- **Integration tests** (`*IT`, Failsafe) — JUnit 5 + [Testcontainers](https://testcontainers.com/).
  Start a real Redis container and verify the transaction semantics the task relies on
  (accumulation, TTL, stored offsets, a refused transaction after a concurrent offset
  change, and that per-command errors surface at `Response.get()`). They
  **self-skip** when no Docker daemon is available.

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
