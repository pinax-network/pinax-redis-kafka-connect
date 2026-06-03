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

Writes are issued through a single Redis [pipeline](https://redis.io/docs/latest/develop/use/pipelining/)
per batch and committed with one `SYNC`.

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
| `included_credits`   | integer | The team's included allowance (cents). Notifications are only evaluated when this is `> 0` and differs from `credit_cutoff`. |
| `credit_cutoff`      | integer | Allowance/cutoff comparison value.                                                                                           |
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

When `included_credits > 0` (and `!= credit_cutoff`), an email is sent the first time
accumulated usage crosses each of these multiples of the allowance:

```
50%   75%   90%   100%   150%   200%
```

## Behaviour & delivery semantics

- **At-least-once.** On a retryable failure Connect re-delivers the batch. The
  `INCRBYFLOAT` increment is **not** idempotent, so a retry may count some credits more
  than once. This is accepted in exchange for **never missing a usage notification** —
  notifications are sent after each successful write.
- **Error classification** (`put()` maps Redis failures to the right Connect signal):

  | Failure | Mapped to | Effect |
    |---|---|---|
  | Connection loss, pool exhaustion, sentinel failover (`JedisException`) | `RetriableException` | Connect retries the batch |
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
  Start a real Redis container and verify the pipeline semantics the task relies on
  (accumulation, TTL, and that per-command errors surface at `Response.get()`). They
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
