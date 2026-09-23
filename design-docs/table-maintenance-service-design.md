<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Design of Table Maintenance Service in Gravitino

## 1. Background

The Table Maintenance Service (TMS) in Gravitino is currently an alpha feature.
The `maintenance/optimizer` package already contains the execution core for statistics collection,
rule evaluation, strategy recommendation, metrics query, and job submission (`Updater`,
`Recommender`, providers, `JobSubmitter`).

Today that core is not hosted as a long-running Gravitino service. Without a server-side component:

1. There is no stable **server-side signal** after Iceberg commits. Engines would otherwise each need a
   TMS-specific listener, or operators must schedule maintenance externally.
2. Configuration, audit, and service-level metrics are hard to centralize when execution is
   ad hoc and process-local.
3. Each run creates its own runtime and provider instances instead of a shared service lifecycle.
4. When Spark maintenance work is needed, submitted work already returns a `jobId` owned by the
   Gravitino job framework. That job-status boundary should stay.

This design turns TMS into a **main-server REST plugin** on port **8090** (same pattern as IdP
via `gravitino.server.rest.extensionPackages`) with colocated IRC, reusing the existing optimizer
execution core.

TMS uses a **dual trigger model** (§5.3):

- **Commit path** — IRC wakes **compaction only** after each successful commit (bounded executor,
  per-policy claim, `minIntervalMs`, Recommender → submit). Best effort.
- **Scheduled path** — a **K8s CronJob** (or any external orchestrator) calls
  `POST …/maintenance/scheduled-run` on a cadence. TMS scans tables, refreshes statistics, and runs
  manifest rewrite, snapshot expiry, and orphan cleanup. At-least-once latest-state.

There is **no** in-process periodic scheduler and **no** cluster-wide scheduler lease inside
Gravitino (§4.5–§4.6).

---

## 2. Goals

1. **In-process plugin on the main server**: Load Table Maintenance through
   `gravitino.server.rest.extensionPackages` (Jersey 2 `Feature`, same pattern as IdP) so the IRC
   callback is registered in the main JVM. The commit path does **not** use HTTP. Operator calls that
   replace the optimizer CLI are the ops APIs in **§7**.
2. **Four independent maintenance policies**: One built-in policy **type** per activity
   (`compaction`, `rewrite-manifests`, `snapshot-expiry`, `orphan-removal`). Each attaches and
   schedules on its own grain (for example snapshot retention at catalog, compaction per table).
   No combined `system_iceberg_table_maintenance` storage model (§5.2).
3. **Maintenance profile (convenience)**: A profile such as `standard` creates and attaches all four
   policies with sensible defaults in one step. Profiles are **not** a fifth policy type (§5.2).
4. **Precedence per maintenance type**: For each maintenance type, the **nearest** attachment along
   `table → schema → catalog → metalake` wins. Policies are **not** additive for maintenance (§5.2).
5. **Dual trigger model**: Commit path drives **compaction only** (§5.4). Scheduled path is driven by
   an **external clock** (K8s CronJob → `scheduled-run` API) for manifest rewrite, snapshot expiry,
   and orphan cleanup (§5.3, §5.5–§5.6).
6. **Reuse existing optimizer execution core**: Both paths invoke the same `Updater` /
   `Recommender` / job-submit paths in `maintenance/optimizer` as **in-process methods**.
7. **Job framework compatibility**: Spark maintenance work continues to use the Gravitino job
   framework. TMS returns or records submitted `jobId` values but does not own job status.
8. **Govern Policy reuse**: Maintenance policies stay on existing `policy_meta` and metalake Policy
   APIs (create / alter / enable / disable / associate). TMS does **not** introduce a parallel policy
   store or `/api/maintenance/table/policies` CRUD.
9. **Multi-node safe processing**: Shared DB **per-policy claims** so only one TMS replica runs
   evaluate → submit for a given `(table, policy)` at a time (§6). Gravitino replicas remain **peers**
   for IRC and commit-path compaction; there is no maintenance **leader node** (§4.5).
10. **Bounded executor on commit path**: Evaluation **must not** run on the IRC commit thread. A
    bounded executor is a **design requirement**, not an implementation detail (§5.4).

---

## 3. Non-Goals

1. **Standalone maintenance daemon**: No separate process or
   `gravitino-iceberg-rest-server.sh`-style entrypoint.
2. **Dedicated auxiliary HTTP listener**: No `GravitinoAuxiliaryService`, no isolated
   `gravitino.maintenance.classpath`, and no dedicated TMS port (for example **9301**). TMS is not
   a dedicated listener like `iceberg-rest` / `lance-rest`.
3. **Built-in HA scheduler**: No `MaintenanceScheduler`, no `scheduler.intervalMs`, and no
   cluster-wide scheduler lease inside Gravitino (§4.5). Timed maintenance uses an external orchestrator.
4. **Per-commit event log**: No `table_maintenance_event` table (§6.3). Interval and recovery use
   `table_maintenance_state` + `job_run_meta`, not one INSERT per commit.
5. **Provider SPI rewrite**: Does not replace `StatisticsUpdater`, `StatisticsCalculator`,
   `StatisticsProvider`, `StrategyProvider`, `TableMetadataProvider`, or `JobSubmitter` contracts.
6. **Engine-side commit report path**: Engines that bypass Gravitino Iceberg REST are out of scope
   for the commit compaction path.
7. **Commit-path HTTP or Kafka**: No `POST …/events/iceberg-commit`, no health resource, and no Kafka
   produce/consume path. Commit handling is **in-process only** (§5.1.1).

---

## 4. Solution Investigations

### 4.1 Option A: Keep process-local execution only

Continue running all optimizer work in ad hoc local processes, with no TMS service endpoint.

**Pros:** No new listener. Minimal implementation work.

**Cons:** No IRC signal target; no centralized service for automated maintenance.

**Decision:** Rejected.

### 4.2 Option B: In-process plugin on the main server (Chosen)

Register Table Maintenance as a Jersey 2 `Feature` through
`gravitino.server.rest.extensionPackages` (same pattern as IdP) so it runs inside the main server
process. The commit path does **not** use HTTP. After each Iceberg commit, the **colocated** IRC hook
enqueues **compaction-only** evaluate → submit on a bounded executor (§5.4). Timed maintenance is
triggered by an external CronJob calling `scheduled-run` (§4.6).

**Pros:** No extra process or port; no remote event hop on the commit path; reuses Policy + Jobs on
the same server; matches plugin packaging; keeps Gravitino replicas peer-equal (no maintenance leader).

**Decision:** **Chosen**.

### 4.3 Option C: Independent long-running Table Maintenance Service process

**Pros:** Full JVM isolation.

**Cons:** Extra deployable; duplicates server lifecycle patterns already covered by the main
webserver plugin.

**Decision:** Rejected. Prefer the in-process plugin on the main server.

### 4.4 Option E: Dedicated aux Jetty listener (:9301)

Implement `GravitinoAuxiliaryService` with `shortName() = "maintenance"`, expose a dedicated Jetty
listener (default **9301**), and keep TMS off the main 8090 JAX-RS app.

**Pros:** Classpath isolation similar to `iceberg-rest` / `lance-rest`.

**Cons:** Extra port and aux enablement; diverges from plugins that already extend
**8090** via `extensionPackages`.

**Decision:** Rejected. Prefer Option B.

### 4.5 Option F: Built-in scheduler with cluster lease (Rejected)

Run a `MaintenanceScheduler` inside every Gravitino replica. Each tick attempts to acquire a
**cluster-wide scheduler lease** in the entity DB; the winner scans all tables, ranks candidates,
and submits jobs until `maxConcurrentJobs` is reached.

**Pros:**

- Self-contained: no K8s (or other orchestrator) dependency.
- Missed commit signals recover on the next tick.
- One configuration surface (`gravitino.maintenance.scheduler.*`).

**Cons:**

- **Leader/follower scheduling inside a peer-replica service.** Only one node runs each tick even
  though every replica serves IRC traffic. That introduces a maintenance **master** role that
  Gravitino's otherwise stateless, load-balanced deployment model does not have elsewhere.
- **Duplicate work without the lease:** if every replica scanned independently, catalog reads,
  statistics refresh, and evaluate calls would multiply by replica count; `maxConcurrentJobs` would
  be ambiguous (per node vs cluster-wide).
- **Reinvents an external orchestrator.** A 5-minute in-process tick is functionally similar to a
  CronJob calling a wake-up API; the orchestration logic (candidate selection, ranking, claims,
  `minIntervalMs`) still lives in TMS either way.
- **Industry pattern:** products that ship a maintenance service often expose a **scheduler or
  trigger API** and let the platform operator choose the clock (Floe: built-in scheduler **or** external
  `POST …/maintenance/trigger`; OpenHouse: K8s CronJob data services). Few require every application
  replica to elect a maintenance leader.

**Decision:** **Rejected.** Use an external clock (§4.6) for timed maintenance. Gravitino nodes stay
peers; coordination is limited to **per-(table, policy) claims** on submit (§6).

### 4.6 Option G: K8s CronJob external clock (Chosen for timed maintenance)

A single CronJob (or enterprise scheduler — Airflow, Argo, etc.) calls TMS on a cadence:

```text
POST /api/metalakes/{metalake}/maintenance/scheduled-run
```

TMS performs candidate selection, statistics refresh, worst-first ranking, hot pipeline
(`manifest → expire`), orphan track, claims, and job submission. The CronJob only answers **when**
to start a maintenance round.

**Pros:**

- **Explicit singleton invoker** — `concurrencyPolicy: Forbid` on the CronJob replaces an internal
  scheduler lease without making Gravitino replicas asymmetric.
- **Familiar ops model** — schedule, timezone, and maintenance window live in GitOps / Helm next to
  other batch jobs.
- **Same TMS logic** — candidate rules, `minIntervalMs`, Recommender, and claims stay in one place;
  only the wake-up mechanism moves out of process.

**Cons:**

- Requires a cluster (or external scheduler). Non-K8s deployments must run an equivalent periodic
  `curl` / ops script (documented in §5.3).
- Cron granularity is coarser than a 5-minute in-process tick; per-table `minIntervalMs` still gates
  actual work.

**Decision:** **Chosen** for manifest rewrite, snapshot expiry, and orphan cleanup.

### 4.7 Industry survey: scheduled maintenance clocks

Most lakehouse maintenance products treat **snapshot expiry, manifest rewrite, and orphan cleanup** as
**time-driven** (scheduler, cron, or platform optimizer interval), not as post-commit hooks.

| Product | Clock mechanism | Typical cadence | Operations on schedule |
| ------- | ---------------- | --------------- | ---------------------- |
| [AWS Glue table optimizers](https://docs.aws.amazon.com/glue/latest/dg/table-optimizers.html) | Managed optimizer runs per table | Default **24h** (`runRateInHours`) | Compaction, snapshot **retention**, **orphan file deletion** — each optimizer type has its own interval |
| [Floe](https://github.com/nssalian/floe) | Per-policy `cronExpression` / `interval` **or** `POST …/maintenance/trigger` | Per-op schedules (e.g. compact every 4h, expire daily) | Compaction, expire snapshots, orphan cleanup, rewrite manifests — **independent schedules per operation** |
| [Apache Amoro](https://amoro.apache.org/docs/latest/configurations/) | AMS `PeriodicTableScheduler` executors | Snapshot expire default **1h**; orphan clean **7d**; dangling deletes **24h** | Snapshot expiration, orphan files, dangling delete files — **separate periodic executors**, not commit hooks |
| [OpenHouse](https://github.com/linkedin/openhouse/blob/main/ARCHITECTURE.md) | K8s **CronJob** data services | Operator-defined | Table maintenance jobs triggered by platform cron |
| [Databricks OPTIMIZE / VACUUM guidance](https://docs.databricks.com/aws/en/tables/operations/optimize) | Scheduled jobs or predictive optimization | **Daily** recommended starting point for `OPTIMIZE`; predictive layer for UC tables | File layout (`OPTIMIZE`) and vacuum are **scheduled / platform-driven**, separate from write path |

**Takeaway for TMS:** timed maintenance belongs on an **external or platform scheduler**. TMS exposes
`scheduled-run`; operators configure **when** to call it (K8s CronJob). **Per-table eligibility**
(`minIntervalMs`, `olderThan`, Recommender thresholds) remains in TMS policy content — the CronJob
does not encode per-table logic.

### 4.8 Industry survey: commit / write-path triggers

**Compaction** (rewrite data files, small-file consolidation) is the operation most often tied to
**writes or commits**. Manifest rewrite, snapshot expiry, and orphan cleanup are usually **not**
run on every commit.

| Product | Write / commit trigger | What runs on the write path | What stays scheduled |
| ------- | ---------------------- | --------------------------- | -------------------- |
| [Databricks Iceberg auto compaction](https://docs.databricks.com/aws/en/tables/tune-file-size) | After a **successful write** when small-file thresholds are met | **Compaction only** (`OPTIMIZE` with `operationParameters.auto = true`) | Larger `OPTIMIZE`, manifest work, expire, vacuum — **scheduled or predictive** |
| [Apache Amoro self-optimizing](https://amoro.apache.org/docs/latest/configurations/) | **Minor** optimization when fragment + equality-delete file count **or** time interval threshold is met (planning is continuous; commit produces new snapshots) | Compaction tiers (minor / major / full) | Snapshot expiration (`SnapshotsExpiringExecutor`), orphan clean (`OrphanFilesCleaningExecutor`), dangling deletes — **separate periodic executors** |
| [Amoro AIP-3](https://cwiki.apache.org/confluence/display/AMORO/AIP-3%3A+Event-Triggered+Optimization+of+Iceberg+Tables+in+Amoro) | Event-triggered **optimization** (file-count / metric signals) | Compaction-style self-optimizing | Expire / orphan remain lifecycle tasks outside the event path |
| [Floe `triggerConditions`](https://github.com/nssalian/floe/blob/main/docs/policies.md) | Optional **health-based** triggers (`smallFilePercentageAbove`, `snapshotCountAbove`, …) with `minIntervalMinutes` | Any enabled op **when conditions fire** (often compaction-first in examples) | Default path is still **cron per operation**; conditions augment, not replace, schedules |
| [AWS Glue compaction optimizer](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-table-optimizers.html) | Threshold-based (`minInputFiles`, `deleteFileThreshold`) inside **scheduled** optimizer runs | Compaction during optimizer run, not inline on catalog commit | Retention and orphan optimizers are **separate scheduled types** |

**Why TMS limits the commit path to compaction:**

| Concern | Compaction on commit | Manifest / expire / orphan on commit |
| ------- | -------------------- | ------------------------------------- |
| Commit latency | Acceptable when bounded (executor + claim + async job submit) | Unacceptable — listing, expire, and orphan scans are heavy |
| Inactive tables | Still benefit when they resume writes | **Never maintained** if commits stop |
| Failed writes | N/A | **Orphan files** appear **without** a successful commit |
| Stale metrics | Recommender can use pre-commit statistics for compaction debt | Expire / orphan need **fresh** table-wide metadata; scheduler pass refreshes stats first |
| Industry alignment | Databricks auto-compact; Amoro minor optimizing | Glue, Amoro, Floe schedule expire / orphan / manifest separately |

**TMS decision:** IRC commit path runs **`system_iceberg_compaction` only** (§5.4). Manifest rewrite,
snapshot expiry, and orphan cleanup run only on **`scheduled-run`** (§5.5–§5.6). This matches the
dominant industry split while keeping a fast write path.

---

## 5. Proposal

### 5.1 Architecture

```text
Spark / Flink / Trino
        │  Iceberg REST commit
        v
Gravitino Iceberg REST (IRC, typically :9001)
        │  commit succeeded (same JVM as main server)
        │
        └─ IRC post-commit hook (§5.4)
                │
                └─ bounded executor → compaction evaluate → submit
                   (per-policy claim; minIntervalMs; Recommender)

K8s CronJob (or external scheduler)
        │  POST …/maintenance/scheduled-run  (§5.3)
        v
ScheduledMaintenanceService
        │
        ├─ Track A — hot pipeline (§5.5):
        │     candidates: manifest / expire due (minIntervalMs)
        │     refresh statistics (Updater)
        │     rank tables worst-first
        │     per table: manifests → expire (Recommender → SQL)
        │
        ├─ Track B — orphan cleanup (§5.6):
        │     candidates: orphan policy + per-table minIntervalMs elapsed
        │     rank oldest-successful-cleanup first
        │     remove orphan files SQL (olderThan enforced server-side)
        │
        ├─ shared: maintenance window (optional) + maxConcurrentJobs
        │     (tables not reached carry over to next Cron invocation)
        v
Gravitino Job framework + job_run_meta (every run — §6.5)
```

#### 5.1.1 In-process commit callback

Commit signals are delivered **only in-process**. After a successful Iceberg commit, the **IRC
post-commit hook** enqueues compaction work on the bounded executor. It does **not** resolve
non-compaction policies or submit manifest / expire / orphan jobs on the commit thread.

| Requirement | Detail |
| ----------- | ------ |
| Deployment  | IRC (`iceberg-rest`) and the main Gravitino server share **one JVM**. |
| Transport   | In-process callback / SPI only — **no** HTTP, **no** Kafka. |
| Payload     | Normalized `table_identifier` (`catalog.schema.table`). |
| Commit scope | **`system_iceberg_compaction` only** (§5.4). |
| Commit cost | Enqueue only on IRC thread; evaluate + submit on bounded executor. |

---

### 5.2 Policy model

#### 5.2.1 Four built-in policy types

Each activity is a **separate** built-in policy type with its own `content`, `minIntervalMs`, and
attachment grain:

| Maintenance type | Illustrative policy type | Built-in job template | Typical attachment | Trigger path |
| ---------------- | ------------------------ | --------------------- | ------------------ | ------------ |
| Compaction | `system_iceberg_compaction` | `builtin-iceberg-compaction` | Table | **Commit** (§5.4) + optional manual / ops |
| Manifest rewrite | `system_iceberg_rewrite_manifests` | `builtin-iceberg-rewrite-manifests` | Table or schema | **Scheduled** (§5.5) |
| Snapshot expiry | `system_iceberg_snapshot_expiration` | `builtin-iceberg-expire-snapshots` | Catalog | **Scheduled** (§5.5) |
| Orphan cleanup | `system_iceberg_orphan_file_removal` | `builtin-iceberg-remove-orphan-files` | Catalog | **Scheduled** (§5.6) |

See [iceberg-compaction-policy](../docs/iceberg-compaction-policy.md),
[iceberg-expire-snapshots-maintenance-job](./iceberg-expire-snapshots-maintenance-job.md),
[iceberg-rewrite-manifests-job](./iceberg-rewrite-manifests-job.md), and
[iceberg-remove-orphan-files-maintenance-job](./iceberg-remove-orphan-files-maintenance-job.md).

#### 5.2.2 Maintenance profile (one-step setup)

A **profile** such as `standard` creates four policy instances and attaches them with sensible
defaults. It is **not** a fifth `policyType`.

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "profile": "standard",
    "target": "catalog.rest_catalog",
    "overrides": {
      "compaction": { "enabled": true },
      "snapshot-expiry": { "olderThanDays": 7 }
    }
  }' \
  http://localhost:8090/api/metalakes/test/maintenance/profiles/apply
```

#### 5.2.3 Precedence (nearest attachment wins)

```text
effective_policy(table, maintenance_type) =
  nearest Active attachment of that type along:
    table → schema → catalog → metalake
```

Only **one** policy per maintenance type is evaluated for a table.

---

### 5.3 Scheduled path: external clock + `scheduled-run` API

Timed maintenance is **not** driven by an in-process ticker. An external orchestrator invokes TMS.

#### 5.3.1 `POST /api/metalakes/{metalake}/maintenance/scheduled-run`

**Request:**

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `tracks` | string[] | no | Subset of `hot-pipeline`, `orphan`. Default: both. |
| `maxTables` | integer | no | Cap tables processed this round (worst-first / oldest-first). |
| `dryRun` | boolean | no | Return planned work without submit. Default `false`. |

**Response:** `202 Accepted`

```json
{
  "roundId": "…",
  "tracks": ["hot-pipeline", "orphan"],
  "tablesConsidered": 120,
  "jobsSubmitted": 8,
  "deferred": 112
}
```

**Behavior:**

1. Authenticate caller (service account or operator token with maintenance invoke privilege).
2. If `maintenanceWindow` is configured (§8.1) and current UTC time is outside the window, return
   `200` with `jobsSubmitted: 0` and `deferredReason: "outside_maintenance_window"`.
3. Enumerate tables with attached policies for the requested tracks.
4. Refresh statistics (`Updater`) before evaluate on the hot pipeline.
5. Rank candidates, claim per `(table, policy)`, apply `minIntervalMs`, Recommender, submit until
   `maxConcurrentJobs` slots are used.
6. Write `job_run_meta` for every submission (§6.5).

**Idempotency:** safe to call on overlapping schedules when Cron `concurrencyPolicy: Forbid` is set.
Per-policy claims prevent duplicate submits across concurrent Gravitino nodes.

#### 5.3.2 Example K8s CronJob

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: gravitino-maintenance-scheduled
spec:
  schedule: "0 22 * * *"          # nightly; cluster timezone
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: trigger
            image: curlimages/curl:8.5.0
            command:
            - sh
            - -c
            - |
              curl -sf -X POST \
                -H "Authorization: Bearer ${MAINTENANCE_TOKEN}" \
                -H "Content-Type: application/json" \
                -d '{"tracks":["hot-pipeline","orphan"]}' \
                "http://gravitino:8090/api/metalakes/${METALAKE}/maintenance/scheduled-run"
          restartPolicy: OnFailure
```

**Non-K8s:** run the same `curl` from cron, Airflow, or CI on the desired cadence.

**Why one CronJob is enough:** orphan eligibility uses **per-table** `minIntervalMs` (default 7 days),
not a separate weekly Cron. A nightly `scheduled-run` scans all tables; only tables past their
interval are submitted.

---

### 5.4 Commit path (compaction only + bounded executor)

```text
IRC commit succeeded (same JVM)
  └─ IRC post-commit hook (§5.1.1)
        │
        └─ enqueue on bounded executor (required):
              resolve effective system_iceberg_compaction policy (§5.2.3)
              upsert table_maintenance_state row if missing
              atomic per-policy claim (§6.1)
              interval gate (minIntervalMs via last_job_id)
              Recommender → submit builtin-iceberg-compaction
              release claim; record job_run_meta (§6.5)
```

**Design requirements:**

1. IRC hook returns quickly — **no** `Recommender`, **no** claim, **no** submit on the commit thread.
2. Bounded executor (fixed queue + worker threads) runs compaction evaluate → submit.
3. **Compaction only** — manifest, expire, and orphan policies are **ignored** on this path (§4.8).
4. **Best effort** (§10): a lost enqueue only delays compaction until the next commit or a manual run.

---

### 5.5 Hot pipeline (scheduled — Track A)

When `scheduled-run` selects a table for **Track A**, enabled operations run in this **fixed order**:

```text
1. manifests         Recommender → rewrite manifests SQL
2. expire            Recommender → expire snapshots SQL
```

Compaction is **not** in this pipeline; it is commit-driven (§5.4).

**Why this order:**

- Rewrite manifests after the file set has stabilized (post-compaction from the write path).
- Expire snapshots after manifest rewrite so metadata reflects the current file set.

Each type has its own **`minIntervalMs`** (§8.3). Track A ranks tables **worst-first** using refreshed
statistics.

---

### 5.6 Orphan cleanup track (scheduled — Track B)

Orphan cleanup runs on a **separate track**, not as step 3 of the hot pipeline.

| Aspect | Track A (hot pipeline) | Track B (orphan) |
| ------ | ---------------------- | ---------------- |
| Operations | manifest rewrite, snapshot expire | `remove_orphan_files` only |
| Candidate signal | metrics / time due (`minIntervalMs`) | per-table `minIntervalMs` since last **successful** orphan job |
| Queue order | worst-first (health score) | **oldest cleanup first** |
| Shared limits | `maxConcurrentJobs` (§8.1) | same |

**Per-table eligibility:** a table becomes eligible again only after **`minIntervalMs`** since its
last successful orphan run (default seven days — §8.3). Listings spread across nights instead of one
global orphan night.

**`olderThan` server-side enforcement:** TMS enforces a **minimum floor** on every evaluate and
policy write (§8.1).

---

### 5.7 Internal structure

| Part | Responsibility |
| ---- | -------------- |
| `TableMaintenanceRESTFeature` | Jersey 2 `Feature`; commit callback, `scheduled-run`, ops resources (§7). |
| `IcebergCommitEventHandler` | Enqueues compaction pipeline on bounded executor (§5.4). |
| `ScheduledMaintenanceService` | `scheduled-run` handler: candidates → stats → evaluate → submit (§5.3). |
| `MaintenanceEvaluateSubmitPipeline` | Claim → interval gate → Recommender → submit for one `(table, policy)`. |
| `TableMaintenanceStateStore` | `table_maintenance_state` upsert / claim / rename (§6). |
| `IcebergTableLifecycleHook` | IRC rename/drop: rewrite or purge state rows (§6.4). |
| Existing optimizer classes | `Updater`, `Recommender`, providers, `JobSubmitter`. |

There is **no** `MaintenanceScheduler` and **no** `TableMaintenanceEventStore`.

---

### 5.8 User process

1. Operator enables the TMS REST plugin (`extensionPackages`) and `iceberg-rest` **in the same JVM**,
   and turns on in-process commit callbacks (§5.1.1 / §8.2).
2. Operator applies a **`standard` profile** or creates four policies and attaches them at the
   intended grains via metalake Policy APIs.
3. Operator deploys a **K8s CronJob** (or equivalent) that calls `scheduled-run` nightly (§5.3.2).
4. Engines write through Gravitino Iceberg REST. On commit success, IRC enqueues **compaction**
   evaluate → submit (§5.4).
5. On each Cron invocation, TMS runs manifest / expire / orphan tracks within limits (§5.3).
6. Operators observe runs in the Gravitino **Jobs** UI / APIs. Manual runs remain available through
   ops APIs (§7) and `runJob`.

---

## 6. Multi-node coordination (shared claim)

On **multiple** Gravitino / TMS nodes, commit compaction and `scheduled-run` handling may run on
**any** replica that receives the IRC commit or the HTTP call. Without coordination, two nodes could
both evaluate and submit the same policy's job for the same table.

**Approach:** shared table `table_maintenance_state` in the Gravitino entity DB. Table identity uses
a **normalized string `table_identifier`** (`catalog.schema.table`), **not** `table_meta.table_id`.

| Table | Role |
| ----- | ---- |
| `table_maintenance_state` | Multi-node **claim**, in-flight `job_id`, finished `last_job_id` per policy (§6.1–§6.2) |

**No cluster-wide scheduler lease.** The external CronJob (with `concurrencyPolicy: Forbid`) is the
singleton **wake-up** invoker. Gravitino replicas remain peers.

`table_maintenance_state` primary key is `(metalake_id, table_identifier, policy_id)` — **one row per
effective maintenance policy instance** for a table. Claim is **per policy row**.

### 6.1 Claim flow

```text
Node A / Node B — both evaluate the same (table, policy)
        │
        ├─ both attempt per-policy claim:
        │     UPDATE … SET state=RUNNING, claim_lease_expires_at=now+leaseMs
        │     WHERE metalake_id=? AND table_identifier=? AND policy_id=? AND state=IDLE
        │     ├─ Node A: rows_affected = 1 → runs evaluate → submit → release to IDLE
        │     └─ Node B: 0 rows → skip (another node holds claim)
```

Gate checks alone are insufficient (read race). **Claim is the write lock** for that policy row.

### 6.2 State table (shared store)

**Table name:** `table_maintenance_state`

| Column | Type | Notes |
| ------ | ---- | ----- |
| `metalake_id` | `BIGINT UNSIGNED NOT NULL` | Metalake that owns the maintenance policy |
| `table_identifier` | `VARCHAR(512) NOT NULL` | Normalized `catalog.schema.table` |
| `policy_id` | `BIGINT UNSIGNED NOT NULL` | Real `policy_meta.policy_id` |
| `state` | `VARCHAR(16) NOT NULL` | `IDLE` / `RUNNING` (per policy row) |
| `updated_at` | `BIGINT NOT NULL` | Epoch millis; claim / reclaim |
| `job_id` | `BIGINT UNSIGNED NULL` | In-flight job (`job_run_meta.job_run_id`) |
| `last_job_id` | `BIGINT UNSIGNED NULL` | Last finished job; `job_finished_at` drives min-interval |
| `last_measured_snapshot_id` | `BIGINT NULL` | Snapshot id at last successful evaluate (§10.3) |
| `claim_lease_expires_at` | `BIGINT NULL` | Epoch millis; reclaim `RUNNING` after expiry (§10.3) |
| `submission_idempotency_key` | `VARCHAR(64) NULL` | Written before job submit; dedupe boundary (§10.3) |

**Primary key:** (`metalake_id`, `table_identifier`, `policy_id`).

Illustrative MySQL DDL:

```sql
CREATE TABLE IF NOT EXISTS `table_maintenance_state` (
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `table_identifier` VARCHAR(512) NOT NULL COMMENT 'normalized catalog.schema.table',
    `policy_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'policy id from policy_meta',
    `state` VARCHAR(16) NOT NULL COMMENT 'IDLE|RUNNING',
    `updated_at` BIGINT(20) NOT NULL COMMENT 'last state upsert time in epoch millis',
    `job_id` BIGINT(20) UNSIGNED NULL COMMENT 'in-flight job_run_id',
    `last_job_id` BIGINT(20) UNSIGNED NULL COMMENT 'last finished job_run_id',
    `last_measured_snapshot_id` BIGINT(20) NULL COMMENT 'snapshot id at last evaluate',
    `claim_lease_expires_at` BIGINT(20) NULL COMMENT 'claim lease expiry, epoch millis',
    `submission_idempotency_key` VARCHAR(64) NULL COMMENT 'idempotency key before job submit',
    PRIMARY KEY (`metalake_id`, `table_identifier`, `policy_id`),
    KEY `idx_state_updated` (`state`, `updated_at`),
    KEY `idx_table_identifier` (`table_identifier`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT 'TMS per-policy claim and job state';
```

### 6.3 `table_maintenance_event` — not in this design

Earlier drafts INSERTed one `table_maintenance_event` row per commit. That table is **not** shipped.

| Need | Mechanism |
| ---- | --------- |
| Minimum time between runs | Per-type `minIntervalMs` + `last_job_id` → `job_run_meta.job_finished_at` (§8.3) |
| Time-driven maintenance without commits | External Cron → `scheduled-run` (§5.3) |
| Compaction after write | Commit path compaction (§5.4) |
| Recovery | Table state + `last_measured_snapshot_id` (§10.2) — not a per-commit log |

A per-commit event log caused unbounded growth and high entity-store write volume for mostly
"not yet" decisions.

### 6.4 Table rename / drop lifecycle (required with string keys)

**Hook:** after a successful Iceberg table rename (or drop), IRC invokes an in-process
`IcebergTableLifecycleHook` registered by the TMS plugin (§5.1.1).

#### Rename

`UPDATE table_maintenance_state SET table_identifier = new WHERE … table_identifier = old`.

#### Drop

`DELETE` all `table_maintenance_state` rows for `(metalake_id, table_identifier)`.

### 6.5 Job run history (`job_run_meta`)

Every maintenance submission — Spark job or in-process execution — creates a `job_run_meta` row.
On job finish, TMS updates `last_job_id`, clears in-flight `job_id`, and records
`last_measured_snapshot_id` when evaluate completes (§10.3).

---

## 7. Optimizer CLI replacement APIs

The commit path and `scheduled-run` do **not** call these routes. They replace the
`gravitino-optimizer` CLI for operators and scripts on the main webserver (**8090**).

| CLI `--type` | Method | Path |
| ------------ | ------ | ---- |
| `submit-strategy-jobs` | `POST` | `/api/maintenance/table/ops/strategy-jobs` |
| `submit-update-stats-job` | `POST` | `/api/maintenance/table/ops/update-stats-jobs` |
| `update-statistics` | `POST` | `/api/maintenance/table/ops/statistics` |
| `append-metrics` | `POST` | `/api/maintenance/table/ops/metrics` |
| `monitor-metrics` | `POST` | `/api/maintenance/table/ops/metrics/monitor` |
| `list-table-metrics` | `GET` | `/api/maintenance/table/ops/metrics/tables` |
| `list-job-metrics` | `GET` | `/api/maintenance/table/ops/metrics/jobs` |

---

## 8. Configuration

### 8.1 Enablement keys (`gravitino.conf`)

| Key | Default | Description |
| --- | ------- | ----------- |
| `gravitino.server.rest.extensionPackages` | none | TMS Feature package. |
| `gravitino.auxService.names` | none | Must include `iceberg-rest` when using IRC. |
| `gravitino.maintenance.claimTimeoutMs` | `300000` | Reclaim stale `RUNNING` claim. |
| `gravitino.maintenance.claimLeaseMs` | `300000` | Claim lease; reclaim `RUNNING` after expiry (§10.3). |
| `gravitino.maintenance.executor.threads` | `4` | Bounded executor worker threads (§5.4). |
| `gravitino.maintenance.executor.queueSize` | `10000` | Bounded executor queue depth. |
| `gravitino.maintenance.scheduled.maxConcurrentJobs` | `10` | Max concurrent maintenance Spark jobs per round. |
| `gravitino.maintenance.scheduled.maintenanceWindow` | none | Optional UTC window for `scheduled-run` submissions. |
| `gravitino.maintenance.orphan.olderThanMinMs` | `259200000` | Server-side minimum `olderThan` (3 days) for orphan cleanup (§5.6). |

There is **no** `gravitino.maintenance.scheduler.enabled` or `scheduler.intervalMs`. Timing for
scheduled maintenance is configured on the **K8s CronJob** (`schedule` field) or equivalent external
cron.

```properties
gravitino.server.rest.extensionPackages = org.apache.gravitino.maintenance.web.rest.feature
gravitino.auxService.names = iceberg-rest
gravitino.iceberg-rest.tableMaintenance.inProcess = true
gravitino.maintenance.claimTimeoutMs = 300000
gravitino.maintenance.executor.threads = 4
gravitino.maintenance.scheduled.maxConcurrentJobs = 10
```

### 8.2 Iceberg REST → TMS in-process event keys

| Key (illustrative) | Default | Description |
| ------------------ | ------- | ----------- |
| `gravitino.iceberg-rest.tableMaintenance.inProcess` | `false` | IRC invokes compaction callback after commit. |

### 8.3 Task types and minimum interval (per policy type)

Each maintenance policy type has its own `minIntervalMs`, compared per `(table, policy_id)` via
`last_job_id` → `job_run_meta.job_finished_at`. Null `last_job_id` → gate passes.

| Task type | Policy type | Code default `minIntervalMs` |
| --------- | ----------- | ---------------------------- |
| `compaction` | `system_iceberg_compaction` | `3600000` (1 hour) |
| `snapshot-expiry` | `system_iceberg_snapshot_expiration` | `86400000` (1 day) |
| `manifest-rewrite` | `system_iceberg_rewrite_manifests` | `86400000` (1 day) |
| `orphan-cleanup` | `system_iceberg_orphan_file_removal` | `604800000` (7 days) |

**Resolution order:** table property override → global `gravitino.conf` key → code default.

---

## 9. Work Plan and Checklist

### 9.1 Suggested Work Plan

| Phase | Work item | Notes |
| ----- | --------- | ----- |
| 1 | In-process plugin + bounded executor | Feature, compaction callback (§5.4). |
| 2 | State table + claim + precedence | `table_maintenance_state` (§6); nearest-wins (§5.2.3). |
| 3 | `scheduled-run` API + scheduled service | Track A / B, worst-first, window (§5.3, §5.5–§5.6). |
| 4 | Commit-path compaction pipeline | Claim, interval, Recommender → submit (§5.4). |
| 5 | Maintenance profile API | `standard` one-step setup (§5.2.2). |
| 6 | Ops APIs | §7. |
| 7 | Hardening + deployment docs | K8s CronJob example (§5.3.2), metrics, fault tolerance (§10). |

#### Phase 3 checklist

- [ ] `POST …/maintenance/scheduled-run` resource (§5.3.1).
- [ ] `ScheduledMaintenanceService`: candidate scan, stats refresh, ranking.
- [ ] Hot pipeline order: `manifests → expire` (§5.5).
- [ ] Orphan track: per-table `minIntervalMs`, oldest-cleanup-first (§5.6).
- [ ] `maxConcurrentJobs`; tables not reached defer to next Cron invocation.
- [ ] Document K8s CronJob manifest and non-K8s cron equivalent.

#### Phase 4 checklist

- [ ] IRC hook enqueues **compaction only** on bounded executor (§5.4).
- [ ] Unit tests: commit thread does not block on evaluate.
- [ ] Commit path ignores manifest / expire / orphan policies.

### 9.2 Review Checklist

| Area | Checklist |
| ---- | --------- |
| Deployment | `extensionPackages`; IRC colocated in same JVM. Ops + `scheduled-run` on **8090**. |
| Policy | Four built-in types; precedence nearest-wins (§5.2); profile is convenience only. |
| Trigger | Commit → **compaction only** (§5.4); timed → **K8s CronJob** → `scheduled-run` (§5.3). |
| No HA scheduler | **No** in-process scheduler lease (§4.5); peer replicas + per-policy claim (§6). |
| Executor | Bounded; evaluation **off** commit thread (§5.4). |
| Durability | `table_maintenance_state` only; **no** per-commit event log (§6.3). |
| Orchestration | Scheduled hot pipeline `manifests → expire` (§5.5); orphan separate track (§5.6). |
| Industry | §4.7–§4.8 document scheduled vs commit triggers with external references. |
| Fault tolerance | Scheduled: at-least-once latest-state; commit: best effort (§10). |

---

## 10. Fault tolerance and delivery guarantees

### 10.1 Delivery models

| Model | TMS target |
| ----- | ---------- |
| Best effort | **Commit compaction path** (§5.4) |
| At-least-once latest-state | **`scheduled-run` path** — recovery from table state, not event rows |
| Exactly-once job effect | **Not required** — claims + idempotency key bound duplicates |

**Commit path:** a lost enqueue delays compaction until the next commit or manual run. Manifest,
expire, and orphan are unaffected because they run on `scheduled-run`.

**Scheduled path:** coalescing is acceptable — maintenance acts on the table's **current** state.

### 10.2 Recovery is driven by table state, not event rows

Recovery uses:

- **Current snapshot id** vs `last_measured_snapshot_id`
- Per-policy **`last_job_id`** / in-flight **`job_id`**
- Per-type **`minIntervalMs`**

### 10.3 Recovery at failure boundaries

| Boundary | Behavior |
| -------- | -------- |
| Commit durable before executor runs compaction | Signal lost → compaction waits for next commit |
| Node fails holding a claim | `claim_lease_expires_at` reclaims `RUNNING`; next pass retries |
| Cron fires while prior round still running | `concurrencyPolicy: Forbid` on CronJob; or TMS returns deferred |
| Job accepted before `job_id` recorded | `submission_idempotency_key` written before submit |

---

## 11. References

1. [Gravitino Iceberg REST service](../docs/iceberg-rest-service.md)
2. [Manage policies in Gravitino](../docs/manage-policies-in-gravitino.md)
3. [Iceberg compaction policy](../docs/iceberg-compaction-policy.md)
4. [Iceberg expire snapshots maintenance job](./iceberg-expire-snapshots-maintenance-job.md)
5. [Iceberg rewrite manifests job](./iceberg-rewrite-manifests-job.md)
6. [Iceberg remove orphan files maintenance job](./iceberg-remove-orphan-files-maintenance-job.md)
7. [Table Maintenance optimizer overview](../docs/table-maintenance-service/optimizer.md)
8. [Amoro AIP-3 – Event-Triggered Optimization](https://cwiki.apache.org/confluence/display/AMORO/AIP-3%3A+Event-Triggered+Optimization+of+Iceberg+Tables+in+Amoro)
9. [Amoro configurations (self-optimizing, table-expire)](https://amoro.apache.org/docs/latest/configurations/)
10. [Floe policies (cron, triggerConditions)](https://github.com/nssalian/floe/blob/main/docs/policies.md)
11. [Floe maintenance trigger API](https://github.com/nssalian/floe)
12. [AWS Glue Iceberg table optimizers](https://docs.aws.amazon.com/glue/latest/dg/table-optimizers.html)
13. [Databricks Iceberg auto compaction](https://docs.databricks.com/aws/en/tables/tune-file-size)
14. [Databricks OPTIMIZE frequency guidance](https://docs.databricks.com/aws/en/tables/operations/optimize)
15. [OpenHouse architecture (Jobs Scheduler / CronJob)](https://github.com/linkedin/openhouse/blob/main/ARCHITECTURE.md)
16. [Apache Iceberg REST Catalog OpenAPI](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
