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

1. There is no stable **server-side commit event** after Iceberg commits. Engines would otherwise each need a
   TMS-specific listener, or operators must schedule maintenance externally.
2. Configuration, audit, and service-level metrics are hard to centralize when execution is
   ad hoc and process-local.
3. Each run creates its own runtime and provider instances instead of a shared service lifecycle.
4. When Spark maintenance work is needed, submitted work already returns a `jobId` owned by the
   Gravitino job framework. That job-status boundary should stay.
5. There is no **embedded scheduler** for stale-claim reclaim on multi-node deployments.

This design turns TMS into a **main-server REST plugin** on port **8090** (same pattern as IdP
via `gravitino.server.rest.extensionPackages`) so colocated IRC can **wake** table maintenance after
commits.

---

## 2. Goals

1. **In-process plugin on the main server**: Load Table Maintenance through
   `gravitino.server.rest.extensionPackages` (Jersey 2 `Feature`, same pattern as IdP) so the IRC
   callback is registered in the main JVM. The commit path does **not** use HTTP. Operator calls that
   replace the optimizer CLI are the ops APIs in **§7**.
2. **IRC in-process commit event**: After successful Iceberg commits via IRC, TMS receives a commit
   event through a **main-server-registered in-process callback / SPI** (IRC and main server share one
   JVM; see **§5.1.1**). The handler **upserts** **`tms-table-commit`** with `task_instance = {table_id}`
   (multi-node coalesce on the unique key). It does **not** call `runJob` and does **not** use
   expand threads (§5.1.1).
3. **Three db-scheduler task names on `scheduled_tasks`**:
   - **`tms-policy-expand`** (①): `task_instance = {policy_id}`; `task_data` empty by default.
     Writes **`tms-table-scheduler`** rows; ① is **retained** (§5.4).
   - **`tms-table-scheduler`** (②): from crontab expand. Instance
     `table:{table_id}:{policy_id}` or `batch:{batch_id}:{policy_id}`;
     `task_data = { tableIds, policyIds }`. Short callback: `runJob` → INSERT job row → **DELETE** → return.
   - **`tms-table-commit`** (③): commit wake-up; `task_instance = {table_id}`;
     `task_data = { tableIds, policyIds }`. Policy chosen at **pick** time (§5.1.1);
     same short submit path (may re-upsert after terminal).
4. **Reuse existing optimizer execution core**: Scheduler task handlers invoke the same `Updater` /
   `Recommender` / job-submit paths already present in `maintenance/optimizer`, as **in-process
   methods**, not as a second copy of the logic.
5. **Job framework compatibility**: Spark maintenance work continues to use the Gravitino job
   framework. TMS records each run in `table_maintenance_job` but does not own job status.
   Automate Jobs **Validation** before/after metrics live on the same row (§6.2).
6. **Govern Policy reuse**: Maintenance policies stay on existing `policy_meta` and metalake Policy
   APIs (create / alter / enable / disable / associate). TMS does **not** introduce a parallel policy
   store or `/api/maintenance/table/policies` CRUD.
7. **Multi-node safe execution**: db-scheduler pick + heartbeat is the mutex for each
   `scheduled_tasks` instance — ① / ② / ③ alike (5.4, §6). Concurrent Spark submits for the same
   `(table, policy)` are also gated by an in-flight `table_maintenance_job` row
   (`finished_at IS NULL`) (§6.2).
8. **Per-run maintenance job table**: Gravitino persists one `table_maintenance_job` row per Spark
   `job_run_id` (Validation JSON + `finished_at`). Enqueue / reclaim for **scheduler tasks** live in
   `scheduled_tasks` (three task names above).
9. **Crontab expand vs commit wake-up**: Crontab dues **`tms-policy-expand`**; expand pages and
   writes **`tms-table-scheduler`** after gates (§5.4). Commit does **not** use the expand pool: IRC
   upserts **`tms-table-commit`** `{table_id}`; the **table** pool picks it and resolves policy at
   pick time (5.1.1, §6.2).
10. **Dedicated TMS execution principal**: All automated maintenance (event enqueue after commit
    and timed policy due) submits Jobs as a built-in metalake user **`tms`**, not as the operator
    who created the policy (§5.5).
11. **Policy evaluate triggers on policy**: Automated maintenance uses `onCommit` and/or `crontab`
    in `policy_version_info.content.schedule` (§5.6). One policy row holds both triggers when needed;
    separate policies are for different maintenance **types**, not for separate trigger modes.
12. **Job template parameters on policy**: Non-auth Spark / job-template parameters live in
    maintenance policy content (`jobOptions` / existing `rewriteOptions`). Attach the same policy
    type at catalog / schema / table; **nearest attachment wins** (table > schema > catalog)
    (§5.7).
13. **Auth credentials via SecretManager**: Passwords, tokens, and access keys used to run Spark
    Jobs are **not** stored in `policy_meta` or a TMS-owned credential table. Sensitive values are
    referenced through Gravitino **SecretManager** (URN / provider), aligned with the pluggable
    secret approach for server config. Opt-in: plaintext / missing secrets still work when the
    secret solution is not enabled (§5.8).

---

## 3. Non-Goals

1. **Standalone maintenance daemon**: No separate process or
   `gravitino-iceberg-rest-server.sh`-style entrypoint.
2. **Dedicated auxiliary HTTP listener**: No `GravitinoAuxiliaryService`, no isolated
   `gravitino.maintenance.classpath`, and no dedicated TMS port (for example **9301**). TMS is not
   a dedicated listener like `iceberg-rest` / `lance-rest`.
3. **Provider SPI rewrite**: Does not replace `StatisticsUpdater`, `StatisticsCalculator`,
   `StatisticsProvider`, `StrategyProvider`, `TableMetadataProvider`, or `JobSubmitter` contracts
   used by the event pipeline.
4. **Engine-side commit report path**: Engines that bypass Gravitino Iceberg REST are out of scope
   for event-driven path.
5. **Commit-path HTTP or Kafka**: No `POST …/events/iceberg-commit`, no health resource, and no Kafka
   produce/consume path. Commit handling is **in-process only** (§5.1.1). APIs that replace the
   optimizer CLI are **§7**, and they are not a commit ingress. Remote IRC / cross-JVM delivery is
   out of scope (follow-up if needed).

## 4. Solution Investigations

### 4.1 Option A: Keep process-local execution only

Continue running all optimizer work in ad hoc local processes, with no TMS service endpoint.

**Pros:** No new listener. Minimal implementation work.

**Cons:** No IRC event target; no centralized service for event-driven expand → spark-submit.

**Decision:** Rejected.

### 4.2 Option B: In-process plugin on the main server (Chosen)

Register Table Maintenance as a Jersey 2 `Feature` through
`gravitino.server.rest.extensionPackages` (same pattern as IdP) so it runs inside the main server
process. The commit path does **not** use HTTP. After each Iceberg commit, the **colocated** IRC hook
invokes a main-server-registered **in-process** callback that **bumps** long-lived
**`tms-policy-expand`** tasks. **All** TMS nodes run db-scheduler; one node picks each due instance
(expand ① or table ②/③) under a short lease.

**Pros:** No extra process or port; no remote event hop on the commit path; work is distributed
across replicas via scheduler pick; reuses Policy + Jobs; matches plugin packaging; lease heartbeat
is built into db-scheduler.

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

### 4.5 Three scheduler task names + per-run job records (db-scheduler — Chosen)

TMS needs cluster-safe scheduling for **three** kinds of work on `scheduled_tasks`:

1. **Policy-expand** (①) — crontab: parse a policy into Spark work units (long-lived row per `policy_id`);
   callback returns after enqueue.
2. **Table-scheduler** (②) — crontab/batch unit: short `runJob` callback then **DELETE**.
3. **Table-commit** (③) — commit wake-up per `{table_id}`; resolve at pick; short submit; **DELETE**;
   may re-upsert after Job terminal.

Plus durable **`table_maintenance_job`** for Validation, `minIntervalMs`, and in-flight occupancy.
Three pools: expand (①) / table (②) / commit (③) so commit decide/submit does not steal crontab
threads (§8.4).

#### Industry and in-project alternatives

| --------------------------------- | -------------------------------------------------------------------- | -------------------------------------------- | ---------------------------------------------------- | -------------------------------------------------------- |
| -------------------------------- | -------------------------------------------------------------------- | -------------------------------------------- | ---------------------------------------------------- | -------------------------------------------------------- |
|                                  | **[db-scheduler](https://github.com/kagkarlsson/db-scheduler)**      | [JobRunr](https://www.jobrunr.io/)           | [ShedLock](https://github.com/lukas-krecan/ShedLock) | [Quartz](https://www.quartz-scheduler.org/) JDBC cluster |
| License                          | Apache 2.0                                                           | LGPL v3 (+ commercial)                       | Apache 2.0                                           | Apache 2.0                                               |
| Embed in main server             | Yes                                                                  | Yes                                          | Yes                                                  | Yes                                                      |
| Cluster CAS / single-flight      | Yes (optimistic lock / `SKIP LOCKED` on `scheduled_tasks`)           | Yes                                          | Lock only                                            | Yes (`QRTZ_*` row locks)                                 |
| Heartbeat for short lease        | Yes (`last_heartbeat`)                                               | Yes                                          | N/A                                                  | Yes                                                      |
| Fits per-policy + one-shot Spark | Yes                                                                  | Yes                                          | No (lock only)                                       | Yes (heavier)                                            |
| H2 unit-test path                | Degraded: disable scheduler; run pipeline directly in tests (§8.4) | Better H2 story                              | Yes                                                  | RAMJobStore only in tests                                |
| Extra ops component              | No                                                                   | Optional dashboard server                    | No                                                   | No                                                       |
| Decision                         | **Chosen**                                                           | Rejected — license + overlaps Gravitino Jobs | Rejected — not a scheduler                           | Rejected — ~11 tables, heavy                             |

#### Why db-scheduler + three task names + job table

1. **Apache License 2.0** — safe for an ASF project; JobRunr is LGPL v3.
2. **Embeddable and light** — one `scheduled_tasks` table for ①/②/③ leases; starts/stops with
   `TableMaintenanceRESTFeature`.
3. **Built-in heartbeat** — while a short callback runs, db-scheduler refreshes `last_heartbeat`.
4. **Clear split** — ① = crontab expand → INSERT ②; ②/③ = short submit → **DELETE**; occupancy +
   Validation on `table_maintenance_job`; ③ coalesce on `{table_id}`.
5. **No fat TMS state machine** — no `evaluate_pending` / `IDLE`/`RUNNING` twin of `picked`.

**Decision:** **Chosen** — db-scheduler with three task names + three pools; `table_maintenance_job`
for per-run records and submit gates.

### 4.6 Where automated Job configuration lives

#### 4.6.1 Job template parameters (`jobOptions`)

Non-auth Spark / job-template parameters (for example executor memory, shuffle partitions, Iceberg
`rewriteOptions`) for automated maintenance runs.

|          | Extra table keyed by catalog / schema / table                 | Policy content `jobOptions` (Chosen)                                              |
| -------- | ------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| Pros     | Explicit                                                      | Reuses `policy_version_info.content` + `policy_relation_meta`; nearest attachment already defined |
| Cons     | Parallel attachment + precedence + UI beside policies; drifts | Auth material belongs in §4.6.2, not here                                         |
| Decision | Rejected                                                      | **Chosen** (§5.7)                                                                 |

#### 4.6.2 Authentication credentials

IRC client auth and credential-vending secrets for TMS Jobs (passwords, OAuth client credentials,
keytabs, and similar).

|          | Auth keys in policy `jobOptions`           | Dedicated `tms_credential` table                  | SecretManager + URN (Chosen)                                              |
| -------- | ------------------------------------------ | ------------------------------------------------- | ------------------------------------------------------------------------- |
| Pros     | One map with Spark options                 | Explicit TMS overlay                              | Pluggable providers (file / Vault / KMS); opt-in; shared with server conf |
| Cons     | Policies are widely readable; secrets leak | Second keystore beside SecretManager; no KMS path | Needs bootstrap for TMS principal secrets                                 |
| Decision | Rejected                                   | Rejected                                          | **Chosen** (§5.8)                                                         |

---

## 5. Proposal

### 5.1 Architecture

TMS uses **three** task names on `scheduled_tasks` (db-scheduler) and **two** pools. Writing a row
does **not** mean every node runs it: expand and table pools poll; **N compete, one pick wins**
per due instance.

| Kind                    | `task_name` (illustrative) | When created                                        | Instance key                                                                      | After pick                                                                            |
| ----------------------- | -------------------------- | --------------------------------------------------- | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| ① Policy expand | `tms-policy-expand` | Policy **create / enable** | `{policy_id}` | Page → INSERT ②; **keep** ① |
| ② Table scheduler | `tms-table-scheduler` | Written by ① (crontab) | `table:{table_id}:{policy_id}` or `batch:{batch_id}:{policy_id}` | Short submit → job row → **DELETE** |
| ③ Table commit | `tms-table-commit` | IRC upsert | `{table_id}` | Short submit → job row → **DELETE**; may re-upsert after terminal |

**Lifecycle sketch:**

```text
Create / enable maintenance policy (policy_id = P)
  → INSERT scheduled_tasks ① tms-policy-expand / {policy_id}
     execution_time = next crontab (or far-future if onCommit-only until first commit)

crontab due → ① picked (expand pool)
  → load policy by {policy_id}; task_data empty or expand cursor only
  → page candidates (default **100**); expand gates; INSERT tms-table-scheduler
       instance = table:{table_id}:{policy_id}
       task_data = { tableIds:[table_id], policyIds:[policy_id] }
  → more tables → cursor in ① task_data, due now; else clear cursor / next crontab
  → do NOT delete ① (unless policy disabled / dropped / replaced)

IRC commit (any node that handled the write; not expand/table pool)
  → upsert tms-table-commit instance={table_id} task_data={ tableIds, policyIds } execution_time=now

② tms-table-scheduler picked (table pool)
  → in-flight gate → sample before_metrics → Recommender → runJob as tms
  → INSERT table_maintenance_job (before_metrics, finished_at=NULL) → DELETE ② → return
  → Job terminal listener: sample after_metrics → UPDATE finished_at

③ tms-table-commit picked (table pool; same threads as ②)
  → resolve onCommit policies + gates **at pick time** (§5.6.1) → same short submit path
  → on Job terminal: if more types needed → upsert same {table_id} again
```

```text
Spark / Flink / Trino
        │  Iceberg REST commit
        v
Gravitino IRC (:9001)
        │
        └─ post-commit → IcebergCommitEventHandler (§5.1.1)
                └─ upsert tms-table-commit / {table_id}
                      task_data = { tableIds, policyIds }; execution_time = now
                      (IRC thread; not expand/table/commit pools)

Node A / Node B / Node C  — expand pool + table pool poll (§5.4); N compete, one pick
        │
        ├─ pick ① tms-policy-expand                 (expand.threads=4)
        │     └─ page → INSERT tms-table-scheduler (table:… / batch:…)
        │
        ├─ pick ② tms-table-scheduler               (table.threads=8)
        │     └─ runJob → INSERT job row → DELETE → return
        │
        ├─ pick ③ tms-table-commit                  (commit.threads=4)
        │     └─ resolve/gate → decide → runJob → INSERT → DELETE → return
        │           (Job terminal may upsert same {table_id} again)
        v
                 Gravitino Job framework (async)

        ┌──────────────────────────────────────────────────────────────┐
        │  scheduled_tasks                                              │
        │  • ① tms-policy-expand — {policy_id}; task_data usually empty │
        │  • ② tms-table-scheduler — table:/batch:; {tableIds,policyIds}│
        │  • ③ tms-table-commit — {table_id}; {tableIds, policyIds}     │
        └──────────────────────────────────────────────────────────────┘
```

| Table                                  | Role                                                                                    |
| -------------------------------------- | --------------------------------------------------------------------------------------- |
| `policy_meta` / `policy_relation_meta` / `policy_version_info` | **What** to expand; `schedule` + non-auth `jobOptions` in **content** (§5.6, §5.7) |
| `scheduled_tasks`                      | ① expand + ② table-scheduler + ③ table-commit (5.4, §6.1)                              |
| `table_maintenance_job`                | Per-run Validation JSON + `finished_at`; submit gates (§6.2)                            |
| SecretManager / SecretProvider         | TMS Spark / Iceberg **auth** material via URN (§5.8); not a TMS-owned table             |
| `user_meta`                            | Built-in metalake user `tms` when authorization is enabled (§5.5)                       |
| `job_run_meta`                         | Spark job run **record** (status + `runtime_job_template` snapshot); not default config |

#### 5.1.1 In-process commit event

Commit events are delivered **only in-process**. After a successful Iceberg commit, the **IRC
post-commit hook** invokes a **main-server-registered callback / SPI** (for example on
`GravitinoEnv`). That callback **upserts** **`tms-table-commit`** with `task_instance = {table_id}`
and `task_data = { tableIds, policyIds }` (`policyIds` finalized at pick — §5.4). Concurrent upserts coalesce
on primary key `(task_name, task_instance)`. The IRC thread does **not** call `runJob`, does **not**
use expand/table pools, and does **not** select `policy_id` — that happens when the **commit** pool
picks ③ (§5.4).

| Requirement | Detail                                                                                                                                      |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| Deployment  | IRC (`iceberg-rest`) and the main Gravitino server share **one JVM**.                                                                       |
| Transport   | In-process callback / SPI only — **no** HTTP `POST …/events/iceberg-commit`, **no** Kafka.                                                  |
| Payload     | `table_id` / table identifier (for upsert `task_instance` / `task_data.tableIds`).                                                          |
| Enqueue     | Upsert `(tms-table-commit, {table_id})` on the IRC thread (must stay short).                                                                |
| Execute     | **`commit.threads`** picks ③ (never IRC / expand / table); resolve + gate + short submit; re-upsert after Job terminal if needed (§5.4). |
| Scheduling  | Expand (①) + table (②) + commit (③) pools; same JDBC DataSource as MySQL / PostgreSQL entity store (§8.4).                                  |


Deployment:

1. Package the TMS plugin jars with the main Gravitino server and set
   `gravitino.server.rest.extensionPackages` to include the TMS Feature package (see §8.1).
2. Enable `iceberg-rest` in `gravitino.auxService.names` (same process as the main server).
3. Enable in-process commit events (`tableMaintenance.inProcess` — §8.2).
4. Enable the embedded scheduler (`gravitino.maintenance.scheduler.enabled` — §8.4).
5. Attach Govern maintenance policies (e.g. `system_iceberg_compaction`) to catalogs/schemas/tables
   via existing Policy APIs on the main server (**8090**).

### 5.3 User process

1. Operator enables the TMS REST plugin (`extensionPackages`), `iceberg-rest` **in the same JVM**,
   in-process commit events (§5.1.1 / §8.2), and the embedded scheduler (§8.4). If authorization is
   enabled, TMS bootstraps the metalake user `tms` and grants (§5.5).
2. Operator creates / enables a maintenance policy (including non-auth `jobOptions`) and associates
   it to catalogs / schemas / tables via metalake Policy APIs. On create/enable, TMS **INSERT**s
   **`tms-policy-expand`** ① for that `policy_id` (5.4, §6.1). Example:

   ```bash
   curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
     -H "Content-Type: application/json" \
     -d '{
       "name": "iceberg_compaction_default",
       "comment": "Built-in Iceberg compaction policy",
       "policyType": "system_iceberg_compaction",
       "enabled": true,
       "content": {
         "rewriteOptions": {
           "target-file-size-bytes": "536870912"
         },
         "jobOptions": {
           "spark.executor.memory": "8g",
           "spark.sql.shuffle.partitions": "200"
         }
       }
     }' \
     http://localhost:8090/api/metalakes/test/policies

   curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
     -H "Content-Type: application/json" \
     -d '{"policiesToAdd": ["iceberg_compaction_default"]}' \
     http://localhost:8090/api/metalakes/test/objects/table/rest_catalog.db.t1/policies
   ```

3. Engines write through Gravitino Iceberg REST. On commit success, the **IRC hook** upserts
   **`tms-table-commit`** `{table_id}` (§5.1.1). Multi-node bursts coalesce on that unique row.
4. The **table** pool picks ③, resolves policy, short-submits, **DELETE**s the row; Job terminal
   may upsert the same `{table_id}` again for the next type (§5.6.1). In-flight occupancy on
   `table_maintenance_job` prevents a second Spark submit for the same `(table, policy)`.
5. Operators observe runs in the Gravitino **Jobs** UI / APIs (including Validation from
   `table_maintenance_job`). Automated Job `audit.creator` is **`tms`**. Manual ops APIs in §7 may
   bump ① or enqueue ② under test hooks.

### 5.4 Execute path — expand + table-scheduler + table-commit

```text
Expand Scheduler (expand.threads=4) — registers tms-policy-expand only
Table Scheduler  (table.threads=8)  — registers tms-table-scheduler only
Commit Scheduler (commit.threads=4) — registers tms-table-commit only

① tms-policy-expand due:
        ├─ pick ①
        ├─ PolicyExpandPipeline:
        │     ├─ load policy by task_instance={policy_id}
        │     │     task_data empty, or expand cursor while paging
        │     ├─ page candidates (expand.enqueueBatchSize, default 100)
        │     ├─ expand gates (in-flight / minIntervalMs)
        │     ├─ INSERT tms-table-scheduler for ungated units:
        │     │     task_instance = table:{table_id}:{policy_id}
        │     │       (or batch:{batch_id}:{policy_id} when batching)
        │     │     task_data = { "tableIds":[...], "policyIds":[...] }
        │     │     // no jobOptions in task_data — load at submit
        │     ├─ more pages → cursor in ① task_data; execution_time = now
        │     └─ else clear cursor; next crontab due (if any)
        └─ ① NOT deleted

② tms-table-scheduler due (table pool):
        ├─ pick ②
        ├─ in-flight re-check → sample before_metrics → Recommender → runJob as tms
        ├─ INSERT table_maintenance_job → DELETE ② → return
        └─ (Spark async; terminal listener writes after_metrics + finished_at)

③ tms-table-commit due (commit pool, `commit.threads`):
        ├─ pick ③
        ├─ resolve / gate onCommit at pick (§5.6.1) → **decide whether to submit Spark**
        ├─ short submit as above → DELETE ③
        └─ on Job terminal: upsert ③ again if next type still needed
```

**Pick semantics:** Writing `scheduled_tasks` only sets **when** a row may run. Nodes do **not** all
execute the same instance.

**Why DELETE ②/③ after submit; keep ①:** ① is the durable per-`policy_id` expand lease. ②/③ are
one-shot submit units; Spark lifetime and anti-double-submit live on `table_maintenance_job` + Job
listener / reconcile (§5.4.2).

**Threads:** Commit **enqueue** = IRC thread (upsert only). Commit **decide + execute** =
`commit.threads` (③ only). Crontab submit = `table.threads` (② only). Expand = `expand.threads`
(① only).

#### 5.4.1 Where schedule state lives

| What | Where it lives | Notes |
| ---- | -------------- | ----- |
| Policy expand | `tms-policy-expand` | `{policy_id}`; `task_data` empty or cursor |
| Crontab / batch submit | `tms-table-scheduler` | `table:…` / `batch:…`; `task_data` `{tableIds, policyIds}` |
| Commit wake-up | `tms-table-commit` | `{table_id}`; `task_data` `{tableIds, policyIds}` |
| jobOptions / secrets | `policy_version_info.content` / SecretManager | Loaded at submit — **not** in `task_data` |
| Per-run Validation | `table_maintenance_job` | In-flight + before/after JSON |

#### 5.4.2 db-scheduler tasks

| Task name | `task_instance` | `task_data` | Pool |
| --------- | --------------- | ----------- | ---- |
| `tms-policy-expand` | `{policy_id}` | empty; optional expand `cursor` while paging | expand (4) |
| `tms-table-scheduler` | `table:{table_id}:{policy_id}` or `batch:{batch_id}:{policy_id}` | `{ "tableIds":[…], "policyIds":[…] }` | table (8) |
| `tms-table-commit` | `{table_id}` | `{ "tableIds":[…], "policyIds":[…] }` | commit (4) |

**Uniqueness:** primary key `(task_name, task_instance)`. Same-table commit bursts coalesce on
`(tms-table-commit, {table_id})`. Crontab units coalesce per `table:{table_id}:{policy_id}` (or batch key).

**Prefixes** `table:` / `batch:` avoid ambiguity between numeric `table_id` and `batch_id`.

**Expand paging (crontab):** each ① pick inserts at most `expand.enqueueBatchSize` (default **100**)
`tms-table-scheduler` rows; cursor in ① `task_data` only while paging, then cleared.

**Policy lifecycle → ①:** create/enable → INSERT ①; alter schedule → update ①; disable/drop → DELETE ①
and outstanding ② for that `policy_id`.

**Expand gates** (inside **① `tms-policy-expand`**, per candidate, **before** INSERT `tms-table-scheduler`):

1. `ensureTableImported` (§5.4.3) — skip unit on import failure.
2. If an in-flight row exists for this `(metalake_id, table_id, policy_id)` (`finished_at IS NULL`):
   **skip** (do not INSERT ②).
3. Else if `MAX(finished_at)` for that key is still within the resolved `minIntervalMs` (table prop →
   global conf → code default; §8.3): **skip** (do not INSERT ②).

**Submit path** (② on the table pool, or ③ on the commit pool):

1. `ensureTableImported` (§5.4.3) — must succeed when import is enabled.
2. Re-check in-flight (`finished_at IS NULL`): if found, **do not** `runJob` — **DELETE** this
   scheduled row and return (occupancy already claimed).
3. **Sample `before_metrics` now** (must be before any table mutation). Cannot be reconstructed later.
4. Policy trigger (`Recommender`).
5. Overlay nearest-policy `jobOptions` (§5.7) and SecretManager auth / credential-vending conf
   (§5.8); `runJob` as principal `tms` → `job_run_id`.
6. **Immediately** `INSERT` `table_maintenance_job` with `job_run_id`, keys, `before_metrics`, and
   `finished_at = NULL` (§6.2). This row is the anti-double-submit **occupancy claim**, not a live
   probe of Spark.
7. **DELETE** this ②/③ `scheduled_tasks` row and **return** — do **not** hold the db-scheduler
   thread for Spark.

**Terminal path** (Job listener / status callback when the Job **just** reaches a terminal state — not
a late reconcile sweep):

1. Sample `after_metrics` **immediately** (table state right after this Job). Too late ⇒ meaningless.
2. `UPDATE` `after_metrics` + `finished_at = now`.

**Reconcile** (periodic / on pick; only repairs **occupancy**, never Validation metrics):

`finished_at IS NULL` means “TMS has not closed the slot yet.” It does **not** mean “Spark is
definitely still running.” **Job lifetime and stuck-job handling belong to the Gravitino Jobs
framework** (status pull, executor retention / stale-active expire — see job docs). TMS mirrors a terminal Job status into `finished_at`. If the Job entity is **missing**, **DELETE** the occupancy row instead (no `finished_at`) so the run is treated as never happened.

| `table_maintenance_job` | Job actual status | Action |
| ----------------------- | ----------------- | ------ |
| `finished_at IS NULL` | Still queued / started / cancelling | Keep in-flight; no submit for same `(table, policy)` |
| `finished_at IS NULL` | SUCCEEDED / FAILED / CANCELLED (including Jobs stale-expire → FAILED) | Set `finished_at` only; **do not** backfill `before_metrics` or late `after_metrics` |
| `finished_at IS NULL` | Job entity missing | **DELETE** the occupancy row (treat as job never ran); do **not** set `finished_at`; **no** metrics; allow waiting `(table, policy)` work to proceed |

Do **not** add a parallel TMS wall-clock Spark timeout. Tighter kill behavior belongs in Jobs /
executor configuration (or a future Jobs-level timeout); TMS reconciles after the Job becomes
terminal.

**`minIntervalMs` for crontab is judged on ①** before INSERT of `tms-table-scheduler`. Commit resolves
gates on ③ pick.

#### 5.4.3 Lazy Gravitino metadata import (`table_meta`)

TMS needs stable `schema_id` / `table_id` for `table_maintenance_job` and `scheduled_tasks` keys.
Those ids live in Gravitino **`schema_meta`** / **`table_meta`**, not in the Iceberg catalog backend
alone.

**When:** only as maintenance is about to run (① expand before INSERT ②, or ②/③ submit before
`runJob`). If the target schema or table has **no corresponding id** in Gravitino metadata yet,
`ensureTableImported` fills it in then — not on a separate background scan, and not as a required
step before IRC upserts ③.

This is **Gravitino import**, not Iceberg REST `registerTable`. Reuse core
`TableDispatcher.loadTable(NameIdentifier)` — it loads from the catalog backend and, when missing,
writes **`table_meta`** (importing the parent schema into **`schema_meta`** first when that id is
also missing).

| Piece      | Detail                                                                                                           |
| ---------- | ---------------------------------------------------------------------------------------------------------------- |
| API        | `TableDispatcher.loadTable` on `metalake.catalog.schema.table` (same path IRC `importTableEntity` uses).         |
| **Not**    | Iceberg REST `registerTable`, a new TMS table, or direct `INSERT` into `table_meta`.                             |
| Trigger    | Maintenance path needs a `table_id` (and parent `schema_id`) and Gravitino metadata does not have them yet.      |
| Where      | `ensureTableImported` on ① expand (before INSERT ②) and on ②/③ submit (before `runJob`).                         |
| Expand tip | Catalog / schema attachments: **list tables from the catalog backend**, then `ensureTableImported` per candidate.|
| Idempotent | If `schema_meta` / `table_meta` already have the ids, `loadTable` is a no-op import.                              |
| Failure    | Log and **skip** that unit’s schedule / submit; never write `table_maintenance_job` without a `table_id`.         |

**Owner after import** (optional, separate from `table_meta` row):

1. Resolve owner from the **catalog backend** (for example Iceberg table property `owner`) via a
   backend-aware resolver — not from the `tms` principal.
2. If resolved and the user exists in metalake `user_meta`, call `OwnerDispatcher.setOwner`.
3. If missing or unknown user, leave Gravitino owner unset — **do not** default to `tms`.
4. If the table was already imported, do not overwrite an existing owner.

**Why not `schema_id` on `table_maintenance_job`:** rows are per **table** + policy. `table_id` already
identifies the schema through `table_meta` namespace (`metalake.catalog.schema`). `schema_id` would
only matter for schema-scoped TMS state, which this design does not have.

### 5.5 TMS execution principal (`tms`)

Automated **event** (commit enqueue) and **timed** (policy due) maintenance both submit Spark Jobs
as one built-in metalake user named **`tms`**. Policy authors are not the Job creator. This keeps
audit, privileges, and credentials on a service identity.

#### When the user is created

| Server mode                                                             | Bootstrap                                                                                                                     |
| ----------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `gravitino.authorization.enable = false` (typical `simple` / none auth) | **Do not** insert `user_meta`. There is no RBAC identity. Jobs still set `audit.creator = tms` as a literal principal name.   |
| `gravitino.authorization.enable = true`                                 | TMS plugin start reconciles **all existing** metalakes; each new metalake is reconciled on `CreateMetalakeEvent`. Idempotent. |

The username is fixed (`tms`). Operators must not reuse it as an interactive login.

#### Bootstrap mechanism (authorization enabled)

Use an **`EventListenerPlugin`**, not an IRC hook and **not** a change to the metalake create API or
`MetalakeHookDispatcher`. Follow the same pattern as `BuiltInJobTemplateEventListener` in core.

| Piece            | Detail                                                                                                                                                                                               |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Listener class   | `TmsPrincipalBootstrapListener` implements `EventListenerPlugin`.                                                                                                                                    |
| Registration     | `TableMaintenanceRESTFeature` registers it on `GravitinoEnv.eventListenerManager().addEventListener("tms-principal", …)` when the TMS plugin starts.                                                 |
| Plugin `start()` | List all in-use metalakes and reconcile `tms` user + `tms_maintenance` role + grants on each (covers existing metalakes and clusters where authorization was enabled after TMS was already running). |
| `onPostEvent`    | On `CreateMetalakeEvent`, reconcile the **new** metalake only.                                                                                                                                       |
| `mode()`         | `ASYNC_ISOLATED` — do not block metalake creation (same as built-in job-template listener).                                                                                                          |
| Reconcile        | Idempotent create-or-skip: user `tms` in `user_meta`, role `tms_maintenance`, metalake-level grants in 5.5 below.                                                                                   |

**Out of scope for bootstrap:** IRC commit/drop hooks (§5.1.1, §6.3), `MetalakeHookDispatcher`
(creator membership is already handled in core), and extending `POST /api/metalakes`.

#### Built-in role and privileges (authorization enabled)

TMS creates a built-in role (illustrative name `tms_maintenance`) granted to user `tms` on the
**metalake** so grants apply to all catalogs / schemas / tables under it.

Product requirement → Gravitino privilege mapping:

| Product wording  | Privilege(s) on metalake          | Why                                                                             |
| ---------------- | --------------------------------- | ------------------------------------------------------------------------------- |
| List catalogs    | `USE_CATALOG`                     | There is no separate `LIST_CATALOG`; listing/using catalogs uses `USE_CATALOG`. |
| List schemas     | `USE_SCHEMA`                      | Listing/using schemas.                                                          |
| List tables      | `USE_SCHEMA` + `PROBE_TABLE_LIKE` | Probe/list table-like objects without implying SELECT data.                     |
| Write all tables | `MODIFY_TABLE`                    | Iceberg rewrite / expire / orphan cleanup mutate table data and metadata.       |

Also grant so TMS can read policies and submit Jobs:

| Privilege          | Why                                                  |
| ------------------ | ---------------------------------------------------- |
| `VIEW_POLICY`      | Read attached maintenance policies and `jobOptions`. |
| `USE_JOB_TEMPLATE` | Use built-in maintenance job templates.              |
| `RUN_JOB`          | Submit Spark maintenance Jobs.                       |

Do **not** grant `MANAGE_USERS`, `CREATE_CATALOG`, or `MANAGE_GRANTS`.

If authorization is later enabled on a cluster that already had TMS running, the next plugin start
bootstraps missing `tms` users and grants.

### 5.6 Policy evaluate triggers (`onCommit` + `crontab`)

Automated TMS is **policy-gated**: without an Active, enabled maintenance policy attached to the
table (directly or via schema / catalog), commit events do nothing and no crontab evaluate is
scheduled. The policy also defines **when to evaluate** — not only thresholds and job parameters.

**Product model:** two automated evaluate triggers (combinable on the **same** policy — **one**
`policy_meta` row, one `content.schedule` object; **not** two policy records just because both
triggers are enabled):

| Trigger    | Meaning                                        | Typical use                                     |
| ---------- | ---------------------------------------------- | ----------------------------------------------- |
| `onCommit` | After IRC commit, drive ordered types (§5.6.1) | Write-heavy tables; types that allow `onCommit` |
| `crontab`  | Periodic expand on a crontab expression        | Timed maintenance; required for orphan-cleanup  |

Use **two policies** only when maintenance **types** differ, not because `onCommit` and `crontab`
are both set on one policy.

These are **not** the same as `minIntervalMs` (§8.3): `crontab` decides **when ① expand runs**;
`onCommit` goes through **③** (not ①). `minIntervalMs` caps how soon another enqueue / submit
may proceed after the last finished job (`MAX(finished_at)` on `table_maintenance_job` for that
`(table, policy)`).

**`orphan-cleanup` must not use `onCommit`.** Policy create/alter rejects `schedule.onCommit = true`
for orphan-cleanup (or ignores it). Orphan cleanup is **crontab-only** (or ops API).

#### 5.6.1 Commit-driven type order

On the **commit path** for a given table, TMS does **not** run every attached `onCommit` type in
parallel. It builds the set of Active, attached, commit-allowed types for that table, then runs
**only those types that are actually attached**, in this **fixed** order:

```text
1. compaction
2. manifest-rewrite
3. snapshot-expiry
```

Types that are **not** attached (or not `onCommit`) are **skipped** — the chain continues with the
next attached type in the list. Never reorder attached types. Never insert a type that is not
attached.

**Sequencing:** `tms-table-commit` is one row per `{table_id}`. Submit the 5.6.1 head chosen at
pick time; on that Job’s terminal, upsert the same `{table_id}` again if another type is still
needed. Do not submit the next type while `finished_at IS NULL` for the previous policy.

**Crontab path** remains per-policy expand (① per `policy_id`) and does **not** require this
cross-type order unless product later unifies timed runs the same way.

**Storage:** trigger configuration lives in **`policy_version_info.content`**, not in `policy_meta`
columns and not in `scheduled_tasks`. The UI reads and writes the same JSON the TMS scheduler
uses.

Illustrative `content.schedule` (exact field names may be finalized with the Policy API / UI):

```json
{
  "minDataFileMse": 1000,
  "rewriteOptions": { "target-file-size-bytes": "536870912" },
  "schedule": {
    "onCommit": true,
    "crontab": "0 2 * * *"
  }
}
```

| Field               | UI                           | TMS runtime                                                           |
| ------------------- | ---------------------------- | --------------------------------------------------------------------- |
| `schedule.onCommit` | Toggle “run on commit”       | IRC upserts ③; table pool drives §5.6.1 order (§5.1.1)                 |
| `schedule.crontab`  | Crontab picker               | Sets / refreshes ① next `scheduled_tasks.execution_time` after expand |

**Rules:**

- At least one of `onCommit` or `crontab` should be set for automated maintenance; both may be set
  on the same policy (same ① `task_instance = {policy_id}`), except **orphan-cleanup** which allows
  **`crontab` only**.
- `crontab` only — common for orphan-cleanup and low-write tables.
- `onCommit` — commit path applies §5.6.1 order across attached compaction / manifest-rewrite /
  snapshot-expiry policies; each step may still skip submit when gates or `Recommender` fail.
- Manual ops APIs (§7) are a separate, operator-initiated path and do not use `schedule`.

**Where state lives:**

```text
policy_version_info.content.schedule  →  what the UI shows; source of truth for triggers
scheduled_tasks ① tms-policy-expand      →  crontab expand; long-lived
scheduled_tasks ② tms-table-scheduler    →  short submit; DELETE after pick
scheduled_tasks ③ tms-table-commit       →  short submit; DELETE after pick
table_maintenance_job + minIntervalMs →  in-flight + expand cooldown before INSERT ② (not crontab)
```

**Crontab timeline (example: `0 2 * * *`):**

```text
[policy create/enable] → INSERT ① tms-policy-expand; execution_time = next 02:00
02:00                  → N nodes poll; ONE picks ①
                       → expand INSERT N × ② tms-table-scheduler (execution_time = now)
                       → retain ①; set next crontab due
~02:00+                → nodes pick each ② (parallel across units / nodes)
                       → sample before → runJob → INSERT table_maintenance_job → DELETE that ②
                       → Job terminal: sample after → UPDATE finished_at
```

`execution_time = 02:00` on ① is the **expand due time**. Spark units ② become due when expand
writes them (typically immediately after). They are **not** also stamped for wall-clock 02:00:00.
Unlike ①, each ② is **DELETE**d after the short submit path; Spark continues asynchronously.

### 5.7 Job template parameters on policy (`policy_version_info.content`)

`job_run_meta.runtime_job_template` is a **run snapshot** (what that Job actually used). Default
parameters for automated maintenance are **not** stored there.

Non-auth job-template parameters (Spark resource and strategy options such as
`spark.executor.memory`, `spark.sql.shuffle.partitions`, Iceberg rewrite options) live in
**`policy_version_info.content`** (same place as `schedule` — §5.6), typically as `jobOptions` /
`rewriteOptions`. They are **not** `policy_meta` columns, and TMS does **not** add a separate
options table. Compaction already forwards `rewriteOptions` as `job.options.*`; other built-in
types (`system_iceberg_snapshot_expiration`, orphan cleanup, manifest rewrite) use the same
`jobOptions` map in content.

**Precedence** (first hit wins), matching policy attachment:

```text
table-attached policy of that type
  > schema-attached policy of that type
  > catalog-attached policy of that type
  > job template base configuration
```

Attach one policy of the type at catalog for cluster defaults. Attach another policy of the **same
type** at a schema or table only where parameters differ. A TMS-owned table keyed by catalog /
schema / table for these options would duplicate `policy_relation_meta` and drift — **rejected**
(§4.6.1).

At submit:

```text
job template base configs
  overlay nearest attached policy jobOptions  (non-auth only)
  overlay SecretManager-resolved auth + credential-vending keys (§5.8)
  → JobSubmitter.runJob(..., jobConfig) as tms
```

Policy create / alter **rejects** credential-shaped keys in `jobOptions` / `rewriteOptions`
(same name rules as property masking: `password`, `secret`, `token`, `access-key`, and similar).
Those keys belong in SecretManager (§5.8), referenced by URN where needed.

### 5.8 Credential vending and IRC authentication (SecretManager)

Automated TMS Jobs talk to Gravitino **Iceberg REST** as Spark catalogs. Two orthogonal concerns:

1. **Credential vending** — how Spark gets temporary storage credentials for table data.
2. **IRC authentication** — how Spark authenticates to the Iceberg REST endpoint (`none` /
   `basic` / `oauth` / `kerberos`).

Sensitive values (passwords, client secrets, keytabs contents, static cloud keys if ever needed)
must **not** sit in `policy_meta`. They are stored and resolved through Gravitino
**SecretManager** / **SecretProvider** (URN references), the same pluggable path Jerry asked for
on server JDBC passwords. The solution is **opt-in**: when SecretManager is not configured,
non-secret auth properties and plaintext test values can still be used for local runs.

Keys below are written into the Job's `spark_conf` map (Iceberg REST catalog properties). Exact
prefixing (`spark.sql.catalog.<name>.…`) is applied by the job template / submitter from the
catalog name in `jobConfig`.

#### Credential vending

When maintenance Jobs should use **vended** storage credentials (recommended for production),
configure these on the **Spark / Job** side for every auth mode:

| Property                             | Required          | Description                                                                                                     |
| ------------------------------------ | ----------------- | --------------------------------------------------------------------------------------------------------------- |
| `header.X-Iceberg-Access-Delegation` | yes (for vending) | Must be `vended-credentials` so IRC returns temporary storage credentials on `loadTable`.                       |
| `uri`                                | yes               | Iceberg REST base URI (for example `http://host:9001/iceberg`).                                                 |
| `type`                               | yes               | `rest`.                                                                                                         |
| `warehouse` / `prefix`               | as needed         | Catalog warehouse or REST prefix when the deployment uses them.                                                 |
| `io-impl`                            | usually yes       | Iceberg FileIO implementation matching the warehouse scheme (for example `org.apache.iceberg.aws.s3.S3FileIO`). |

**Do not** put long-lived `s3.access-key-id` / `s3.secret-access-key` (or equivalent) into TMS Job
`spark_conf` when vending is enabled — the client must consume the vended block from IRC. Mixing
static keys with the vending header causes engines to ignore temporary credentials.

**Catalog-side** (operator configures on the Gravitino catalog / IRC, not in policy `jobOptions`):

| Property                                                           | Required              | Description                                                                                                                                        |
| ------------------------------------------------------------------ | --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `data-access` or IRC `header.X-Iceberg-Access-Delegation` defaults | recommended           | Advertise `vended-credentials` to clients.                                                                                                         |
| `credential-providers`                                             | yes for token vending | For example `s3-token`, `oss-token`, `adls-token`, `gcs-token` (see [Credential vending](../docs/security/credential-vending.md)).                 |
| Provider-specific keys (`s3-role-arn`, `s3-region`, …)             | yes per provider      | Server-side keys used to mint temporary credentials. Secrets here also go through SecretManager / catalog secret bindings, not TMS policy content. |

Auth mode subsections below only add IRC **client authentication** properties on top of this
credential-vending baseline.

#### Auth: none

No IRC client authentication (typical `simple` / open IRC in lab).

| Property | Required | Description |
| -------------------------------------------------------- | ---------------- | ---------------- | ----------------------------------------------------------------------------------------- |
| *(omit `rest.auth.type`)* or `rest.auth.type` = `none`   | no               | no               | No username / password / token.                                                           |
| Credential-vending properties in 5.8 Credential vending | if using vending | no (header only) | Still set `header.X-Iceberg-Access-Delegation=vended-credentials` when storage is vended. |

No SecretManager entries are required for IRC auth itself.

#### Auth: basic

HTTP Basic against Gravitino IRC (local users / basic authenticator). TMS Jobs authenticate as
the maintenance principal (usually user `tms` when authorization is on).

| Property | Required | Description |
| -------------------------------------------------------- | ---------------- | -------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `rest.auth.type`                                         | yes              | no             | `basic`.                                                                                                               |
| `rest.auth.basic.username`                               | yes              | no             | For automated TMS runs: `tms` (or the configured TMS username).                                                        |
| `rest.auth.basic.password` | yes | **yes** |
| Credential-vending properties in 5.8 Credential vending | if using vending | —              | Same as parent section.                                                                                                |

#### Auth: oauth

OAuth2 client credentials or bearer token against Gravitino IRC.

| Property | Required | Description |
| -------------------------------------------------------- | ----------------------------------------- | -------------- | ---------------------------------------------------------------------------------- |
| `rest.auth.type`                                         | yes                                       | no             | `oauth2`.                                                                          |
| `token`                                                  | one of token **or** client-credential set | **yes**        | Bearer access token path (short-lived; often minted at submit rather than stored). |
| `oauth2-server-uri`                                      | for client-credential path                | no             | Token endpoint URI.                                                                |
| `credential`                                             | for client-credential path                | **yes**        | OAuth client id and secret, typically `client_id:client_secret`.                   |
| `scope`                                                  | recommended                               | no             | OAuth scope (Iceberg may default to `catalog` if omitted).                         |
| Credential-vending properties in 5.8 Credential vending | if using vending                          | —              | Same as parent section.                                                            |

Prefer client-credential + SecretManager for `credential` so TMS does not embed long-lived bearer
tokens in policy or templates.

#### Auth: kerberos

Kerberos / SPNEGO style access when Gravitino authenticators include `kerberos`. Spark / Job
runtime must be able to obtain a TGT for the TMS service principal.

| Property | Required | Description |
| ---------------------------------------------------------------------------- | ----------------------------------------------------------------------- | -------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `rest.auth.type`                                                             | yes (when Iceberg REST client exposes it) / Gravitino client `authType` | no                                     | `kerberos` (or deployment-specific key used by the maintenance Job’s Iceberg / Gravitino client).                     |
| Kerberos principal                                                           | yes                                                                     | no                                     | Service principal used by TMS Jobs (for example `tms/_HOST@REALM`).                                                   |
| Keytab path or keytab material | yes | **yes** for keytab bytes / path secret |
| `java.security.krb5.conf` / Hadoop `hadoop.security.authentication=kerberos` | as required by the cluster                                              | no                                     | Cluster Kerberos wiring for the Spark driver/executors.                                                               |
| Credential-vending properties in 5.8 Credential vending                     | if using vending                                                        | —                                      | Same as parent section. Kerberos authenticates to IRC; storage access still uses vended credentials when enabled.     |

#### Resolution at submit

```text
1. Load non-auth spark_conf / jobOptions from template ⊕ nearest policy
2. Select IRC auth mode (none | basic | oauth | kerberos) from TMS / metalake settings
3. Add credential-vending keys when enabled (header.X-Iceberg-Access-Delegation=…)
4. Resolve SecretManager URNs for password / oauth credential / keytab
5. Redact secrets in logs and in persisted runtime_job_template snapshots
6. runJob as principal tms
```

No `tms_credential` table. Switching from a local file SecretProvider to Vault / cloud KMS is a
provider configuration change only.

---

## 6. Multi-node coordination

On **multiple** Gravitino / TMS nodes, IRC on the commit node **upserts** ③, but **every** node runs
db-scheduler. Writing `scheduled_tasks` does **not** cause all nodes to execute; when
`execution_time` is due, nodes **compete** and **only one** picks a given instance (CAS + heartbeat).

**Enqueue ①:** policy create/enable INSERT; crontab sets next `execution_time` (5.4, 5.6).

**Enqueue ②:** written only by a successful ① expand pick (§5.4).

**Enqueue ③:** IRC upsert `(tms-table-commit, {table_id})` (§5.1.1).

**Expand mutex:** pick on ① (`expand.threads`) (§6.1).

**Table mutex:** pick on each ② (`table.threads`); after short submit, **DELETE**.
**Commit mutex:** pick on each ③ (`commit.threads`); after decide/submit, **DELETE**.
Anti-double-submit: in-flight `table_maintenance_job` + `minIntervalMs` (5.4.2, §6.2).

**Dead worker / stuck Spark:** missed heartbeats only free a **hung callback** (callbacks are short).
Long Spark / lost terminal updates are repaired by **reconcile** on `finished_at IS NULL` (§5.4.2) —
set `finished_at` only; never late-sample Validation metrics.

| Table                                  | Role                                                                    |
| -------------------------------------- | ----------------------------------------------------------------------- |
| `policy_meta` / `policy_relation_meta` / `policy_version_info` | **What** to expand; `schedule` + non-auth `jobOptions` in **content** (§5.6, §5.7) |
| `scheduled_tasks`                      | ① long-lived expand + ②/③ one-shot (DELETE after short submit)          |
| `table_maintenance_job`                | Per-run Validation JSON + submit gates (§6.2)                           |
| SecretManager / SecretProvider         | IRC auth secrets for `tms` (§5.8); no `tms_credential` table            |
| `user_meta`                            | Built-in `tms` user when authorization is enabled (§5.5)                |

Auth material uses SecretManager. Job-template **parameters** stay on policy attachments.

### 6.1 Three-pool leases (`scheduled_tasks`)

```text
Policy create → INSERT ① (policy_id)
crontab due → pick ① (expand.threads) → INSERT many ② → retain ①
IRC commit → upsert ③ {table_id} (IRC thread; not either pool)
Node A / B / C
        │
        ├─ expand pool: pick ① → INSERT ② → retain ①
        ├─ table pool:   pick ② → short submit → DELETE ②
        ├─ commit pool:  pick ③ → decide/submit → DELETE ③
        │                 (Job terminal may re-upsert same {table_id})
        └─ picker dies → missed heartbeats → instance runnable again
```

**Not** “write `scheduled_tasks` ⇒ every node executes.” **Yes** “when due ⇒ every node may try;
exactly one claim runs that instance.”

Illustrative `scheduled_tasks` usage (db-scheduler owned; MySQL-shaped):

```sql
-- owned by db-scheduler; see upstream DDL
-- PRIMARY KEY (task_name, task_instance)
-- ① tms-policy-expand / {policy_id}  (task_data empty or cursor)
-- ② tms-table-scheduler / table:{table_id}:{policy_id} or batch:{batch_id}:{policy_id}
--    task_data = { tableIds, policyIds }
-- ③ tms-table-commit / {table_id}  task_data = { tableIds, policyIds }
-- execution_time, picked, picked_by, last_heartbeat, version, task_data, ...
-- ②/③: DELETE after short submit path (do not wait for Spark).
-- ③ may upsert the same {table_id} again after Job terminal (§5.6.1).
-- ① is deleted only on policy disable / drop / replace (§5.4.2).
```

### 6.2 Per-run maintenance job table (`table_maintenance_job`)

**Table name:** `table_maintenance_job`

One row per Spark `job_run_id`. The same table serves Automate **Jobs → Validation** (before/after
JSON), expand-time `minIntervalMs`, and in-flight submit guards. `minIntervalMs` uses `finished_at`
on this row as the task end time, not `job_run_meta.job_finished_at`.

| Column           | Type                       | Notes                                                                        |
| ---------------- | -------------------------- | ---------------------------------------------------------------------------- |
| `job_run_id`     | `BIGINT UNSIGNED NOT NULL` | `job_run_meta.job_run_id`; primary key                                       |
| `metalake_id`    | `BIGINT UNSIGNED NOT NULL` | Metalake id                                                                  |
| `table_id`       | `BIGINT UNSIGNED NOT NULL` | `table_meta` surrogate id (after 5.4.3 import)                              |
| `policy_id`      | `BIGINT UNSIGNED NOT NULL` | `policy_meta.policy_id`                                                      |
| `before_metrics` | `MEDIUMTEXT NULL`          | JSON sampled **before** table mutation; may stay null until terminal persist |
| `after_metrics`  | `MEDIUMTEXT NULL`          | JSON after job terminal status; null while pending                           |
| `finished_at`    | `BIGINT UNSIGNED NULL`     | Task end time (epoch millis); **null = in-flight**; drives `minIntervalMs`   |

**Primary key:** (`job_run_id`). No `table_identifier` or `schema_id` column — `table_id` is
sufficient and aligns with `policy_relation_meta.metadata_object_id` for table policies.

No `evaluate_pending`, no `IDLE`/`RUNNING`, no application lease heartbeat — those are
`scheduled_tasks` concerns.

| Store                   | Role                                   | Suitable for Jobs Validation?  |
| ----------------------- | -------------------------------------- | ------------------------------ |
| `statistic_meta`        | Latest statistic per object + name     | No — current value only        |
| `table_metrics`         | Append-only time series by `metric_ts` | No — not keyed by `job_run_id` |
| `job_metrics`           | Job-scoped optimizer time series       | No — not table before/after UI |
| `table_maintenance_job` | Frozen before/after JSON per job       | **Yes**                        |

**Expand / submit gates** (for a given `(metalake_id, table_id, policy_id)`):

1. **Min interval (on ①):** `SELECT MAX(finished_at) …` vs resolved `minIntervalMs` (§8.3) — if
   within cooldown, **do not INSERT** ②.
2. **In-flight (on ① and re-checked on ②):** `SELECT 1 … WHERE finished_at IS NULL LIMIT 1` — if
   found, skip enqueue / skip submit.

**Lifecycle:**

1. After gates + `Recommender` pass: `runJob` → obtain `job_run_id`.
2. **Immediately** `INSERT` (`job_run_id`, `metalake_id`, `table_id`, `policy_id`) with
   `finished_at = NULL` (in-flight claim). `before_metrics` / `after_metrics` may be null here.
3. Spark runs asynchronously. **Sample** `before_metrics` before any table mutation (TMS submit
   path or Spark job start). Persisting that JSON may wait until step 4.
4. On job terminal status (job callback / listener): `UPDATE` `before_metrics` (if not written
   yet), `after_metrics`, and `finished_at`.

Do **not** sample “before” metrics after rewrite / expire has already mutated the table.

Illustrative MySQL DDL:

```sql
CREATE TABLE IF NOT EXISTS `table_maintenance_job` (
    `job_run_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'job run id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `table_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'table id from table_meta',
    `policy_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'policy id from policy_meta',
    `before_metrics` MEDIUMTEXT NULL COMMENT 'JSON sampled before mutation; may persist at terminal',
    `after_metrics` MEDIUMTEXT NULL COMMENT 'JSON object string after job; null while pending',
    `finished_at` BIGINT(20) UNSIGNED NULL COMMENT 'maintenance task end time (epoch millis); null = in-flight',
    PRIMARY KEY (`job_run_id`),
    KEY `idx_tmj_table_policy_finished` (`metalake_id`, `table_id`, `policy_id`, `finished_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT 'per-run TMS job record: Validation JSON + submit gates';
```

**JSON format:** UTF-8 object with flat metric keys. Values are numbers, strings, or booleans.
Optional top-level fields:

- `task_type` — e.g. `compaction`, `snapshot-expiry` (recommended in `before_metrics`)
- `passed` — overall validation result (recommended in `after_metrics` only)

Illustrative `before_metrics` / `after_metrics` by task type:

| Task type          | Example keys in JSON                                      | Auto pass/fail            |
| ------------------ | --------------------------------------------------------- | ------------------------- |
| `compaction`       | `num_files`, `avg_file_size_bytes`, `snapshot_count`      | Recommended (`passed`)    |
| `snapshot-expiry`  | `snapshot_count`, optional `deleted_manifest_files_count` | Recommended (`passed`)    |
| `manifest-rewrite` | `manifest_file_count` (if collected)                      | Optional / skip           |
| `orphan-cleanup`   | delete summary counts; skip Files before/after compare    | Skip table-metric compare |

Example `after_metrics`:

```json
{
  "task_type": "compaction",
  "num_files": 42,
  "avg_file_size_bytes": 134217728,
  "snapshot_count": 8,
  "passed": true
}
```

### 6.3 Table drop lifecycle

After a successful Iceberg **table drop**, IRC invokes an in-process `IcebergTableLifecycleHook`:

1. Resolve dropped `catalog.schema.table` → `table_id` when present in `table_meta`.
2. `DELETE` outstanding **`tms-table-scheduler`** rows for that table (`table:{table_id}:…`) and
   **`tms-table-commit`** `{table_id}`.
3. `DELETE` from `table_maintenance_job` for `(metalake_id, table_id)`.
4. Do **not** delete **`tms-policy-expand`** ① (policy-scoped); remaining tables under the policy
   still expand on the next due. In-flight Spark jobs are **not** cancelled by this hook.

**Table rename is out of scope.**

---

## 7. Optimizer CLI replacement APIs

The commit path stays in-process and does **not** call these APIs. They replace the
`gravitino-optimizer` CLI (`--type ...`) for operators and scripts. The same plugin serves them on
the main webserver (**8090**).

- Prefix: `/api/maintenance/table/ops/...`. Not shown in the Compact-policy UI.
- Caller must have **WRITE** on each target table. Missing privilege → **403**.
- `--conf-path` stays server configuration. `update-statistics` and `append-metrics` send JSON Lines
  in the body. The API does not accept a server `--file-path`.
- `dryRun=true` returns the recommendation or job config and does not submit.

| CLI `--type`              | Method | Path                                           | Body or query                                                                                      |
| ------------------------- | ------ | ---------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `submit-strategy-jobs`    | `POST` | `/api/maintenance/table/ops/strategy-jobs`     | `identifiers`, `strategyName`, `dryRun`, `limit`                                                   |
| `submit-update-stats-job` | `POST` | `/api/maintenance/table/ops/update-stats-jobs` | `identifiers`, `dryRun`, `updateMode` (`stats` / `metrics` / `all`), `updaterOptions`, `sparkConf` |
| `update-statistics`       | `POST` | `/api/maintenance/table/ops/statistics`        | `calculatorName`, `identifiers`, `statisticsPayload` (JSON Lines)                                  |
| `append-metrics`          | `POST` | `/api/maintenance/table/ops/metrics`           | `calculatorName`, `identifiers`, `statisticsPayload` (JSON Lines)                                  |
| `monitor-metrics`         | `POST` | `/api/maintenance/table/ops/metrics/monitor`   | `identifiers`, `actionTime`, `rangeSeconds`, `partitionPath`                                       |
| `list-table-metrics`      | `GET`  | `/api/maintenance/table/ops/metrics/tables`    | `identifiers`, `partitionPath`                                                                     |
| `list-job-metrics`        | `GET`  | `/api/maintenance/table/ops/metrics/jobs`      | `identifiers`                                                                                      |

Each route calls the existing optimizer command implementation. The IRC hook and scheduler
pipelines (`PolicyExpandPipeline` / `MaintenanceSparkSubmitPipeline`) do not call this group.

---

## 8. Configuration

### 8.1 Enablement keys (`gravitino.conf`)

| Key                                       | Default | Description                                                                                               |
| ----------------------------------------- | ------- | --------------------------------------------------------------------------------------------------------- |
| `gravitino.server.rest.extensionPackages` | none    | Must include the TMS Feature package (illustrative: `org.apache.gravitino.maintenance.web.rest.feature`). |
| `gravitino.auxService.names`              | none    | Must include `iceberg-rest` when using IRC. TMS itself is **not** started this way.                       |

### 8.2 Iceberg REST → TMS in-process event keys

Illustrative keys (exact names may be finalized in implementation).

| Key (illustrative)                                  | Default | Description                                                                                |
| --------------------------------------------------- | ------- | ------------------------------------------------------------------------------------------ |
| `gravitino.iceberg-rest.tableMaintenance.inProcess` | `false` | When `true`, IRC invokes the **main-server-registered event callback / SPI** after commit. |

Because IRC may use an isolated classloader, the callback must be registered by the TMS plugin (for
example on `GravitinoEnv`), not a direct cast to TMS implementation classes. IRC and the main
server must share **one JVM**.

```properties
gravitino.server.rest.extensionPackages = org.apache.gravitino.maintenance.web.rest.feature
gravitino.auxService.names = iceberg-rest
gravitino.iceberg-rest.tableMaintenance.inProcess = true
gravitino.maintenance.scheduler.enabled = true
gravitino.maintenance.scheduler.expand.threads = 4
gravitino.maintenance.scheduler.table.threads = 8
gravitino.maintenance.scheduler.commit.threads = 4
```

HTTP `tableMaintenance.uri` / Kafka produce-consume keys are **not** in scope (Non-Goal #5).

### 8.3 Task types and minimum interval (global default + table override)

TMS recognizes four maintenance **task types** (aligned with product Compact policy surface):

| Task type          | Typical job / policy                             | Code default `minIntervalMs` |
| ------------------ | ------------------------------------------------ | ---------------------------- |
| `compaction`       | rewrite data files / `system_iceberg_compaction` | `3600000` (1 hour)           |
| `snapshot-expiry`  | expire snapshots                                 | `86400000` (1 day)           |
| `manifest-rewrite` | rewrite manifests                                | `86400000` (1 day)           |
| `orphan-cleanup`   | orphan file cleanup                              | `604800000` (7 days)         |

**Commit path:** only `compaction`, `manifest-rewrite`, and `snapshot-expiry` may use `onCommit`,
and only in that order among attached types (§5.6.1). **`orphan-cleanup` is crontab-only.**

**Catalog / schema attachments:** When a policy (including snapshot-expiry) is attached at
**catalog** or **schema** scope, expand walks tables under that attachment and enqueues one crontab
② per table — all with the **same** `policy_id` (`{table_id}:{policy_id}`), **paged** across ① picks
(5.4.2, default 100 ② per pick). **One table per Spark job**; no multi-table `task_instance`.

**Resolution order** (first hit wins), same idea as Amoro table props + AMS defaults:

```text
1. Table property override (if set)
2. Global gravitino.conf key (if set)
3. Code default in the table above
```

**Global keys** (`gravitino.conf`, prefix `gravitino.maintenance.`):

| Key                                   | Description                                                                                  |
| ------------------------------------- | -------------------------------------------------------------------------------------------- |
| `task.compaction.minIntervalMs`       | Default min interval for compaction jobs                                                     |
| `task.snapshot-expiry.minIntervalMs`  | Default min interval for snapshot expiry                                                     |
| `task.manifest-rewrite.minIntervalMs` | Default min interval for manifest rewrite                                                    |
| `task.orphan-cleanup.minIntervalMs`   | Default min interval for orphan cleanup                                                      |
| `expand.enqueueBatchSize`             | Max ② rows one ① expand pick may INSERT (default `100`; crontab paging cursor in ① `task_data`) |

**Table-level overrides** (Iceberg / Gravitino table properties):

| Property                                     | Overrides                                    |
| -------------------------------------------- | -------------------------------------------- |
| `maintenance.compaction.minIntervalMs`       | Compaction min interval for this table       |
| `maintenance.snapshot-expiry.minIntervalMs`  | Snapshot expiry min interval for this table  |
| `maintenance.manifest-rewrite.minIntervalMs` | Manifest rewrite min interval for this table |
| `maintenance.orphan-cleanup.minIntervalMs`   | Orphan cleanup min interval for this table   |

On each **① expand**, for every candidate work unit the pipeline checks `MAX(finished_at)` on
`table_maintenance_job` for that `(table_id, policy_id)` against the resolved `minIntervalMs` for
that task type (§5.4.2). An in-flight row (`finished_at IS NULL`) also skips INSERT ② (and ②
re-checks before `runJob`). Policy content still owns **trigger thresholds** (e.g. MSE); the
interval only limits how often expand may enqueue another unit once ① runs (scheduler pick or ops
API).

Example table override:

```sql
ALTER TABLE rest_catalog.db.orders SET TBLPROPERTIES (
  'maintenance.compaction.minIntervalMs' = '7200000'
);
```

### 8.4 db-scheduler keys (`gravitino.conf`)

db-scheduler has **one** thread pool per `Scheduler`. TMS runs **three** schedulers on one
`scheduled_tasks` table:

| Scheduler | Registers | Default threads | conf key |
| --------- | --------- | --------------- | -------- |
| Expand | `tms-policy-expand` only | `4` | `…scheduler.expand.threads` |
| Table | `tms-table-scheduler` only | `8` | `…scheduler.table.threads` |
| Commit | `tms-table-commit` only | `4` | `…scheduler.commit.threads` |

**Commit threading:** see §5.4. IRC only **upserts** ③ (short; not on any db-scheduler pool).
**Decide whether to submit Spark + short submit** for ③ runs only on **`commit.threads=4`** —
not IRC, not `expand.threads`, not `table.threads`.

| Key | Default | Description |
| --- | ------- | ----------- |
| `gravitino.maintenance.scheduler.enabled` | `true` on MySQL/PostgreSQL; `false` on H2 | Enables all three schedulers |
| `gravitino.maintenance.scheduler.expand.threads` | `4` | Expand pool (① only) |
| `gravitino.maintenance.scheduler.table.threads` | `8` | Table pool (② only) |
| `gravitino.maintenance.scheduler.commit.threads` | `4` | Commit pool (③ only): post-IRC decide + short submit |
| `gravitino.maintenance.scheduler.pollingIntervalMs` | `10000` | Poll interval (all) |
| `gravitino.maintenance.scheduler.heartbeatIntervalMs` | `60000` | Heartbeat while callback runs |
| `gravitino.maintenance.scheduler.missedHeartbeatsLimit` | `6` | Misses before dead |
| `gravitino.maintenance.scheduler.alwaysPersistTimestampInUTC` | `true` on MySQL | MySQL timestamp handling |

JDBC pool size ≥ `expand.threads + table.threads + commit.threads` (+ REST/IRC headroom). Defaults
are enough for most deployments; override only when tuning.

Dependency (illustrative):

```kotlin
implementation("com.github.kagkarlsson:db-scheduler:<version>")
```

`scheduled_tasks` DDL is added to the entity-store migration for MySQL and PostgreSQL backends only.


---

## 9. Work Plan and Checklist

### 9.1 Suggested Work Plan

This design delivers the in-process plugin, three `scheduled_tasks` names (①/②/③), three pools
(`expand.threads=4` / `table.threads=8` / `commit.threads=4`), per-run `table_maintenance_job`, `tms` principal +
SecretManager, and policy `jobOptions`.

| Phase | Work item                                | Notes                                                                                                                          |
| ----- | ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| 1     | Load the in-process plugin               | `TableMaintenanceRESTFeature`; start/stop db-scheduler; `TmsPrincipalBootstrapListener`.                                       |
| 2     | Internal expand + spark-submit pipelines | `PolicyExpandPipeline` + `MaintenanceSparkSubmitPipeline`; unit tests.                                                         |
| 3     | db-scheduler three tasks + three pools   | ①+②+③; expand=4 / table=8 / commit=4; migration; heartbeats (5.4, §8.4).                                                      |
| 4     | In-process IRC hook (upsert ③)           | Upsert `tms-table-commit` (§5.1.1); policy create inserts ①; expand writes ② (§6.1).                                              |
| 5     | Hardening                                | Service metrics, graceful shutdown, H2 path tests, user docs.                                                                  |
| 6     | Optimizer CLI replacement APIs           | Ops resources in §7. Same commands as `gravitino-optimizer`.                                                                   |
| 7     | TMS principal + SecretManager auth       | `tms` user/role (§5.5); credential vending + none/basic/oauth/kerberos (§5.8); policy `schedule` (§5.6) + `jobOptions` (§5.7). |

#### Phase 1 checklist

- [ ] Add `TableMaintenanceRESTFeature` (Jersey 2 `Feature`) that registers the in-process callback.
      Ops resources are added in phase 6. Do not add a health or commit-event resource.
- [ ] Register via `gravitino.server.rest.extensionPackages` (illustrative package
      `org.apache.gravitino.maintenance.web.rest.feature`).
- [ ] Package plugin jars with the main Gravitino server distribution (on the main server classpath).
- [ ] Document `extensionPackages` enablement.
- [ ] Add a unit test that the feature registers the callback and exposes no commit or health
      resource.
- [ ] `TmsPrincipalBootstrapListener` (`EventListenerPlugin`): skip `user_meta` when authorization is
      disabled; when enabled, register on `eventListenerManager`, reconcile all metalakes on plugin
      `start()`, and reconcile on `CreateMetalakeEvent` (`ASYNC_ISOLATED`) (§5.5).

#### Phase 2 checklist

- [ ] Implement `GravitinoTableImportService` (`TableDispatcher.loadTable` → `table_meta`; 5.4.3).
- [ ] Backend-aware owner resolution after import; never default owner to `tms`.
- [ ] Implement `PolicyExpandPipeline` (attachments → expand gates → INSERT ungated `tms-table-scheduler` ②; retain ①).
- [ ] Implement `MaintenanceSparkSubmitPipeline` calling `Recommender.submitForStrategyName`.
- [ ] Enforce gates: in-flight / min-interval on ①; ②/③ short submit then DELETE; Job listener + reconcile (§5.4.2).
- [ ] Resolve Active attached policies via existing Policy / `StrategyProvider` (no new policy store).
- [ ] Add unit tests for skip / noop / submit outcomes.

#### Phase 3 checklist

- [ ] Add `TableMaintenanceScheduler` with expand (4) + table (8) + commit (4) pools (§8.4).
- [ ] Add entity-store migration for `scheduled_tasks` (MySQL / PostgreSQL).
- [ ] Wire `DataSource` from the relational entity store; honor §8.4 heartbeat keys.
- [ ] On H2 backends: scheduler disabled; pipelines invoked directly in tests (§8.4).
- [ ] Integration test: two nodes; one pick wins; in-flight blocks second submit; reconcile closes
      stale `finished_at IS NULL` without inventing metrics.

#### Phase 4 checklist

- [ ] Add `IcebergCommitEventHandler` and main-server-registered in-process callback / SPI (§5.1.1 /
      §8.2).
- [ ] Add EntityStore migration for **`table_maintenance_job`** (§6.2).
- [ ] On policy create/enable: INSERT **`tms-policy-expand`** ① (`policy_id`) (§5.4.2).
- [ ] IRC post-commit **upserts** `tms-table-commit` `{table_id}`; rejects orphan-cleanup `onCommit`;
      does **not** `runJob` on the IRC thread (decide/execute on `commit.threads`).
- [ ] Tests: attached subset runs in fixed order; missing types skipped; terminal re-upserts ③;
      concurrent commits coalesce on one row.
- [ ] Wire IRC post-commit hook to the in-process callback (`tableMaintenance.inProcess`).
- [ ] Wire IRC **drop** hook to delete outstanding ②+③ + `table_maintenance_job` rows (§6.3).
- [ ] Sample `before_metrics` before `runJob`; INSERT job row; DELETE ②/③ immediately; Job listener
      writes `after_metrics` + `finished_at`; reconcile: terminal → `finished_at` only; missing Job → **DELETE** occupancy (§5.4.2).
- [ ] Integration tests: crontab `table:{table_id}:{policy_id}`; commit coalesce on
      `(tms-table-commit,{table_id})`; multi-node upsert; pick-time policy resolve; drop cleans rows.
- [ ] Do **not** ship HTTP `…/events/iceberg-commit` or Kafka ingress.

#### Phase 5 checklist

- [ ] Service metrics: enqueue counts, pick counts, submit counts, dead-execution recoveries, failures.
- [ ] Graceful shutdown: stop db-scheduler before entity store closes.
- [ ] Update user-facing TMS / optimizer docs for scheduler enqueue + per-run `table_maintenance_job`.

#### Phase 6 checklist

- [ ] Add the seven ops resources in §7, each calling the existing optimizer command implementation.
- [ ] Require WRITE on each target table; missing privilege returns 403.
- [ ] `dryRun=true` returns the recommendation or job config and does not submit.
- [ ] Accept statistics and metrics JSON Lines in the body. Do not accept a server `--file-path`.
- [ ] Tests: each CLI `--type` maps to one route; the commit path does not call these routes.

#### Phase 7 checklist

- [ ] Wire **SecretManager** for TMS IRC auth secrets (no `tms_credential` table). Opt-in: plaintext
      / missing secrets still work when SecretManager is not enabled.
- [ ] Policy create/alter rejects credential-shaped keys in `jobOptions` / `rewriteOptions`.
- [ ] Submit path: nearest policy `jobOptions` (table > schema > catalog) overlay template base;
      add credential-vending header when enabled; resolve auth for **none / basic / oauth /
      kerberos** (§5.8); `runJob` as `tms`.
- [ ] Tests: authorization off skips user insert; authorization on grants `USE_CATALOG`,
      `USE_SCHEMA`, `PROBE_TABLE_LIKE`, `MODIFY_TABLE`, `VIEW_POLICY`, `USE_JOB_TEMPLATE`, `RUN_JOB`.
- [ ] Tests: table attachment overrides catalog `jobOptions`; auth secrets never persist in
      `policy_meta`; `runtime_job_template` redacts passwords / tokens / keytabs.

### 9.2 Review Checklist

| Area         | Checklist                                                                                                                                  |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------ |
| Deployment   | Enabled via `gravitino.server.rest.extensionPackages`; IRC colocated in the same JVM. Ops APIs on **8090** (§7).                           |
| Classpath    | TMS plugin on main server classpath; **not** an aux isolated listener.                                                                     |
| Triggers     | Commit: ordered attached types (§5.6.1); crontab: per-policy ①; orphan-cleanup crontab-only.                                               |
| Scheduling   | **db-scheduler**: three pools expand/table/commit; ②/③ short submit then DELETE; occupancy on job table (5.4, §8.4).                    |
| Job records  | `table_maintenance_job` one row per `job_run_id` (Validation JSON + submit gates).                                                         |
| Import       | Lazy `table_meta` import via `TableDispatcher.loadTable` (§5.4.3); not Iceberg `registerTable`.                                            |
| Ops API      | Seven routes replace `gravitino-optimizer` (§7). Not used by the commit path. Table WRITE required.                                        |
| Pipeline     | ①→INSERT ②; ②/③ short submit; listener writes after; reconcile: terminal → `finished_at`; missing Job → DELETE occupancy (§5.4.2).       |
| Validation   | `table_maintenance_job` (`before_metrics` / `after_metrics` JSON + `finished_at`) (§6.2).                                                  |
| Drop         | Drop hook deletes outstanding ②+③ + `table_maintenance_job` rows (§6.3). Rename out of scope.                                              |
| Multi-node   | N compete, one pick; ②/③ pick+heartbeat anti-double-submit; dead-worker gated by `table_maintenance_job`.                               |
| Policy       | Reuses metalake Policy APIs; create inserts ①; `schedule` + `jobOptions` in `policy_version_info.content` (§5.6, §5.7).                   |
| Job boundary | Expand due ≠ Spark wall-clock; Spark in Jobs; Validation on `table_maintenance_job` (§6.2).                                                |
| Principal    | Automated Jobs run as `tms`; `TmsPrincipalBootstrapListener` on plugin start + `CreateMetalakeEvent` when authorization is enabled (§5.5). |
| Credentials  | Auth via SecretManager (none/basic/oauth/kerberos) + credential vending (§5.8); not policy / not `tms_credential`.                         |
| Security     | Ops APIs require table WRITE. TMS role is least-privilege for list + table write + run job. No commit-event or health endpoint.            |
| License      | db-scheduler is **Apache 2.0**; no LGPL scheduling dependency.                                                                             |

---

## 10. References

1. [Gravitino Iceberg REST service](../docs/iceberg-rest-service.md)
2. [Gravitino Lance REST service](../docs/lance-rest-service.md)
3. [Manage policies in Gravitino](../docs/manage-policies-in-gravitino.md)
4. [Iceberg compaction policy](../docs/iceberg-compaction-policy.md)
5. [Table Maintenance optimizer overview](../docs/table-maintenance-service/optimizer.md)
6. [Design of SCIM 2.0 User and Group Provisioning in Gravitino](./gravitino-scim-provisioning.md)
7. [Amoro AIP-3 – Event-Triggered Optimization of Iceberg Tables](https://cwiki.apache.org/confluence/display/AMORO/AIP-3%3A+Event-Triggered+Optimization+of+Iceberg+Tables+in+Amoro)
8. [OpenHouse architecture (Jobs Scheduler / CronJob data services)](https://github.com/linkedin/openhouse/blob/main/ARCHITECTURE.md)
9. [Apache Iceberg REST Catalog OpenAPI](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
10. [db-scheduler](https://github.com/kagkarlsson/db-scheduler) — embedded persistent scheduler (Apache 2.0)
