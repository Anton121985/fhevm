# Host Listener Poller — Requirements & Implementation Plan

## 1. Requirements

### 1.1 Role and Scope
- Role
  - Poller is a fallback to the WebSocket host-listener.
  - It incrementally processes finalized blocks missed by the WS listener because of WS gaps/drops or short reorgs near the head (bounded by a configurable lag).
- Scope
  - Processes only finalized blocks (as defined by `finality_lag`).
  - Ingests events only for the ACL and TFHE contracts (same ABIs as `AclContract` and `TfheContract`).
  - Inserts data idempotently into the DB; no destructive corrections (no deletes, minimal updates).

### 1.2 Finality and Progress Anchor

#### 1.2.1 Finality
- Config
  - `finality_lag: u64` (e.g., 15).
- Rule
  - Each iteration:
    - `latest = eth_blockNumber()`.
    - `safe_tip = latest.saturating_sub(finality_lag)`.
  - Poller must not process any block `b` with `b > safe_tip`.
- Purpose
  - Blocks above `safe_tip` are unstable; the poller leaves them to WS listener reorg handling.

#### 1.2.2 Progress Anchor
- State
  - For each `chain_id`, maintain monotonic `last_caught_up_block`: “All finalized blocks with number ≤ `last_caught_up_block` have been processed at least once by the poller.”
- Storage
  - New table `host_listener_poller_state`:
    - `chain_id` BIGINT PRIMARY KEY
    - `last_caught_up_block` BIGINT NOT NULL
    - `updated_at` TIMESTAMP NOT NULL DEFAULT NOW()
- Rule
  - Each iteration:
    - If `safe_tip ≤ last_caught_up_block`: nothing to do; sleep.
    - Else process blocks `b` in `(last_caught_up_block, safe_tip]` (optionally in batches).
    - On successful processing up to `T ≤ safe_tip`, update `last_caught_up_block = T`.
- Purpose
  - Ensure every finalized block is processed exactly once by the poller (unless intentionally re-run).
  - Avoid re-processing the same blocks forever as the chain grows.
  - Make continuous processing of `current_block - finality_lag` well-defined and efficient.

### 1.3 Data Model
- Existing tables
  - `host_chain_blocks_valid(chain_id, block_hash, block_number, ...)`:
    - Populated by host-listener and poller via `Database::mark_block_as_valid`.
  - `computations`, `pbs_computations`, `allowed_handles`:
    - Populated via `Database::insert_tfhe_event`, `insert_pbs_computations`, `insert_allowed_handle`.
  - Tenants & telemetry tables:
    - Used by `Database::new` and telemetry today.
- New table
  - `host_listener_poller_state` as above.
  - No foreign keys; keyed only by `chain_id`.

### 1.4 Configuration (CLI)
- `--rpc-url <HTTP_URL>` (required) — HTTP JSON-RPC endpoint for the L1 node.
- `--acl-contract-address <ADDR>` — single ACL contract address to monitor.
- `--tfhe-contract-address <ADDR>` — single TFHE contract address to monitor.
- `--database-url <URL>` — PostgreSQL connection URL.
- `--coprocessor-api-key <UUID>` — tenant API key.
- `--finality-lag <BLOCKS>` (default e.g., 15) — depth behind the head considered final.
- `--batch-size <BLOCKS>` (default e.g., 100) — max number of blocks to process per iteration.
- `--poll-interval <MS>` — sleep between iterations when there is no work or after processing a batch.
- `--retry-interval <MS>` — backoff between retry attempts on HTTP/RPC or DB failures.
- Logging & telemetry: `--log-level`, `--service-name` (e.g., `host-listener-poller`).

### 1.5 Behavior

#### 1.5.1 Startup
1. Parse CLI arguments.
2. Configure logging and `telemetry::setup_otlp(service_name)` if provided.
3. Create HTTP JSON-RPC provider using alloy for `rpc_url`.
4. Fetch `chain_id` from RPC.
5. Initialize `Database` via `Database::new(database_url, coprocessor_api_key, ...)`.
6. Verify RPC `chain_id` == `db.chain_id`; if mismatch, abort with error.
7. Initialize `last_caught_up_block`:
   - Query `host_listener_poller_state` for `chain_id`.
   - If found, use that value.
   - If not found:
     - Set `last_caught_up_block` to `Database::read_last_valid_block()`.
     - Insert initial row into `host_listener_poller_state`.

#### 1.5.2 Main Loop (incremental catch-up)
Each iteration:
1. Compute finality
   - `latest = eth_blockNumber()`.
   - `safe_tip = latest.saturating_sub(finality_lag)`.
   - If `safe_tip ≤ last_caught_up_block`:
     - Log “no new finalized blocks” and sleep `poll_interval`.
     - Continue.
2. Determine batch range
   - `target = min(safe_tip, last_caught_up_block + batch_size)`.
   - Blocks to process this iteration: `b ∈ (last_caught_up_block, target]`.
3. Process each block in range
   - For each block `b` in `(last_caught_up_block + 1 ..= target)`:
     - Fetch logs for block `b` using `eth_getLogs` with `fromBlock = b`, `toBlock = b`, and `address = [acl_address, tfhe_address]` (omit absent addresses).
     - Fetch block header for `b` to build `BlockSummary`.
     - Build `BlockLogs<Log>` with summary.number = `b`, hash/timestamp from header, `catchup = true`.
     - Ingest via shared logic:
       - Decode ACL logs and call `Database::handle_acl_event`.
       - Decode TFHE logs and call `Database::insert_tfhe_event`.
       - Mark the block as valid via `Database::mark_block_as_valid`.
       - All within a DB transaction created by `Database::new_transaction()` and optionally wrapped in a DB retry loop.
     - If an unrecoverable error occurs for `b`:
       - Log the error; break the block loop.
       - Do not update `last_caught_up_block` beyond `b - 1`.
4. Advance progress
   - If all blocks up to `target` were processed successfully:
     - Update `host_listener_poller_state.last_caught_up_block = target` for `chain_id`.
   - Log an iteration summary: `chain_id`, `latest`, `safe_tip`, previous and new `last_caught_up_block`, `blocks_processed`, `errors`.
5. Sleep
   - Sleep `poll_interval` and repeat.

#### 1.5.3 Idempotency and Concurrency
- DB-level idempotency:
  - Inserts into `computations`, `pbs_computations`, `allowed_handles`, `host_chain_blocks_valid` use primary keys and `ON CONFLICT DO NOTHING`.
  - Re-processing a block (e.g., after a crash and retry) yields no duplicates.
- Concurrency:
  - Poller can run alongside the WebSocket host-listener and other poller instances (typically one per chain).
  - Races are harmless:
    - If host-listener ingests first, poller writes are no-ops.
    - If poller ingests first, host-listener is constrained by the same DB keys.

### 1.6 Event Scope and Guarantees
- Events in scope
  - ACL: all events handled by `Database::handle_acl_event` (e.g., `Allowed`, `AllowedForDecryption`, plus others with side-effects).
  - TFHE: all events handled by `Database::insert_tfhe_event` (arithmetic/logic ops, casts, `TrivialEncrypt`, `FheRand*`, `FheIfThenElse`, etc.). Administrative no-ops remain no-ops.
- Guarantee
  - For any `chain_id`, and assuming no reorg deeper than `finality_lag`:
    - Every block `b` such that `b ≤ last_caught_up_block` has had its ACL/TFHE logs replayed at least once by the poller with the current ingestion code.
    - Newly finalized blocks (`safe_tip` increasing) are processed exactly once (modulo retry on transient failures).
- Limitations
  - Poller will not revisit blocks ≤ `last_caught_up_block` unless `host_listener_poller_state` is manually reset.
  - Historic mistakes prior to `last_caught_up_block` or code upgrades may require a separate backfill tool to re-run ingestion for older ranges.

### 1.7 Observability
- Logging
  - Once per iteration: `chain_id`, `latest`, `safe_tip`, `last_caught_up_block_before`, `last_caught_up_block_after`, `blocks_processed`, `blocks_failed`.
  - Per-block errors: block number, error type (RPC/DB), number of retries.
- Metrics
  - Via telemetry (minimal set):
    - `host_poller_blocks_processed{chain_id}` (incremented by blocks processed successfully).
    - `host_poller_http_retries{chain_id}`.
    - `host_poller_db_errors{chain_id}`.
  - More detailed metrics can be added later if needed.

## 2. Implementation Plan (reusing host-listener code)

### 2.1 DB Migration
1. Add a migration in `db-migration/migrations`:
```sql
CREATE TABLE IF NOT EXISTS host_listener_poller_state (
    chain_id BIGINT PRIMARY KEY,
    last_caught_up_block BIGINT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
```
- No foreign keys.

### 2.2 Shared Ingestion Logic
2. In `host-listener/src/cmd/mod.rs`, ingestion lives in `db_insert_block_no_retry` (decodes ACL/TFHE logs, writes DB, marks block valid).
3. Create `host-listener/src/database/ingest.rs` and move the ingestion body into:
```rust
pub async fn ingest_block_logs(
    db: &mut Database,
    block_logs: &BlockLogs<Log>,
    acl_address: Option<Address>,
    tfhe_address: Option<Address>,
) -> Result<(), sqlx::Error> { ... }
```
- Address matching remains single-address-per-contract (as today); no multi-address support needed.
4. In `cmd/mod.rs`, keep `db_insert_block` as a thin retrying wrapper that calls `ingest_block_logs`, passing through the optional single ACL/TFHE addresses.

### 2.3 Poller State Helpers
5. Add helpers for `host_listener_poller_state` (e.g., `host-listener/src/poller/state.rs`):
```rust
pub async fn get_last_caught_up_block(
    pool: &PgPool,
    chain_id: i64,
) -> Result<Option<i64>, sqlx::Error>;

pub async fn set_last_caught_up_block(
    pool: &PgPool,
    chain_id: i64,
    block: i64,
) -> Result<(), sqlx::Error>;
```
- `pool` can be obtained via `db.pool.read().await.clone()` as in existing code.

### 2.4 HTTP Chain Client
6. Add `host-listener/src/poller/http_client.rs`:
```rust
pub struct HttpChainClient {
    provider: HttpProvider<...>,
    acl_address: Option<Address>,
    tfhe_address: Option<Address>,
    retry_interval: Duration,
}

impl HttpChainClient {
    pub async fn latest_block_number(&self) -> anyhow::Result<u64> { ... }
    pub async fn logs_for_block(&self, block: u64) -> anyhow::Result<Vec<Log>> { ... }
    pub async fn header_for_block(&self, block: u64) -> anyhow::Result<Header> { ... }
}
```
- `logs_for_block` builds a Filter with `fromBlock = block`, `toBlock = block`, and an address list containing the single ACL and/or TFHE address when provided; handles transient errors with a retry loop using `retry_interval`.

### 2.5 Poller Core
7. Add `host-listener/src/poller/mod.rs`:
```rust
pub struct PollerConfig {
    pub rpc_url: String,
    pub acl_address: Option<Address>,
    pub tfhe_address: Option<Address>,
    pub database_url: DatabaseURL,
    pub coprocessor_api_key: Uuid,
    pub finality_lag: u64,
    pub batch_size: u64,
    pub poll_interval: Duration,
    pub retry_interval: Duration,
    pub log_level: Level,
    pub service_name: String,
}

pub async fn run_poller(config: PollerConfig) -> anyhow::Result<()> { ... }
```
- `run_poller` sets up logging/telemetry, builds `HttpChainClient`, initializes `Database`, validates `chain_id`, reads/initializes `last_caught_up_block`, then loops:
  - Compute `latest`, `safe_tip`.
  - If `safe_tip > last_caught_up_block`:
    - `target = min(safe_tip, last_caught_up_block + batch_size)`.
    - For `b` in `(last_caught_up_block, target]` fetch logs/header, build `BlockLogs`, and call `ingest_block_logs`.
    - On success update `host_listener_poller_state` and local `last_caught_up_block`.
  - Sleep `poll_interval`.

### 2.6 Poller Binary
8. Add `host-listener/src/bin/poller.rs`:
```rust
#[derive(Parser, Debug, Clone)]
pub struct Args { ... }

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    // configure tracing_subscriber with args.log_level
    let config = PollerConfig::from(args);
    host_listener::poller::run_poller(config).await
}
```
- `Args` mirrors `PollerConfig` and parses the single ACL/TFHE contract addresses.
9. Update `host-listener/Cargo.toml` to add the new binary target.

### 2.7 Observability Integration
10. In `run_poller`:
- Maintain counters per iteration: `blocks_processed`, `http_retries`, `db_errors`.
- Export via telemetry if available:
  - `host_poller_blocks_processed{chain_id}`
  - `host_poller_http_retries{chain_id}`
  - `host_poller_db_errors{chain_id}`
- Ensure logs contain iteration summaries and error details.

### 2.8 Testing
11. Unit/small tests
- For `get_last_caught_up_block`/`set_last_caught_up_block`.
- For `HttpChainClient` filter construction with optional single ACL/TFHE addresses.
12. Integration tests
- Scenario A: Poller catch-up starting behind the chain head; verify processing up to `safe_tip` fills DB.
- Scenario B: WS gap—stop WS listener for N blocks, run poller, confirm ingestion of missed finalized blocks between previous `last_caught_up_block` and current `safe_tip`.
- Scenario C: Reorg within `finality_lag`—produce a reorg, ensure the poller processes only finalized canonical blocks.
