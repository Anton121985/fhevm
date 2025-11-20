mod http_client;
mod metrics;
pub mod state;

use std::time::Duration;

use alloy::primitives::Address;
use alloy::rpc::types::Log;
use anyhow::{anyhow, Context, Result};
use sqlx::types::Uuid;
use tokio::time::sleep;
use tracing::{error, info, warn, Level};

use fhevm_engine_common::telemetry;
use fhevm_engine_common::utils::DatabaseURL;

use crate::cmd::block_history::BlockSummary;
use crate::database::ingest::{ingest_block_logs, BlockLogs};
use crate::database::tfhe_event_propagate::Database;
use crate::poller::http_client::HttpChainClient;
use crate::poller::metrics::{
    inc_blocks_processed, inc_db_errors, inc_http_retries,
};
use crate::poller::state::{
    get_last_caught_up_block, set_last_caught_up_block,
};

const DEFAULT_DEPENDENCE_CACHE_SIZE: u16 = 128;
const MAX_DB_RETRIES: u64 = 10;

#[derive(Clone, Debug)]
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

pub async fn run_poller(config: PollerConfig) -> Result<()> {
    if !config.service_name.is_empty() {
        if let Err(err) = telemetry::setup_otlp(&config.service_name) {
            warn!(error = %err, "Failed to setup OTLP");
        }
    }

    let acl_address = config.acl_address;
    let tfhe_address = config.tfhe_address;

    let client = HttpChainClient::new(
        &config.rpc_url,
        acl_address,
        tfhe_address,
        config.retry_interval,
    )
    .await?;

    let (chain_id, http_retries) = client.chain_id().await?;
    let chain_id_str = chain_id.to_string();
    if http_retries > 0 {
        inc_http_retries(&chain_id_str, http_retries);
    }

    let mut db = Database::new(
        &config.database_url,
        &config.coprocessor_api_key,
        DEFAULT_DEPENDENCE_CACHE_SIZE,
    )
    .await?;

    if chain_id != db.chain_id {
        error!(
            chain_id_blockchain = ?chain_id,
            chain_id_db = ?db.chain_id,
            tenant_id = ?db.tenant_id,
            coprocessor_api_key = ?config.coprocessor_api_key,
            "Chain ID mismatch with database",
        );
        return Err(anyhow!(
            "Chain ID mismatch with database, blockchain: {} vs db: {}, tenant_id: {}, coprocessor_api_key: {}",
            chain_id,
            db.chain_id,
            db.tenant_id,
            config.coprocessor_api_key
        ));
    }

    let pool = db.pool.read().await.clone();
    let initial_anchor =
        get_last_caught_up_block(&pool, chain_id as i64).await?;
    let mut last_caught_up_block = match initial_anchor {
        Some(block) => u64::try_from(block)
            .context("last_caught_up_block cannot be negative")?,
        None => {
            let initial = db.read_last_valid_block().await.unwrap_or(0);
            set_last_caught_up_block(&pool, chain_id as i64, initial).await?;
            u64::try_from(initial)
                .context("initial last_caught_up_block cannot be negative")?
        }
    };

    info!(
        chain_id = chain_id,
        last_caught_up_block = last_caught_up_block,
        finality_lag = config.finality_lag,
        batch_size = config.batch_size,
        poll_interval_ms = config.poll_interval.as_millis(),
        retry_interval_ms = config.retry_interval.as_millis(),
        "Starting host-listener poller"
    );

    loop {
        let (latest, latest_retries) = client.latest_block_number().await?;
        let mut http_retries = latest_retries;

        let safe_tip = latest.saturating_sub(config.finality_lag);
        if safe_tip <= last_caught_up_block {
            if http_retries > 0 {
                inc_http_retries(&chain_id_str, http_retries);
            }
            info!(
                chain_id = chain_id,
                latest_block = latest,
                safe_tip = safe_tip,
                last_caught_up_block = last_caught_up_block,
                "No new finalized blocks, sleeping"
            );
            sleep(config.poll_interval).await;
            continue;
        }

        let target = safe_tip
            .min(last_caught_up_block.saturating_add(config.batch_size));
        let blocks_to_process = target - last_caught_up_block;

        let mut processed_blocks = 0;
        let mut db_errors = 0;

        for block in (last_caught_up_block + 1)..=target {
            let (logs, log_retries) = client.logs_for_block(block).await?;
            http_retries += log_retries;
            let (header, header_retries) =
                client.header_for_block(block).await?;
            http_retries += header_retries;

            let summary: BlockSummary = header.into();
            let block_logs = BlockLogs {
                logs,
                summary,
                catchup: true,
            };

            match ingest_with_retry(
                &mut db,
                &block_logs,
                acl_address,
                tfhe_address,
                config.retry_interval,
            )
            .await
            {
                Ok(retries) => {
                    db_errors += retries;
                    processed_blocks += 1;
                }
                Err((err, retries)) => {
                    db_errors += retries;
                    error!(
                        block = block,
                        block_hash = ?block_logs.summary.hash,
                        error = %err,
                        retries = retries,
                        "Failed to ingest block"
                    );
                    break;
                }
            }
        }

        let new_anchor = last_caught_up_block + processed_blocks;
        let blocks_failed = blocks_to_process.saturating_sub(processed_blocks);

        if new_anchor > last_caught_up_block {
            let anchor = i64::try_from(new_anchor)
                .context("last_caught_up_block overflow")?;
            set_last_caught_up_block(&pool, chain_id as i64, anchor).await?;
            last_caught_up_block = new_anchor;
        }

        inc_blocks_processed(&chain_id_str, processed_blocks);
        if http_retries > 0 {
            inc_http_retries(&chain_id_str, http_retries);
        }
        if db_errors > 0 {
            inc_db_errors(&chain_id_str, db_errors);
        }

        info!(
            chain_id = chain_id,
            latest_block = latest,
            safe_tip = safe_tip,
            last_caught_up_block_before = new_anchor - processed_blocks,
            last_caught_up_block_after = last_caught_up_block,
            blocks_processed = processed_blocks,
            blocks_failed = blocks_failed,
            http_retries = http_retries,
            db_errors = db_errors,
            "Poller iteration complete"
        );

        sleep(config.poll_interval).await;
    }
}

async fn ingest_with_retry(
    db: &mut Database,
    block_logs: &BlockLogs<Log>,
    acl_address: Option<Address>,
    tfhe_address: Option<Address>,
    retry_interval: Duration,
) -> Result<u64, (sqlx::Error, u64)> {
    let mut errors = 0;
    loop {
        match ingest_block_logs(db, block_logs, acl_address, tfhe_address).await
        {
            Ok(_) => return Ok(errors),
            Err(err) => {
                errors += 1;
                if errors > MAX_DB_RETRIES {
                    return Err((err, errors));
                }
                warn!(
                    block = ?block_logs.summary.number,
                    retries = errors,
                    error = %err,
                    "Retrying block ingestion"
                );
                db.reconnect().await;
                sleep(retry_interval).await;
            }
        }
    }
}
