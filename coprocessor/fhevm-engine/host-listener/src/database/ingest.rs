use std::collections::HashSet;

use alloy::primitives::Address;
use alloy::rpc::types::Log;
use alloy::sol_types::SolEventInterface;
use fhevm_engine_common::types::Handle;
use tracing::{error, info};

use crate::cmd::block_history::BlockSummary;
use crate::contracts::{AclContract, TfheContract};
use crate::database::tfhe_event_propagate::{
    acl_result_handles, tfhe_result_handle, Database, LogTfhe,
};

pub struct BlockLogs<T> {
    pub logs: Vec<T>,
    pub summary: BlockSummary,
    pub catchup: bool,
}

pub async fn ingest_block_logs(
    db: &mut Database,
    block_logs: &BlockLogs<Log>,
    acl_address: Option<Address>,
    tfhe_address: Option<Address>,
) -> Result<(), sqlx::Error> {
    let mut tx = db.new_transaction().await?;
    let mut is_allowed = HashSet::<Handle>::new();
    let mut tfhe_event_log = vec![];

    for log in &block_logs.logs {
        let is_acl_address = acl_address
            .map(|addr| addr == log.inner.address)
            .unwrap_or(false);
        if acl_address.is_none() || is_acl_address {
            if let Ok(event) =
                AclContract::AclContractEvents::decode_log(&log.inner)
            {
                info!(acl_event = ?event, "ACL event");
                for handle in acl_result_handles(&event) {
                    is_allowed.insert(handle.to_vec());
                }
                db.handle_acl_event(
                    &mut tx,
                    &event,
                    &log.transaction_hash,
                    &log.block_number,
                )
                .await?;
                continue;
            }
        }

        let is_tfhe_address = tfhe_address
            .map(|addr| addr == log.inner.address)
            .unwrap_or(false);
        if tfhe_address.is_none() || is_tfhe_address {
            if let Ok(event) =
                TfheContract::TfheContractEvents::decode_log(&log.inner)
            {
                let log = LogTfhe {
                    event,
                    transaction_hash: log.transaction_hash,
                    is_allowed: false, // updated in the next loop
                    block_number: log.block_number,
                };
                tfhe_event_log.push(log);
                continue;
            }
        }

        if is_acl_address || is_tfhe_address {
            error!(
                event_address = ?log.inner.address,
                acl_address = ?acl_address,
                tfhe_address = ?tfhe_address,
                log = ?log,
                "Cannot decode event",
            );
        }
    }

    for tfhe_log in tfhe_event_log {
        info!(tfhe_log = ?tfhe_log, "TFHE event");
        let is_allowed =
            if let Some(result_handle) = tfhe_result_handle(&tfhe_log.event) {
                is_allowed.contains(&result_handle.to_vec())
            } else {
                false
            };
        let tfhe_log = LogTfhe {
            is_allowed,
            ..tfhe_log
        };
        db.insert_tfhe_event(&mut tx, &tfhe_log).await?;
    }

    db.mark_block_as_valid(&mut tx, &block_logs.summary).await?;
    tx.commit().await
}
