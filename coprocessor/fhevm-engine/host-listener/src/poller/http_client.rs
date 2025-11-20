use std::time::Duration;

use alloy::eips::BlockId;
use alloy::primitives::Address;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::{Filter, Header, Log};
use anyhow::{Context, Result};
use reqwest::Url;
use tokio::time::sleep;
use tracing::warn;

use fhevm_engine_common::types::BlockchainProvider;

pub struct HttpChainClient {
    provider: BlockchainProvider,
    addresses: Vec<Address>,
    retry_interval: Duration,
}

impl HttpChainClient {
    pub async fn new(
        rpc_url: &str,
        acl_address: Option<Address>,
        tfhe_address: Option<Address>,
        retry_interval: Duration,
    ) -> Result<Self> {
        let url = Url::parse(rpc_url)
            .context("Invalid rpc_url provided to poller HTTP client")?;
        let provider = ProviderBuilder::new().connect_http(url);

        let mut addresses = Vec::new();
        if let Some(address) = acl_address {
            addresses.push(address);
        }
        if let Some(address) = tfhe_address {
            addresses.push(address);
        }

        Ok(Self {
            provider,
            addresses,
            retry_interval,
        })
    }

    pub async fn chain_id(&self) -> Result<(u64, u64)> {
        let mut retries = 0;
        loop {
            match self.provider.get_chain_id().await {
                Ok(chain_id) => return Ok((chain_id, retries)),
                Err(err) => {
                    retries += 1;
                    warn!(
                        error = %err,
                        retries = retries,
                        "Failed to fetch chain id, retrying"
                    );
                }
            }
            sleep(self.retry_interval).await;
        }
    }

    pub async fn latest_block_number(&self) -> Result<(u64, u64)> {
        let mut retries = 0;
        loop {
            match self.provider.get_block_number().await {
                Ok(latest) => return Ok((latest, retries)),
                Err(err) => {
                    retries += 1;
                    warn!(
                        error = %err,
                        retries = retries,
                        "Failed to fetch latest block number, retrying"
                    );
                }
            }
            sleep(self.retry_interval).await;
        }
    }

    pub async fn logs_for_block(&self, block: u64) -> Result<(Vec<Log>, u64)> {
        let mut retries = 0;
        let filter = Self::build_filter(block, &self.addresses);
        loop {
            match self.provider.get_logs(&filter).await {
                Ok(logs) => return Ok((logs, retries)),
                Err(err) => {
                    retries += 1;
                    warn!(
                        block = block,
                        error = %err,
                        retries = retries,
                        "Failed to fetch logs for block, retrying"
                    );
                }
            }
            sleep(self.retry_interval).await;
        }
    }

    pub async fn header_for_block(&self, block: u64) -> Result<(Header, u64)> {
        let mut retries = 0;
        let block_id = BlockId::number(block);
        loop {
            match self.provider.get_block(block_id).await {
                Ok(Some(block)) => return Ok((block.header, retries)),
                Ok(None) => {
                    retries += 1;
                    warn!(
                        block = block,
                        retries = retries,
                        "Block not found, retrying"
                    );
                }
                Err(err) => {
                    retries += 1;
                    warn!(
                        block = block,
                        error = %err,
                        retries = retries,
                        "Failed to fetch block header, retrying"
                    );
                }
            }
            sleep(self.retry_interval).await;
        }
    }

    pub(crate) fn build_filter(block: u64, addresses: &[Address]) -> Filter {
        let mut filter = Filter::new().from_block(block).to_block(block);
        if !addresses.is_empty() {
            filter = filter.address(addresses.to_vec());
        }
        filter
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn filter_builder_sets_addresses_and_block_bounds() {
        let addr1 = Address::from([1u8; 20]);
        let addr2 = Address::from([2u8; 20]);

        let filter = HttpChainClient::build_filter(42, &[addr1, addr2]);
        let serialized = serde_json::to_value(filter).unwrap();

        let from_block = serialized
            .get("fromBlock")
            .and_then(Value::as_str)
            .expect("fromBlock missing");
        let to_block = serialized
            .get("toBlock")
            .and_then(Value::as_str)
            .expect("toBlock missing");

        let from_block_num =
            u64::from_str_radix(from_block.trim_start_matches("0x"), 16)
                .unwrap();
        let to_block_num =
            u64::from_str_radix(to_block.trim_start_matches("0x"), 16).unwrap();
        assert_eq!(from_block_num, 42);
        assert_eq!(to_block_num, 42);

        let mut addresses: Vec<Address> =
            serde_json::from_value(serialized.get("address").cloned().unwrap())
                .unwrap();
        addresses.sort();
        let mut expected = vec![addr1, addr2];
        expected.sort();
        assert_eq!(addresses, expected);
    }

    #[test]
    fn filter_builder_skips_addresses_when_empty() {
        let filter = HttpChainClient::build_filter(1, &[]);
        let serialized = serde_json::to_value(filter).unwrap();
        assert!(serialized.get("address").is_none());
    }
}
