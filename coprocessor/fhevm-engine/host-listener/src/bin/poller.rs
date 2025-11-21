use std::str::FromStr;
use std::time::Duration;

use alloy::primitives::Address;
use anyhow::{anyhow, Context};
use clap::Parser;
use sqlx::types::Uuid;
use tokio_util::sync::CancellationToken;
use tracing::Level;

use fhevm_engine_common::metrics_server;
use fhevm_engine_common::utils::DatabaseURL;
use host_listener::poller::{run_poller, PollerConfig};

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, help = "HTTP JSON-RPC endpoint for the L1 node")]
    rpc_url: String,

    #[arg(long, help = "ACL contract address to monitor")]
    acl_contract_address: Option<String>,

    #[arg(long, help = "TFHE contract address to monitor")]
    tfhe_contract_address: Option<String>,

    #[arg(
        long,
        default_value = "postgresql://postgres:postgres@localhost:5432/coprocessor",
        help = "PostgreSQL connection URL"
    )]
    database_url: DatabaseURL,

    #[arg(long, help = "Coprocessor API key")]
    coprocessor_api_key: Uuid,

    #[arg(
        long,
        default_value_t = 15,
        help = "Depth behind the head considered final (in blocks)"
    )]
    finality_lag: u64,

    #[arg(
        long,
        default_value_t = 100,
        help = "Maximum number of blocks to process per iteration"
    )]
    batch_size: u64,

    #[arg(
        long,
        default_value_t = 1000,
        help = "Sleep duration between iterations in milliseconds"
    )]
    poll_interval_ms: u64,

    #[arg(
        long,
        default_value_t = 1000,
        help = "Backoff between retry attempts for RPC/DB failures in milliseconds"
    )]
    retry_interval_ms: u64,

    #[arg(
        long,
        default_value_t = 10,
        help = "Maximum number of HTTP/RPC retry attempts (in addition to the initial attempt) before failing an operation"
    )]
    max_http_retries: u64,

    #[arg(
        long,
        help = "Address for Prometheus metrics HTTP server (e.g. 0.0.0.0:9100); if unset, metrics server is disabled"
    )]
    metrics_addr: Option<String>,

    #[arg(
        long,
        value_parser = clap::value_parser!(Level),
        default_value_t = Level::INFO
    )]
    log_level: Level,

    #[arg(long, default_value = "host-listener-poller")]
    service_name: String,
}

fn parse_address(
    value: &Option<String>,
    label: &str,
) -> anyhow::Result<Option<Address>> {
    match value {
        Some(address) => {
            Address::from_str(address).map(Some).with_context(|| {
                anyhow!("Invalid {label} contract address: {address}")
            })
        }
        None => Ok(None),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .json()
        .with_level(true)
        .with_max_level(args.log_level)
        .init();

    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let acl_address = parse_address(&args.acl_contract_address, "acl")?;
    let tfhe_address = parse_address(&args.tfhe_contract_address, "tfhe")?;

    let cancel_token = CancellationToken::new();
    metrics_server::spawn(
        args.metrics_addr.clone(),
        cancel_token.child_token(),
    );

    let config = PollerConfig {
        rpc_url: args.rpc_url,
        acl_address,
        tfhe_address,
        database_url: args.database_url,
        coprocessor_api_key: args.coprocessor_api_key,
        finality_lag: args.finality_lag,
        batch_size: args.batch_size,
        poll_interval: Duration::from_millis(args.poll_interval_ms),
        retry_interval: Duration::from_millis(args.retry_interval_ms),
        service_name: args.service_name,
        max_http_retries: args.max_http_retries,
    };

    run_poller(config).await
}
