use sqlx::postgres::PgPoolOptions;

use host_listener::poller::state::{
    get_last_caught_up_block, set_last_caught_up_block,
};
use test_harness::instance::ImportMode;

#[tokio::test]
async fn poller_state_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let allow_db = std::env::var("COPROCESSOR_TEST_LOCALHOST").is_ok()
        || std::env::var("COPROCESSOR_TEST_LOCALHOST_RESET").is_ok()
        || std::env::var("COPROCESSOR_TEST_WITH_DOCKER").is_ok();

    if !allow_db {
        eprintln!(
            "skipping poller_state_round_trip: set COPROCESSOR_TEST_LOCALHOST \
             or COPROCESSOR_TEST_WITH_DOCKER to run"
        );
        return Ok(());
    }

    let db_instance =
        test_harness::instance::setup_test_db(ImportMode::WithKeysNoSns)
            .await?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(db_instance.db_url())
        .await?;

    let chain_id: i64 = 12345;
    sqlx::query("DELETE FROM host_listener_poller_state WHERE chain_id = $1")
        .bind(chain_id)
        .execute(&pool)
        .await?;

    assert_eq!(get_last_caught_up_block(&pool, chain_id).await?, None);

    set_last_caught_up_block(&pool, chain_id, 5).await?;
    assert_eq!(get_last_caught_up_block(&pool, chain_id).await?, Some(5));

    set_last_caught_up_block(&pool, chain_id, 7).await?;
    assert_eq!(get_last_caught_up_block(&pool, chain_id).await?, Some(7));

    Ok(())
}
