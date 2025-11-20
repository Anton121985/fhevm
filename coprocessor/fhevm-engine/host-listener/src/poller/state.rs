use sqlx::{PgPool, Row};

pub async fn get_last_caught_up_block(
    pool: &PgPool,
    chain_id: i64,
) -> Result<Option<i64>, sqlx::Error> {
    let row = sqlx::query(
        r#"
        SELECT last_caught_up_block
        FROM host_listener_poller_state
        WHERE chain_id = $1
        "#,
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| r.get::<i64, _>("last_caught_up_block")))
}

pub async fn set_last_caught_up_block(
    pool: &PgPool,
    chain_id: i64,
    block: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO host_listener_poller_state (chain_id, last_caught_up_block)
        VALUES ($1, $2)
        ON CONFLICT (chain_id) DO UPDATE
        SET last_caught_up_block = EXCLUDED.last_caught_up_block,
            updated_at = NOW()
        "#,
    )
    .bind(chain_id)
    .bind(block)
    .execute(pool)
    .await?;

    Ok(())
}
