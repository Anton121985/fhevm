use crate::database::tfhe_event_propagate::Database;

pub async fn get_last_caught_up_block(
    db: &Database,
    chain_id: i64,
) -> Result<Option<i64>, sqlx::Error> {
    db.get_last_caught_up_block(chain_id).await
}

pub async fn set_last_caught_up_block(
    db: &Database,
    chain_id: i64,
    block: i64,
) -> Result<(), sqlx::Error> {
    db.set_last_caught_up_block(chain_id, block).await
}
