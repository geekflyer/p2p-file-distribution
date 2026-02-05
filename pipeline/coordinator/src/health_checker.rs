use std::sync::Arc;
use std::time::Duration;
use tokio::time;

use crate::api::AppState;
use crate::db::DbPool;

/// Background worker that marks stale workers as unhealthy
pub async fn run_health_checker(state: Arc<AppState>) {
    let mut interval = time::interval(Duration::from_secs(5));

    loop {
        interval.tick().await;

        match mark_stale_workers(&state.pool).await {
            Ok(count) if count > 0 => {
                tracing::info!("Marked {} workers as unhealthy", count);
            }
            Err(e) => {
                tracing::error!("Health checker error: {}", e);
            }
            _ => {}
        }
    }
}

async fn mark_stale_workers(pool: &DbPool) -> Result<u64, sqlx::Error> {
    crate::db::mark_stale_workers_unhealthy(pool).await
}
