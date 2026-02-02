use std::sync::Arc;
use std::time::Duration;
use tokio::time;

use crate::api::AppState;
use crate::db::DbPool;

/// Background worker that marks stale servers as unhealthy
pub async fn run_health_checker(state: Arc<AppState>) {
    let mut interval = time::interval(Duration::from_secs(5));

    loop {
        interval.tick().await;

        match mark_stale_servers(&state.pool).await {
            Ok(count) if count > 0 => {
                tracing::info!("Marked {} servers as unhealthy", count);
            }
            Err(e) => {
                tracing::error!("Health checker error: {}", e);
            }
            _ => {}
        }
    }
}

async fn mark_stale_servers(pool: &DbPool) -> Result<u64, sqlx::Error> {
    crate::db::mark_stale_servers_unhealthy(pool).await
}
