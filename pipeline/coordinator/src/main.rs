mod admin_ui;
mod api;
mod db;
mod health_checker;

use axum::{
    Router,
    routing::{get, post},
};
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use api::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env.local if present (for local development)
    dotenvy::from_filename(".env.local").ok();

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("coordinator=info".parse()?)
                .add_directive("tower_http=debug".parse()?),
        )
        .init();

    // Get configuration from environment
    let database_url = std::env::var("PIPELINE_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "sqlite:pipeline.db?mode=rwc".to_string());
    let http_port = std::env::var("HTTP_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080u16);

    // Create database pool
    tracing::info!("Connecting to database...");
    let pool = db::create_pool(&database_url).await?;
    tracing::info!("Database connected");

    // Create shared state
    let state = Arc::new(AppState { pool });

    // Spawn health checker background task
    let health_state = state.clone();
    tokio::spawn(async move {
        health_checker::run_health_checker(health_state).await;
    });

    // Build router
    let app = Router::new()
        // Admin UI
        .route("/", get(admin_ui::admin_dashboard))
        // Admin API
        .route("/admin/workers", get(api::list_workers))
        .route("/admin/distributions", get(api::list_distributions).post(api::create_distribution))
        .route("/admin/distributions/{id}", get(api::get_distribution_details))
        .route("/admin/distributions/{id}/cancel", post(api::cancel_distribution))
        .route("/admin/distributions/{id}/purge", post(api::purge_distribution))
        // Worker API
        .route("/worker/check-in", post(api::check_in))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state);

    // Start server
    let addr = format!("0.0.0.0:{}", http_port);
    tracing::info!("Starting coordinator on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
