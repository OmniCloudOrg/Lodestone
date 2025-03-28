mod api;
mod config;
mod discovery;
mod router;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{Router, Server};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use crate::api::services::services_routes;
use crate::config::Config;
use crate::discovery::InMemoryServiceDiscovery;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    
    // Load configuration
    let config = match Config::from_file("config/default.toml") {
        Ok(config) => config,
        Err(e) => {
            info!("Failed to load config, using defaults: {}", e);
            Config::default()
        }
    };
    
    // Initialize service discovery
    let discovery = Arc::new(InMemoryServiceDiscovery::new());
    
    // Build application router
    let app = Router::new()
        .merge(services_routes(discovery.clone()))
        .route("/health", axum::routing::get(|| async { "OK" }));
    
    // Start the server
    let addr = SocketAddr::from(([0, 0, 0, 0], config.server.port));
    info!("Starting server on {}", addr);
    
    Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;
    
    Ok(())
}
