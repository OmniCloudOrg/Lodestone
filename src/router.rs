// src/router.rs
use anyhow::Result;

// Re-export Router from harbr_router
pub type Router = harbr_router::Router;

// Provide a convenience function to create a router
pub async fn create_router(
    listen_addr: &str,
    global_timeout_ms: u64,
    max_connections: usize,
) -> Result<Router> {
    // Create router configuration
    let router_config = harbr_router::ProxyConfig::new(
        listen_addr,
        global_timeout_ms,
        max_connections,
    );
    
    // Create the router
    let router = Router::new(router_config);
    
    Ok(router)
}