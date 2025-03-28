use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use axum::serve;
use tokio::net::TcpListener;
use slog::{Logger, Drain};

mod config;
mod consensus;
mod discovery_old;
mod discovery;
mod router;
mod security;
mod store;
mod prelude;
mod health;
mod service;
mod error;

use crate::config::Settings;
use crate::consensus::RaftNode;
use crate::discovery_old::ServiceRegistry;
use crate::router::Router;
use crate::security::TlsConfig;
use crate::store::Store;
use crate::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Display startup banner
    println!(r#"
                  @@@@       
                @@@@@@@@@   
              @@@@@@@@@@@@ 
            @@@@@@@@@@@@@@@
           @@@@@@@@@@@@@@@@@
          @@@@@@@@@@@@@@@@@@@
         @@@@@@@@@@@@@@@@@@@@@@
       @@@@@@@@@@@@@@@@@@@@@@@@@
     @@@@@@@@@@@@@@@@@@@@@@@@@@@@          ██╗      ██████╗ ██████╗ ███████╗███████╗████████╗ ██████╗ ███╗   ██╗███████╗
   @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@         ██║     ██╔═══██╗██╔══██╗██╔════╝██╔════╝╚══██╔══╝██╔═══██╗████╗  ██║██╔════╝
  @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@         ██║     ██║   ██║██║  ██║█████╗  ███████╗   ██║   ██║   ██║██╔██╗ ██║█████╗  
 @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@         ██║     ██║   ██║██║  ██║██╔══╝  ╚════██║   ██║   ██║   ██║██║╚██╗██║██╔══╝  
  @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@       ███████╗╚██████╔╝██████╔╝███████╗███████║   ██║   ╚██████╔╝██║ ╚████║███████╗
   @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@         ╚══════╝ ╚═════╝ ╚═════╝ ╚══════╝╚══════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═══╝╚══════╝
    @@@@@@@@@@@@@@@@@@@@@@@@@@@@@                      High-Performance Service Discovery and Routing System
       @@@@@@@@@@@@@@@@@@@@@@@@                                           Version: 0.1.0
        @@@@@@@@@@@@@@@@@@@@@@
           @@@@@@@@@@@@@@@@@ 
             @@@@@@@@@@@@@@
    "#);

    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Initialize slog logger
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = Logger::root(drain, slog::o!());

    // Load configuration
    let settings = Settings::new()?;

    // Initialize the storage layer
    let store = Arc::new(Store::new("data")?);
    
    // Initialize Raft consensus
    let raft_node = Arc::new(RwLock::new(RaftNode::new(
        settings.raft.node_id,
        settings.raft.peers.clone(),
        store.clone(),
        logger.clone(),
    )?));
    
    // Initialize the service registry
    let registry = Arc::new(RwLock::new(ServiceRegistry::new(store.clone())));
    
    // Initialize TLS
    let tls_config = TlsConfig::new(
        &settings.security.cert_path,
        &settings.security.key_path,
    )?;
    
    // Initialize the router with all features
    let app = Router::new(registry.clone());

    // Start Raft ticker
    let raft_clone = raft_node.clone();
    let settings_clone = settings.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(settings_clone.raft_heartbeat_interval());
        loop {
            interval.tick().await;
            let mut _node = raft_clone.write().await;
        }
    });

    // Start the HTTP server
    let addr = SocketAddr::new(
        settings.server.host,
        settings.server.port,
    );
    
    tracing::info!("Starting server on {}", addr);
    
    // Create TcpListener
    let listener = TcpListener::bind(addr).await?;
    let _acceptor = tls_config.get_acceptor();
    
    serve(
        listener,
        app.into_make_service()
    )
    .await?;

    Ok(())
}