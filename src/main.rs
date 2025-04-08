// src/main.rs
use anyhow::Result;
use clap::{App, Arg, SubCommand};
use std::path::PathBuf;
use tokio::sync::Mutex;
use std::sync::Arc;
use tracing::{info, warn, error};

fn main() -> Result<()> {
    tokio::runtime::Runtime::new()?.block_on(async {
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
    @@@@@@@@@@@@@@@@@@@@@@@@@@@                      High-Performance Service Discovery and Routing System
       @@@@@@@@@@@@@@@@@@@@@@@@                                           Version: 0.1.1
        @@@@@@@@@@@@@@@@@@@@@@
           @@@@@@@@@@@@@@@@@ 
             @@@@@@@@@@@@@@
    "#);

    // Initialize tracing with better formatting
    tracing_subscriber::fmt()
        .with_env_filter("info,lodestone=debug")
        .init();

    // Parse command line arguments
    let matches = App::new("Lodestone")
        .version("0.1.1")
        .author("Lodestone Team")
        .about("High-Performance Service Discovery and Routing System")
        .arg(
            Arg::with_name("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file")
                .default_value("lodestone.toml")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("node-id")
                .long("node-id")
                .value_name("ID")
                .help("Unique identifier for this node")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("bind-addr")
                .long("bind-addr")
                .value_name("ADDR")
                .help("Address to bind for API traffic")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("router-port")
                .long("router-port")
                .value_name("PORT")
                .help("Port for the router to listen on")
                .default_value("8080")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("api-port")
                .long("api-port")
                .value_name("PORT")
                .help("Port for the API to listen on")
                .default_value("8081")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("data-dir")
                .long("data-dir")
                .value_name("DIR")
                .help("Directory to store data")
                .default_value("./data")
                .takes_value(true),
        )
        .subcommand(
            SubCommand::with_name("service")
                .about("Service management commands")
                .subcommand(
                    SubCommand::with_name("register")
                        .about("Register a new service")
                        .arg(
                            Arg::with_name("name")
                                .required(true)
                                .help("Service name"),)
                                .arg(
                                    Arg::with_name("address")
                                        .required(true)
                                        .help("Service address (host:port)"),
                                )
                                .arg(
                                    Arg::with_name("tags")
                                        .multiple(true)
                                        .help("Tags for service categorization"),
                                )
                        )
                        .subcommand(
                            SubCommand::with_name("deregister")
                                .about("Deregister a service")
                                .arg(
                                    Arg::with_name("id")
                                        .required(true)
                                        .help("Service ID to deregister"),
                                )
                        )
                )
                .get_matches();
        
            // Load configuration
            let config_path = matches.value_of("config").unwrap();
            info!("Loading configuration from {}", config_path);
            
            let node_config = Lodestone::config::NodeConfig {
                node_id: matches.value_of("node-id").map(|s| s.to_string()),
                bind_addr: matches.value_of("bind-addr").map(|s| s.to_string()),
                router_port: matches.value_of("router-port").unwrap().parse()?,
                api_port: matches.value_of("api-port").unwrap().parse()?,
                data_dir: PathBuf::from(matches.value_of("data-dir").unwrap()),
                config_path: config_path.to_string(),
            };
        
            // Handle subcommands
            if let Some(service_matches) = matches.subcommand_matches("service") {
                if let Some(register_matches) = service_matches.subcommand_matches("register") {
                    // Handle service registration via CLI
                    let name = register_matches.value_of("name").unwrap();
                    let address = register_matches.value_of("address").unwrap();
                    let tags = register_matches
                        .values_of("tags")
                        .map(|v| v.map(|s| s.to_string()).collect())
                        .unwrap_or_else(Vec::new);
        
                    info!("Registering service {} at {} with tags: {:?}", name, address, tags);
                    
                    // Create client and register service
                    let client = Lodestone::client::LodestoneClient::new(&format!(
                        "http://{}:{}", 
                        node_config.bind_addr().as_str(), 
                        node_config.api_port
                    ));
                    
                    let result = client.register_service(name, address, tags).await?;
                    println!("Service registered with ID: {}", result);
                    return Ok(());
                } else if let Some(deregister_matches) = service_matches.subcommand_matches("deregister") {
                    // Handle service deregistration via CLI
                    let id = deregister_matches.value_of("id").unwrap();
                    info!("Deregistering service with ID: {}", id);
                    
                    // Create client and deregister service
                    let client = Lodestone::client::LodestoneClient::new(&format!(
                        "http://{}:{}", 
                        node_config.bind_addr().as_str(), 
                        node_config.api_port
                    ));
                    
                    client.deregister_service(id).await?;
                    println!("Service deregistered successfully");
                    return Ok(());
                }
            }
        
            // Load full configuration
            let config = node_config.load_full_config()?;
        
            // Create and configure store
            let store = Lodestone::store::create_store(&node_config.data_dir).await?;
        
            // Create service registry
            let registry = Lodestone::discovery::ServiceRegistry::new(
                store.clone(), 
                config.discovery.clone(), 
                node_config.node_id()
            );
        
            // Create router
            let router = Arc::new(Mutex::new(Lodestone::router::create_router(
                &format!("{}:{}", node_config.bind_addr(), config.node.router_port),
                config.router.global_timeout_ms,
                config.router.max_connections
            ).await?));

            // Create API server
            let mut api_server = Lodestone::api::ApiServer::new(
                &format!("{}:{}", node_config.bind_addr(), config.node.api_port),
                registry.clone().into(),
                router
            );
        
            // Start services
            registry.start().await?;
            api_server.start().await?;
        
            // Wait for Ctrl+C
            tokio::signal::ctrl_c().await?;
            info!("Shutdown signal received, gracefully stopping node...");
            
            // Graceful shutdown
            api_server.shutdown().await?;
            registry.stop().await?;
            
            info!("Node shutdown complete");
            Ok(())
            })
        }