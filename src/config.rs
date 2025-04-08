// src/config.rs
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use tracing::{info, warn};
use uuid::Uuid;

/// Main configuration for Lodestone
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// General node configuration
    #[serde(default)]
    pub node: NodeSettings,
    
    /// Router configuration
    #[serde(default)]
    pub router: RouterSettings,
    
    /// Discovery configuration
    #[serde(default)]
    pub discovery: DiscoverySettings,
    
    /// Security settings
    #[serde(default)]
    pub security: SecuritySettings,
    
    /// Advanced settings
    #[serde(default)]
    pub advanced: AdvancedSettings,
}

impl Config {
    /// Load configuration from file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(&path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }
    
    /// Save configuration to file
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let content = toml::to_string_pretty(self)?;
        fs::write(path, content)?;
        Ok(())
    }
    
    /// Create a new default configuration
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node: NodeSettings::default(),
            router: RouterSettings::default(),
            discovery: DiscoverySettings::default(),
            security: SecuritySettings::default(),
            advanced: AdvancedSettings::default(),
        }
    }
}

/// Node-specific settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSettings {
    /// Unique identifier for this node
    pub id: Option<String>,
    
    /// Node name for human-readable identification
    pub name: Option<String>,
    
    /// IP address to bind to
    pub bind_ip: Option<String>,
    
    /// Port for the router to listen on
    pub router_port: u16,
    
    /// Port for the API to listen on
    pub api_port: u16,
    
    /// Directory to store data
    pub data_dir: String,
    
    /// Tags for node categorization
    pub tags: Vec<String>,
}

impl Default for NodeSettings {
    fn default() -> Self {
        Self {
            id: None,
            name: None,
            bind_ip: Some("0.0.0.0".to_string()),
            router_port: 8080,
            api_port: 8081,
            data_dir: "./data".to_string(),
            tags: vec![],
        }
    }
}

/// Router configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouterSettings {
    /// Global timeout for requests in milliseconds
    pub global_timeout_ms: u64,
    
    /// Maximum number of connections
    pub max_connections: usize,
    
    /// Enable automatic retries
    pub enable_retries: bool,
    
    /// Default number of retries
    pub default_retry_count: u32,
    
    /// TCP proxy configuration
    pub tcp_proxy: TcpProxySettings,
    
    /// Static routes (path -> upstream)
    pub static_routes: HashMap<String, String>,
}

impl Default for RouterSettings {
    fn default() -> Self {
        Self {
            global_timeout_ms: 30000,
            max_connections: 10000,
            enable_retries: true,
            default_retry_count: 3,
            tcp_proxy: TcpProxySettings::default(),
            static_routes: HashMap::new(),
        }
    }
}

/// TCP proxy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TcpProxySettings {
    /// Enable TCP proxy
    pub enabled: bool,
    
    /// TCP listen port
    pub listen_port: u16,
    
    /// Enable connection pooling
    pub connection_pooling: bool,
    
    /// Maximum idle time in seconds
    pub max_idle_time_secs: u64,
    
    /// Enable UDP proxy
    pub udp_enabled: bool,
    
    /// UDP listen port
    pub udp_listen_port: u16,
}

impl Default for TcpProxySettings {
    fn default() -> Self {
        Self {
            enabled: true,
            listen_port: 9090,
            connection_pooling: true,
            max_idle_time_secs: 60,
            udp_enabled: false,
            udp_listen_port: 9090,
        }
    }
}

/// Service discovery configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoverySettings {
    /// Health check interval in seconds
    pub health_check_interval_secs: u64,
    
    /// Health check timeout in milliseconds
    pub health_check_timeout_ms: u64,
    
    /// Default health check path for HTTP services
    pub default_health_check_path: String,
    
    /// TTL for service registrations in seconds
    pub service_ttl_secs: u64,
    
    /// Deregistration delay in seconds
    pub deregistration_delay_secs: u64,
}

impl Default for DiscoverySettings {
    fn default() -> Self {
        Self {
            health_check_interval_secs: 10,
            health_check_timeout_ms: 2000,
            default_health_check_path: "/health".to_string(),
            service_ttl_secs: 60,
            deregistration_delay_secs: 30,
        }
    }
}

/// Security settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecuritySettings {
    /// Enable TLS
    pub tls_enabled: bool,
    
    /// Path to TLS certificate
    pub tls_cert_path: Option<String>,
    
    /// Path to TLS key
    pub tls_key_path: Option<String>,
    
    /// Require client certificates
    pub require_client_certs: bool,
    
    /// Path to CA certificate
    pub ca_cert_path: Option<String>,
    
    /// Enable API authentication
    pub api_auth_enabled: bool,
    
    /// API access tokens
    pub api_tokens: Vec<String>,
}

impl Default for SecuritySettings {
    fn default() -> Self {
        Self {
            tls_enabled: false,
            tls_cert_path: None,
            tls_key_path: None,
            require_client_certs: false,
            ca_cert_path: None,
            api_auth_enabled: false,
            api_tokens: vec![],
        }
    }
}

/// Advanced settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvancedSettings {
    /// Log level
    pub log_level: String,
    
    /// Enable metrics
    pub metrics_enabled: bool,
    
    /// Metrics port
    pub metrics_port: u16,
    
    /// Enable profiling
    pub profiling_enabled: bool,
    
    /// Profiling port
    pub profiling_port: u16,
}

impl Default for AdvancedSettings {
    fn default() -> Self {
        Self {
            log_level: "info".to_string(),
            metrics_enabled: true,
            metrics_port: 9100,
            profiling_enabled: false,
            profiling_port: 9101,
        }
    }
}

/// Node configuration passed from command line
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Unique identifier for this node
    pub node_id: Option<String>,
    
    /// Bind address for all services
    pub bind_addr: Option<String>,
    
    /// Port for the router
    pub router_port: u16,
    
    /// Port for the API
    pub api_port: u16,
    
    /// Data directory
    pub data_dir: PathBuf,
    
    /// Configuration file path
    pub config_path: String,
}

impl NodeConfig {
    /// Generate a node ID if one was not provided
    pub fn node_id(&self) -> String {
        self.node_id.clone().unwrap_or_else(|| Uuid::new_v4().to_string())
    }
    
    /// Get the bind address
    pub fn bind_addr(&self) -> String {
        self.bind_addr.clone().unwrap_or_else(|| "0.0.0.0".to_string())
    }
    
    /// Load the full configuration, overriding with command line values
    pub fn load_full_config(&self) -> Result<Config> {
        let mut config = if let Ok(cfg) = Config::from_file(&self.config_path) {
            cfg
        } else {
            warn!("Could not load config from {}, using defaults", self.config_path);
            Config::default()
        };
        
        // Override with command line values
        if let Some(node_id) = &self.node_id {
            config.node.id = Some(node_id.clone());
        }
        
        if let Some(bind_addr) = &self.bind_addr {
            config.node.bind_ip = Some(bind_addr.clone());
        }
        
        config.node.router_port = self.router_port;
        config.node.api_port = self.api_port;
        config.node.data_dir = self.data_dir.to_string_lossy().to_string();
        
        Ok(config)
    }
}