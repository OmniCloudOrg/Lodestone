use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Parse error: {0}")]
    ParseError(#[from] serde_json5::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    pub jwt_secret: String,
    pub cert_path: String,
    pub key_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    pub node_id: u64,
    pub peers: Vec<u64>,
    pub election_timeout: u64,
    pub heartbeat_interval: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub security: SecurityConfig,
    pub raft: RaftConfig,
}

impl Config {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        
        let config = serde_json5::from_str(&contents)?;
        Ok(config)
    }
    
    pub fn default() -> Self {
        Self {
            server: ServerConfig {
                host: "127.0.0.1".to_string(),
                port: 8080,
            },
            security: SecurityConfig {
                jwt_secret: "default-secret-change-me".to_string(),
                cert_path: "certs/server.crt".to_string(),
                key_path: "certs/server.key".to_string(),
            },
            raft: RaftConfig {
                node_id: 1,
                peers: vec![2, 3],
                election_timeout: 1000,
                heartbeat_interval: 100,
            },
        }
    }
}
