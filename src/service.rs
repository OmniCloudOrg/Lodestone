// src/service.rs
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use uuid::Uuid;

/// Health status of a service instance
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServiceHealth {
    /// Service is healthy and available
    Healthy,
    
    /// Service is unhealthy but still registered
    Unhealthy,
    
    /// Service is in an unknown state
    Unknown,
    
    /// Service is being monitored but not ready
    Starting,
    
    /// Service is intentionally offline for maintenance
    Maintenance,
    
    /// Service has been marked for deregistration
    Deregistering,
}

impl Default for ServiceHealth {
    fn default() -> Self {
        ServiceHealth::Unknown
    }
}

impl std::fmt::Display for ServiceHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServiceHealth::Healthy => write!(f, "healthy"),
            ServiceHealth::Unhealthy => write!(f, "unhealthy"),
            ServiceHealth::Unknown => write!(f, "unknown"),
            ServiceHealth::Starting => write!(f, "starting"),
            ServiceHealth::Maintenance => write!(f, "maintenance"),
            ServiceHealth::Deregistering => write!(f, "deregistering"),
        }
    }
}

/// A registered service
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Service {
    /// Unique service name
    pub name: String,
    
    /// Service instances
    pub instances: Vec<ServiceInstance>,
    
    /// Service metadata
    pub metadata: HashMap<String, String>,
    
    /// Service creation time
    pub created_at: DateTime<Utc>,
    
    /// Last updated time
    pub updated_at: DateTime<Utc>,
}

impl Service {
    /// Create a new service
    pub fn new(name: &str) -> Self {
        let now = Utc::now();
        Self {
            name: name.to_string(),
            instances: Vec::new(),
            metadata: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }
    
    /// Add an instance to this service
    pub fn add_instance(&mut self, instance: ServiceInstance) {
        self.instances.push(instance);
        self.updated_at = Utc::now();
    }
    
    /// Remove an instance by ID
    pub fn remove_instance(&mut self, instance_id: &str) -> Option<ServiceInstance> {
        let position = self.instances.iter().position(|i| i.id == instance_id)?;
        let instance = self.instances.remove(position);
        self.updated_at = Utc::now();
        Some(instance)
    }
    
    /// Get an instance by ID
    pub fn get_instance(&self, instance_id: &str) -> Option<&ServiceInstance> {
        self.instances.iter().find(|i| i.id == instance_id)
    }
    
    /// Get a mutable reference to an instance by ID
    pub fn get_instance_mut(&mut self, instance_id: &str) -> Option<&mut ServiceInstance> {
        self.instances.iter_mut().find(|i| i.id == instance_id)
    }
    
    /// Count the number of healthy instances
    pub fn healthy_instance_count(&self) -> usize {
        self.instances.iter().filter(|i| i.health == ServiceHealth::Healthy).count()
    }
    
    /// Get all healthy instances
    pub fn healthy_instances(&self) -> Vec<&ServiceInstance> {
        self.instances.iter().filter(|i| i.health == ServiceHealth::Healthy).collect()
    }
    
    /// Check if the service has any healthy instances
    pub fn is_available(&self) -> bool {
        self.instances.iter().any(|i| i.health == ServiceHealth::Healthy)
    }
    
    /// Update the metadata
    pub fn update_metadata(&mut self, key: &str, value: &str) {
        self.metadata.insert(key.to_string(), value.to_string());
        self.updated_at = Utc::now();
    }
    
    /// Set multiple metadata values at once
    pub fn set_metadata(&mut self, metadata: HashMap<String, String>) {
        self.metadata = metadata;
        self.updated_at = Utc::now();
    }
}

/// A specific instance of a service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInstance {
    /// Unique ID for this instance
    pub id: String,
    
    /// Service name this instance belongs to
    pub service_name: String,
    
    /// Host address (IP or hostname)
    pub host: String,
    
    /// Port number
    pub port: u16,
    
    /// Protocol (http, https, tcp, udp)
    pub protocol: String,
    
    /// Health check path (for HTTP/HTTPS)
    pub health_check_path: Option<String>,
    
    /// Current health status
    pub health: ServiceHealth,
    
    /// Tags for categorization
    pub tags: Vec<String>,
    
    /// Instance-specific metadata
    pub metadata: HashMap<String, String>,
    
    /// Time to live in seconds before deregistration
    pub ttl: Option<u64>,
    
    /// Last heartbeat time
    pub last_heartbeat: Option<DateTime<Utc>>,
    
    /// Creation time
    pub created_at: DateTime<Utc>,
    
    /// Last updated time
    pub updated_at: DateTime<Utc>,
    
    /// Node ID hosting this instance
    pub node_id: String,
    
    /// Weight for load balancing (higher is more traffic)
    pub weight: u32,
}

impl ServiceInstance {
    /// Create a new service instance
    pub fn new(
        service_name: &str,
        host: &str,
        port: u16,
        protocol: &str,
        node_id: &str,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4().to_string(),
            service_name: service_name.to_string(),
            host: host.to_string(),
            port,
            protocol: protocol.to_string(),
            health_check_path: None,
            health: ServiceHealth::Unknown,
            tags: Vec::new(),
            metadata: HashMap::new(),
            ttl: None,
            last_heartbeat: Some(now),
            created_at: now,
            updated_at: now,
            node_id: node_id.to_string(),
            weight: 100,
        }
    }
    
    /// Get the full address as a string
    pub fn address(&self) -> String {
        format!("{}://{}:{}", self.protocol, self.host, self.port)
    }
    
    /// Get the socket address
    pub fn socket_addr(&self) -> Option<SocketAddr> {
        format!("{}:{}", self.host, self.port).parse().ok()
    }
    
    /// Update the health status
    pub fn update_health(&mut self, health: ServiceHealth) {
        self.health = health;
        self.updated_at = Utc::now();
    }
    
    /// Record a heartbeat
    pub fn heartbeat(&mut self) {
        self.last_heartbeat = Some(Utc::now());
    }
    
    /// Check if the TTL has expired
    pub fn is_expired(&self) -> bool {
        match (self.ttl, self.last_heartbeat) {
            (Some(ttl), Some(last_heartbeat)) => {
                let duration = Utc::now() - last_heartbeat;
                let ttl_duration = chrono::Duration::from_std(Duration::from_secs(ttl)).unwrap();
                duration > ttl_duration
            }
            _ => false,
        }
    }
    
    /// Set the TTL
    pub fn set_ttl(&mut self, ttl_secs: u64) {
        self.ttl = Some(ttl_secs);
        self.updated_at = Utc::now();
    }
    
    /// Add a tag
    pub fn add_tag(&mut self, tag: &str) {
        if !self.tags.contains(&tag.to_string()) {
            self.tags.push(tag.to_string());
            self.updated_at = Utc::now();
        }
    }
    
    /// Remove a tag
    pub fn remove_tag(&mut self, tag: &str) -> bool {
        let len = self.tags.len();
        self.tags.retain(|t| t != tag);
        let removed = self.tags.len() < len;
        if removed {
            self.updated_at = Utc::now();
        }
        removed
    }
    
    /// Set health check path
    pub fn set_health_check_path(&mut self, path: &str) {
        self.health_check_path = Some(path.to_string());
        self.updated_at = Utc::now();
    }
    
    /// Update the instance weight
    pub fn set_weight(&mut self, weight: u32) {
        self.weight = weight;
        self.updated_at = Utc::now();
    }
    
    /// Update the metadata
    pub fn update_metadata(&mut self, key: &str, value: &str) {
        self.metadata.insert(key.to_string(), value.to_string());
        self.updated_at = Utc::now();
    }
    
    /// Set multiple metadata values at once
    pub fn set_metadata(&mut self, metadata: HashMap<String, String>) {
        self.metadata = metadata;
        self.updated_at = Utc::now();
    }
}