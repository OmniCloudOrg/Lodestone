// src/discovery.rs
use crate::service::{Service, ServiceInstance, ServiceHealth};
use crate::config::DiscoverySettings;
use crate::store::Store;
use anyhow::{Result, Context};
use chrono::Utc;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};
use tokio::time::{interval, Duration};
use tracing::{debug, info, warn, error};
use uuid::Uuid;

/// Type of registry event
#[derive(Debug, Clone)]
pub enum RegistryEvent {
    /// Service registered
    ServiceRegistered {
        service_name: String,
        instance_id: String,
    },
    /// Service instance deregistered
    ServiceDeregistered {
        service_name: String,
        instance_id: String,
    },
    /// Service instance health changed
    ServiceHealthChanged {
        service_name: String,
        instance_id: String,
        old_health: ServiceHealth,
        new_health: ServiceHealth,
    },
    /// Service updates (metadata, etc.)
    ServiceUpdated {
        service_name: String,
    },
}

/// Service registry for managing service discovery
pub struct ServiceRegistry {
    /// Store for persisting registry state
    store: Arc<Box<dyn Store>>,
    
    /// Services cache
    services: RwLock<HashMap<String, Service>>,
    
    /// Registry settings
    settings: DiscoverySettings,
    
    /// Event sender
    event_tx: broadcast::Sender<RegistryEvent>,
    
    /// Node ID running this registry
    node_id: String,
    
    /// Registry is active flag
    active: RwLock<bool>,
}

impl ServiceRegistry {
    /// Create a new service registry
    pub fn new(
        store: Arc<Box<dyn Store>>,
        settings: DiscoverySettings,
        node_id: String,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(100);
        
        Self {
            store,
            services: RwLock::new(HashMap::new()),
            settings,
            event_tx,
            node_id,
            active: RwLock::new(false),
        }
    }
    
    /// Start the registry
    pub async fn start(&self) -> Result<()> {
        // Load services from store
        self.load_services().await?;
        
        // Mark as active
        let mut active = self.active.write().await;
        *active = true;
        drop(active);
        
        // Start health checker
        self.start_health_checker();
        
        // Start TTL checker
        self.start_ttl_checker();
        
        info!("Service registry started");
        Ok(())
    }
    
    /// Stop the registry
    pub async fn stop(&self) -> Result<()> {
        // Mark as inactive
        let mut active = self.active.write().await;
        *active = false;
        
        // Save services to store
        self.save_services().await?;
        
        info!("Service registry stopped");
        Ok(())
    }
    
    /// Register a new service instance
    pub async fn register_service(
        &self,
        service_name: &str,
        host: &str,
        port: u16,
        protocol: &str,
        tags: Vec<String>,
        health_check_path: Option<String>,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<String> {
        // Create service instance
        let mut instance = ServiceInstance::new(
            service_name,
            host,
            port,
            protocol,
            &self.node_id,
        );
        
        // Set tags
        instance.tags = tags;
        
        // Set health check path
        if let Some(path) = health_check_path {
            instance.health_check_path = Some(path);
        } else if protocol == "http" || protocol == "https" {
            instance.health_check_path = Some(self.settings.default_health_check_path.clone());
        }
        
        // Set metadata
        if let Some(meta) = metadata {
            instance.metadata = meta;
        }
        
        // Set TTL
        instance.set_ttl(self.settings.service_ttl_secs);
        
        // Get instance ID
        let instance_id = instance.id.clone();
        
        // Update services
        let mut services = self.services.write().await;
        
        // Get or create service
        let service = services
            .entry(service_name.to_string())
            .or_insert_with(|| Service::new(service_name));
        
        // Add instance to service
        service.add_instance(instance);
        
        // Save to store
        self.save_service(service_name, service).await?;
        
        // Notify of registration
        let _ = self.event_tx.send(RegistryEvent::ServiceRegistered {
            service_name: service_name.to_string(),
            instance_id: instance_id.clone(),
        });
        
        info!("Registered service instance {} for service {}", instance_id, service_name);
        
        Ok(instance_id)
    }
    
    /// Deregister a service instance
    pub async fn deregister_service(&self, service_name: &str, instance_id: &str) -> Result<()> {
        let mut services = self.services.write().await;
        
        // Find service
        if let Some(service) = services.get_mut(service_name) {
            // Remove instance
            if let Some(instance) = service.remove_instance(instance_id) {
                // Save to store
                drop(services); // Release lock before async call
                self.save_services().await?;
                
                // Notify of deregistration
                let _ = self.event_tx.send(RegistryEvent::ServiceDeregistered {
                    service_name: service_name.to_string(),
                    instance_id: instance_id.to_string(),
                });
                
                info!("Deregistered service instance {} for service {}", instance_id, service_name);
                
                return Ok(());
            }
        }
        
        Err(anyhow::anyhow!("Service instance not found"))
    }
    
    /// Get a service by name
    pub async fn get_service(&self, service_name: &str) -> Option<Service> {
        let services = self.services.read().await;
        services.get(service_name).cloned()
    }
    
    /// Get a service instance
    pub async fn get_instance(&self, service_name: &str, instance_id: &str) -> Option<ServiceInstance> {
        let service = self.get_service(service_name).await?;
        service.get_instance(instance_id).cloned()
    }
    
    /// Get all services
    pub async fn get_services(&self) -> HashMap<String, Service> {
        let services = self.services.read().await;
        services.clone()
    }
    
    /// Get all instances for a service
    pub async fn get_instances(&self, service_name: &str) -> Vec<ServiceInstance> {
        if let Some(service) = self.get_service(service_name).await {
            service.instances
        } else {
            Vec::new()
        }
    }
    
    /// Get healthy instances for a service
    pub async fn get_healthy_instances(&self, service_name: &str) -> Vec<ServiceInstance> {
        if let Some(service) = self.get_service(service_name).await {
            service.instances.into_iter()
                .filter(|i| i.health == ServiceHealth::Healthy)
                .collect()
        } else {
            Vec::new()
        }
    }
    
    /// Update service instance health
    pub async fn update_instance_health(
        &self,
        service_name: &str,
        instance_id: &str,
        health: ServiceHealth,
    ) -> Result<()> {
        let mut services = self.services.write().await;
        
        // Find service
        if let Some(service) = services.get_mut(service_name) {
            // Find instance
            if let Some(instance) = service.get_instance_mut(instance_id) {
                let old_health = instance.health;
                
                // Update health if changed
                if old_health != health {
                    instance.update_health(health);
                    
                    // Save to store
                    drop(services); // Release lock before async call
                    self.save_services().await?;
                    
                    // Notify of health change
                    let _ = self.event_tx.send(RegistryEvent::ServiceHealthChanged {
                        service_name: service_name.to_string(),
                        instance_id: instance_id.to_string(),
                        old_health,
                        new_health: health,
                    });
                    
                    debug!(
                        "Updated service instance {} health for service {} from {:?} to {:?}",
                        instance_id, service_name, old_health, health
                    );
                }
                
                return Ok(());
            }
        }
        
        Err(anyhow::anyhow!("Service instance not found"))
    }
    
    /// Record a heartbeat for an instance
    pub async fn heartbeat(&self, service_name: &str, instance_id: &str) -> Result<()> {
        let mut services = self.services.write().await;
        
        // Find service
        if let Some(service) = services.get_mut(service_name) {
            // Find instance
            if let Some(instance) = service.get_instance_mut(instance_id) {
                // Record heartbeat
                instance.heartbeat();
                
                // If instance was unhealthy, mark as healthy
                if instance.health != ServiceHealth::Healthy {
                    drop(services); // Release lock before recursive call
                    self.update_instance_health(service_name, instance_id, ServiceHealth::Healthy).await?;
                }
                
                return Ok(());
            }
        }
        
        Err(anyhow::anyhow!("Service instance not found"))
    }
    
    /// Subscribe to registry events
    pub fn subscribe(&self) -> broadcast::Receiver<RegistryEvent> {
        self.event_tx.subscribe()
    }
    
    /// Load services from store
    async fn load_services(&self) -> Result<()> {
        let services_data = self.store.get("services").await?;
        
        if let Some(data) = services_data {
            let services: HashMap<String, Service> = serde_json::from_slice(&data)
                .context("Failed to deserialize services from store")?;
            
            let mut services_lock = self.services.write().await;
            *services_lock = services;
            
            info!("Loaded {} services from store", services_lock.len());
        } else {
            info!("No services found in store");
        }
        
        Ok(())
    }
    
    /// Save all services to store
    async fn save_services(&self) -> Result<()> {
        let services = self.services.read().await;
        let data = serde_json::to_vec(&*services)
            .context("Failed to serialize services for store")?;
        
        self.store.set("services", &data).await?;
        debug!("Saved {} services to store", services.len());
        
        Ok(())
    }
    
    /// Save a specific service to store
    async fn save_service(&self, service_name: &str, service: &Service) -> Result<()> {
        let data = serde_json::to_vec(service)
            .context("Failed to serialize service for store")?;
        
        self.store.set(&format!("service:{}", service_name), &data).await?;
        debug!("Saved service {} to store", service_name);
        
        Ok(())
    }
    
    /// Start the health checker
    fn start_health_checker(&self) {
        let registry = self.clone();
        let interval_secs = self.settings.health_check_interval_secs;
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(interval_secs));
            
            loop {
                interval.tick().await;
                
                // Check if registry is still active
                let active = *registry.active.read().await;
                if !active {
                    break;
                }
                
                // Perform health checks
                if let Err(e) = registry.check_service_health().await {
                    error!("Error performing health checks: {}", e);
                }
            }
        });
    }
    
    /// Start the TTL checker
    fn start_ttl_checker(&self) {
        let registry = self.clone();
        let check_interval = Duration::from_secs(self.settings.service_ttl_secs / 2);
        
        tokio::spawn(async move {
            let mut interval = interval(check_interval);
            
            loop {
                interval.tick().await;
                
                // Check if registry is still active
                let active = *registry.active.read().await;
                if !active {
                    break;
                }
                
                // Check for expired instances
                if let Err(e) = registry.check_expired_instances().await {
                    error!("Error checking expired instances: {}", e);
                }
            }
        });
    }
    
    /// Perform health checks on all instances
    async fn check_service_health(&self) -> Result<()> {
        let services = self.get_services().await;
        
        for (service_name, service) in services {
            for instance in service.instances {
                let service_name = service_name.clone();
                let instance_id = instance.id.clone();
                let registry = self.clone();
                
                tokio::spawn(async move {
                    // Check instance health asynchronously
                    match registry.check_instance_health(&instance).await {
                        Ok(health) => {
                            // Update health status if needed
                            if instance.health != health {
                                if let Err(e) = registry.update_instance_health(&service_name, &instance_id, health).await {
                                    error!("Failed to update health for {}/{}: {}", service_name, instance_id, e);
                                }
                            }
                        }
                        Err(e) => {
                            error!("Health check failed for {}/{}: {}", service_name, instance_id, e);
                            
                            // Mark as unhealthy on error
                            if instance.health == ServiceHealth::Healthy {
                                if let Err(e) = registry.update_instance_health(&service_name, &instance_id, ServiceHealth::Unhealthy).await {
                                    error!("Failed to mark unhealthy for {}/{}: {}", service_name, instance_id, e);
                                }
                            }
                        }
                    }
                });
            }
        }
        
        Ok(())
    }
    
    /// Check an individual instance's health
    async fn check_instance_health(&self, instance: &ServiceInstance) -> Result<ServiceHealth> {
        // Skip instances in certain states
        match instance.health {
            ServiceHealth::Deregistering | ServiceHealth::Maintenance => {
                return Ok(instance.health);
            }
            _ => {}
        }
        
        // Different health check based on protocol
        match instance.protocol.as_str() {
            "http" | "https" => {
                self.check_http_health(instance).await
            }
            "tcp" => {
                self.check_tcp_health(instance).await
            }
            _ => {
                // For other protocols, just trust the current health status
                Ok(instance.health)
            }
        }
    }
    
    /// Check HTTP health for an instance
    async fn check_http_health(&self, instance: &ServiceInstance) -> Result<ServiceHealth> {
        // Get health check path
        let health_path = instance.health_check_path.as_deref()
            .unwrap_or(&self.settings.default_health_check_path);
        
        // Build health check URL
        let url = format!("{}://{}:{}{}", 
            instance.protocol, 
            instance.host, 
            instance.port, 
            health_path
        );
        
        // Create HTTP client with timeout
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(self.settings.health_check_timeout_ms))
            .build()?;
        
        // Perform the health check
        let response = client.get(&url).send().await?;
        
        // Check status code
        if response.status().is_success() {
            Ok(ServiceHealth::Healthy)
        } else {
            Ok(ServiceHealth::Unhealthy)
        }
    }
    
    /// Check TCP health for an instance
    async fn check_tcp_health(&self, instance: &ServiceInstance) -> Result<ServiceHealth> {
        // Try to connect to the instance
        let addr = format!("{}:{}", instance.host, instance.port);
        let timeout = Duration::from_millis(self.settings.health_check_timeout_ms);
        
        match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(&addr)).await {
            Ok(Ok(_)) => Ok(ServiceHealth::Healthy),
            _ => Ok(ServiceHealth::Unhealthy),
        }
    }
    
    /// Check for expired TTLs
    async fn check_expired_instances(&self) -> Result<()> {
        let services = self.get_services().await;
        let mut to_deregister = Vec::new();
        
        // Collect instances to deregister
        for (service_name, service) in services {
            for instance in service.instances {
                if instance.is_expired() {
                    to_deregister.push((service_name.clone(), instance.id.clone()));
                }
            }
        }
        
        // Deregister expired instances
        for (service_name, instance_id) in to_deregister {
            warn!("Deregistering expired instance {} for service {}", instance_id, service_name);
            
            if let Err(e) = self.deregister_service(&service_name, &instance_id).await {
                error!("Failed to deregister expired instance {}/{}: {}", service_name, instance_id, e);
            }
        }
        
        Ok(())
    }
}

impl Clone for ServiceRegistry {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            services: RwLock::new(HashMap::new()), // Empty services map in clone
            settings: self.settings.clone(),
            event_tx: self.event_tx.clone(),
            node_id: self.node_id.clone(),
            active: RwLock::new(false), // Clones are not active by default
        }
    }
}