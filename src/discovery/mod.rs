use async_trait::async_trait;
use futures::future::select;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::error;
use uuid::Uuid;
use std::collections::HashMap;
use std::str::EncodeUtf16;
use std::sync::Arc;
use tokio::sync::RwLock;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Service {
    pub id: String,
    pub name: String,
    pub address: String,
    pub port: u16,
    pub metadata: HashMap<String, String>,
    pub health_check_path: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Error)]
pub enum DiscoveryError {
    #[error("Service not found: {0}")]
    ServiceNotFound(String),
    #[error("Service already exists: {0}")]
    ServiceAlreadyExists(String),
    #[error("Storage error: {0}")]
    StorageError(String),
}

#[async_trait]
pub trait ServiceDiscovery: Send + Sync {
    async fn register(&self, service: Service) -> Result<Service, DiscoveryError>;
    async fn deregister(&self, id: &str) -> Result<(), DiscoveryError>;
    async fn get_service(&self, id: &str) -> Result<Service, DiscoveryError>;
    async fn list_services(&self) -> Result<Vec<Service>, DiscoveryError>;
    async fn list_services_by_name(&self, name: &str) -> Result<Vec<Service>, DiscoveryError>;
}

pub struct InMemoryServiceDiscovery {
    services: Arc<RwLock<HashMap<String, Service>>>
}

impl InMemoryServiceDiscovery {
    pub fn new() -> Self {
        Self {
            services: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl ServiceDiscovery for InMemoryServiceDiscovery {
    async fn register(&self, mut service: Service) -> Result<Service, DiscoveryError> {
        let mut services = self.services.write().await;
        
        if service.id.is_empty() {
            service.id = Uuid::new_v4().to_string();
        }
        
        let now = chrono::Utc::now();
        service.created_at = now;
        service.updated_at = now;
        
        services.insert(service.id.clone(), service.clone());
        Ok(service)
    }

    async fn deregister(&self, id: &str) -> Result<(), DiscoveryError> {
        let mut services = self.services.write().await;
        
        if services.remove(id).is_none() {
            return Err(DiscoveryError::ServiceNotFound(id.to_string()));
        }
        
        Ok(())
    }

    async fn get_service(&self, id: &str) -> Result<Service, DiscoveryError> {
        let services = self.services.read().await;
        
        services.get(id)
            .cloned()
            .ok_or_else(|| DiscoveryError::ServiceNotFound(id.to_string()))
    }

    async fn list_services(&self) -> Result<Vec<Service>, DiscoveryError> {
        let services = self.services.read().await;
        Ok(services.values().cloned().collect())
    }

    async fn list_services_by_name(&self, name: &str) -> Result<Vec<Service>, DiscoveryError> {
        let services = self.services.read().await;
        Ok(services.values()
            .filter(|s| s.name == name)
            .cloned()
            .collect())
    }
}
