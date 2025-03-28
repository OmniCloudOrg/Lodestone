use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use async_trait::async_trait;
use thiserror::Error;

use crate::discovery::{Service, ServiceDiscovery, DiscoveryError};

#[derive(Debug, Error)]
pub enum LoadBalancerError {
    #[error("No services available for: {0}")]
    NoServicesAvailable(String),
    #[error("Discovery error: {0}")]
    DiscoveryError(#[from] DiscoveryError),
}

#[async_trait]
pub trait LoadBalancer: Send + Sync {
    async fn next_service(&self, service_name: &str) -> Result<Service, LoadBalancerError>;
}

pub struct RoundRobinLoadBalancer<T: ServiceDiscovery> {
    discovery: Arc<T>,
    counters: dashmap::DashMap<String, AtomicUsize>,
}

impl<T: ServiceDiscovery> RoundRobinLoadBalancer<T> {
    pub fn new(discovery: Arc<T>) -> Self {
        Self {
            discovery,
            counters: dashmap::DashMap::new(),
        }
    }
}

#[async_trait]
impl<T: ServiceDiscovery> LoadBalancer for RoundRobinLoadBalancer<T> {
    async fn next_service(&self, service_name: &str) -> Result<Service, LoadBalancerError> {
        let services = self.discovery.list_services_by_name(service_name).await?;
        
        if services.is_empty() {
            return Err(LoadBalancerError::NoServicesAvailable(service_name.to_string()));
        }
        
        let counter = self.counters
            .entry(service_name.to_string())
            .or_insert_with(|| AtomicUsize::new(0));
        
        let index = counter.fetch_add(1, Ordering::SeqCst) % services.len();
        Ok(services[index].clone())
    }
}
