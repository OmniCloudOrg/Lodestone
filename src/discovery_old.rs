use crate::health::{HealthCheck, HealthStatus};
// src/discovery/mod.rs
use crate::prelude::*;
use crate::service::Service;
use crate::store::Store;
use reqwest;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{self, Duration};

pub struct ServiceRegistry {
    store: Arc<Store>,
    health_checks: RwLock<Vec<String>>,
}

impl ServiceRegistry {
    pub fn new(store: Arc<Store>) -> Self {
        let registry = Self {
            store,
            health_checks: RwLock::new(Vec::new()),
        };

        // Spawn health check task
        tokio::spawn(registry.clone().run_health_checks());

        registry
    }

    pub async fn register(&self, service: Service) -> Result<()> {
        self.store.set(&service.id, &service)?;

        let mut health_checks = self.health_checks.write().await;
        health_checks.push(service.id.clone());

        Ok(())
    }

    pub async fn deregister(&self, service_id: &str) -> Result<()> {
        self.store.delete(service_id)?;

        let mut health_checks = self.health_checks.write().await;
        if let Some(pos) = health_checks.iter().position(|id| id == service_id) {
            health_checks.remove(pos);
        }

        Ok(())
    }

    pub async fn get_service(&self, service_id: &str) -> Result<Option<Service>> {
        self.store.get(service_id)
    }

    pub async fn list_services(&self) -> Result<Vec<Service>> {
        self.store.list()
    }

    pub async fn get_services_by_name(&self, name: &str) -> Result<Vec<Service>> {
        self.store.scan_prefix(&format!("service:{}", name))
    }

    async fn run_health_checks(self) {
        let mut interval = time::interval(Duration::from_secs(10));

        loop {
            interval.tick().await;

            let health_checks = self.health_checks.read().await;
            for service_id in health_checks.iter() {
                if let Ok(Some(service)) = self.store.get(service_id) {
                    let health = self.check_health(&service).await;
                    if health.status == HealthStatus::Unhealthy {
                        tracing::warn!(
                            "Service {} failed health check: {:?}",
                            service_id,
                            health.message
                        );
                    }
                }
            }
        }
    }

    async fn check_health(&self, service: &Service) -> HealthCheck {
        match reqwest::get(&service.health_check_url).await {
            Ok(response) => HealthCheck {
                status: if response.status().is_success() {
                    HealthStatus::Healthy
                } else {
                    HealthStatus::Unhealthy
                },
                message: None,
                timestamp: chrono::Utc::now(),
            },
            Err(e) => HealthCheck {
                status: HealthStatus::Unhealthy,
                message: Some(e.to_string()),
                timestamp: chrono::Utc::now(),
            },
        }
    }
}

impl Clone for ServiceRegistry {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            health_checks: RwLock::new(Vec::new()),
        }
    }
}
