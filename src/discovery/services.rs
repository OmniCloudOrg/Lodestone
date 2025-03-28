use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
    Router,
    routing::{get, post, delete},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::discovery::{Service, ServiceDiscovery, DiscoveryError};

#[derive(Debug, Deserialize)]
pub struct ServiceRegistration {
    pub name: String,
    pub address: String,
    pub port: u16,
    pub metadata: Option<std::collections::HashMap<String, String>>,
    pub health_check_path: Option<String>,
}

pub fn services_routes<T: ServiceDiscovery + 'static>(discovery: Arc<T>) -> Router {
    Router::new()
        .route("/services", get(list_services::<T>))
        .route("/services", post(register_service::<T>))
        .route("/services/:id", get(get_service::<T>))
        .route("/services/:id", delete(deregister_service::<T>))
        .with_state(discovery)
}

async fn register_service<T: ServiceDiscovery>(
    State(discovery): State<Arc<T>>,
    Json(registration): Json<ServiceRegistration>,
) -> Result<Json<Service>, (StatusCode, String)> {
    let service = Service {
        id: String::new(), // Will be generated in the register method
        name: registration.name,
        address: registration.address,
        port: registration.port,
        metadata: registration.metadata.unwrap_or_default(),
        health_check_path: registration.health_check_path,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };

    match discovery.register(service).await {
        Ok(service) => Ok(Json(service)),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn list_services<T: ServiceDiscovery>(
    State(discovery): State<Arc<T>>,
) -> Result<Json<Vec<Service>>, (StatusCode, String)> {
    match discovery.list_services().await {
        Ok(services) => Ok(Json(services)),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn get_service<T: ServiceDiscovery>(
    State(discovery): State<Arc<T>>,
    Path(id): Path<String>,
) -> Result<Json<Service>, (StatusCode, String)> {
    match discovery.get_service(&id).await {
        Ok(service) => Ok(Json(service)),
        Err(DiscoveryError::ServiceNotFound(_)) => {
            Err((StatusCode::NOT_FOUND, format!("Service not found: {}", id)))
        }
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn deregister_service<T: ServiceDiscovery>(
    State(discovery): State<Arc<T>>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    match discovery.deregister(&id).await {
        Ok(_) => Ok(StatusCode::NO_CONTENT),
        Err(DiscoveryError::ServiceNotFound(_)) => {
            Err((StatusCode::NOT_FOUND, format!("Service not found: {}", id)))
        }
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}
