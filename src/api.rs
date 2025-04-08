// src/api.rs
use crate::discovery::ServiceRegistry;
use crate::service::ServiceHealth;
use crate::router::Router;
use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use warp::{Filter, Reply, Rejection};
use warp::filters::BoxedFilter;
use tracing::info;

/// API server for handling HTTP requests
#[derive(Clone)]
pub struct ApiServer {
    /// Bind address
    bind_addr: String,
    
    /// Service registry
    service_registry: Arc<ServiceRegistry>,
    
    /// Router
    router: Arc<tokio::sync::Mutex<Router>>,
    
    /// Raft manager (optional)
//    raft_manager: Option<RaftManager>,
    
    /// Shutdown channel
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl ApiServer {
    /// Create a new API server
    pub fn new(
        bind_addr: &str,
        service_registry: Arc<ServiceRegistry>,
        router: Arc<tokio::sync::Mutex<Router>>,
        // raft_manager: Option<RaftManager>,
    ) -> Self {
        Self {
            bind_addr: bind_addr.to_string(),
            service_registry,
            router,
            // raft_manager,
            shutdown_tx: None,
        }
    }
    
    /// Start the API server
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting API server on {}", self.bind_addr);
        
        // Set up routes
        let api_routes = self.setup_routes();
        
        // Create shutdown channel
        let (tx, mut rx) = mpsc::channel::<()>(1);
        self.shutdown_tx = Some(tx);
        
        // Parse bind address
        let socket_addr: SocketAddr = self.bind_addr.parse()
            .context("Invalid bind address")?;
        
        // Start server
        let (_, server) = warp::serve(api_routes)
            .bind_with_graceful_shutdown(socket_addr, async move {
                let _ = rx.recv().await;
                info!("API server shutdown signal received");
            });
        
        // Spawn server
        tokio::spawn(server);
        
        info!("API server started");
        
        Ok(())
    }
    
    /// Shutdown the API server
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down API server");
        
        // Send shutdown signal
        if let Some(tx) = &self.shutdown_tx {
            let _ = tx.send(()).await;
        }
        
        Ok(())
    }
    
    /// Set up all API routes
    fn setup_routes(&self) -> BoxedFilter<(impl Reply,)> {
        // Clone required services
        let service_registry = self.service_registry.clone();
        let router = Arc::clone(&self.router);
        // let raft_manager = self.raft_manager.clone();
        
        // Health check endpoint
        let health = warp::path!("health")
            .and(warp::get())
            .map(|| {
                warp::reply::json(&HealthResponse {
                    status: "ok".to_string(),
                    message: "Service is healthy".to_string(),
                })
            });
        
        // Setup service registry routes
        let service_routes = self.setup_service_routes(service_registry.clone());
        
        // Setup router routes
        let router_routes = self.setup_router_routes(&router);

        // Setup metrics endpoint
        let metrics = warp::path!("metrics")
            .and(warp::get())
            .map(|| {
                // This would integrate with a metrics system in a real implementation
                warp::reply::with_status(
                    "# Metrics would be here",
                    warp::http::StatusCode::OK,
                )
            });
        
        // Combine all routes
        health
            .or(service_routes)
            .or(router_routes)
    //        .or(cluster_routes)
    //        .or(raft_routes)
            .or(metrics)
            .recover(handle_rejection)
            .with(warp::log("api"))
            .boxed()
    }
    
    /// Setup service registry routes
    fn setup_service_routes(&self, registry: Arc<ServiceRegistry>) -> BoxedFilter<(impl Reply,)> {
        // List all services
        let list_services = warp::path!("services")
            .and(warp::get())
            .and(with_registry(registry.clone()))
            .and_then(handle_list_services);
        
        // Get service details
        let get_service = warp::path!("services" / String)
            .and(warp::get())
            .and(with_registry(registry.clone()))
            .and_then(handle_get_service);
        
        // Register a service
        let register_service = warp::path!("services")
            .and(warp::post())
            .and(warp::body::json())
            .and(with_registry(registry.clone()))
            .and_then(handle_register_service);
        
        // Deregister a service
        let deregister_service = warp::path!("services" / String / String)
            .and(warp::delete())
            .and(with_registry(registry.clone()))
            .and_then(handle_deregister_service);
        
        // Update service health
        let update_health = warp::path!("services" / String / String / "health")
            .and(warp::put())
            .and(warp::body::json())
            .and(with_registry(registry.clone()))
            .and_then(handle_update_health);
        
        // Record service heartbeat
        let heartbeat = warp::path!("services" / String / String / "heartbeat")
            .and(warp::post())
            .and(with_registry(registry.clone()))
            .and_then(handle_heartbeat);
        
        // Combine service routes
        warp::path("v1").and(
            list_services
                .or(get_service)
                .or(register_service)
                .or(deregister_service)
                .or(update_health)
                .or(heartbeat)
        ).boxed()
    }
    
    /// Setup router routes
    fn setup_router_routes(&self, router: &Arc<tokio::sync::Mutex<Router>>) -> BoxedFilter<(impl Reply,)> {
        // Clone service registry
        let service_registry = self.service_registry.clone();

        // Get all routes
        let get_routes = warp::path!("routes")
            .and(warp::get())
            .and(with_router(router.clone()))
            .and_then(handle_get_routes)
            .boxed();
        
        // Get a specific route
        let get_route = warp::path!("routes" / String)
            .and(warp::get())
            .and(with_router(router.clone()))
            .and_then(handle_get_route)
            .boxed();
        
        // Create or update a route
        let upsert_route = warp::path!("routes" / String)
            .and(warp::put())
            .and(warp::body::json())
            .and(with_router(router.clone()))
            .and(with_registry(service_registry.clone()))
            .and_then(handle_upsert_route);
        
        // Delete a route
        let delete_route = warp::path!("routes" / String)
            .and(warp::delete())
            .and(with_router(router.clone()))
            .and_then(handle_delete_route);
        
        // Update TCP config
        let update_tcp = warp::path!("router" / "tcp")
            .and(warp::put())
            .and(warp::body::json())
            .and(with_router(router.clone()))
            .and_then(handle_update_tcp);
        
        // Update global router settings
        let update_settings = warp::path!("router" / "settings")
            .and(warp::put())
            .and(warp::body::json())
            .and(with_router(router.clone()))
            .and_then(handle_update_settings);
        
        // Combine router routes
        warp::path("v1").and(
            get_routes
                .or(get_route)
                .or(upsert_route)
                .or(delete_route)
                .or(update_tcp)
                .or(update_settings)
        ).boxed()
    }
}

// Helper function to inject the service registry into route handlers
fn with_registry(registry: Arc<ServiceRegistry>) -> impl Filter<Extract = (Arc<ServiceRegistry>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || registry.clone())
}

// Helper function to inject the router into route handlers
fn with_router(router: Arc<tokio::sync::Mutex<Router>>) -> impl Filter<Extract = (Arc<tokio::sync::Mutex<Router>>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || router.clone())
}

/// Service registration request
#[derive(Debug, Deserialize)]
struct RegisterServiceRequest {
    /// Service name
    name: String,
    
    /// Host address
    host: String,
    
    /// Port number
    port: u16,
    
    /// Protocol (http, https, tcp, udp)
    protocol: String,
    
    /// Health check path (for HTTP/HTTPS)
    health_check_path: Option<String>,
    
    /// Tags for categorization
    tags: Option<Vec<String>>,
    
    /// Instance-specific metadata
    metadata: Option<HashMap<String, String>>,
    
    /// Time to live in seconds before deregistration
    ttl: Option<u64>,
}

/// Service health update request
#[derive(Debug, Deserialize)]
struct HealthUpdateRequest {
    /// New health status
    health: String,
}

/// TCP configuration request
#[derive(Debug, Deserialize)]
struct TcpConfigRequest {
    /// Enable TCP proxy
    enabled: Option<bool>,
    
    /// TCP listen address
    listen_addr: Option<String>,
    
    /// Enable connection pooling
    connection_pooling: Option<bool>,
    
    /// Maximum idle time in seconds
    max_idle_time_secs: Option<u64>,
    
    /// Enable UDP proxy
    udp_enabled: Option<bool>,
    
    /// UDP listen address
    udp_listen_addr: Option<String>,
}

/// Global router settings request
#[derive(Debug, Deserialize)]
struct RouterSettingsRequest {
    /// HTTP listen address
    listen_addr: Option<String>,
    
    /// Global timeout in milliseconds
    global_timeout_ms: Option<u64>,
    
    /// Maximum number of connections
    max_connections: Option<usize>,
}

/// Node addition request
#[derive(Debug, Deserialize)]
struct AddNodeRequest {
/// Node ID
node_id: u64,
    
/// Node address
address: String,
}

/// Standard API response
#[derive(Debug, Serialize)]
struct ApiResponse<T> {
/// Success flag
success: bool,

/// Response message
message: String,

/// Response data
#[serde(skip_serializing_if = "Option::is_none")]
data: Option<T>,
}

/// Health check response
#[derive(Debug, Serialize)]
struct HealthResponse {
/// Health status
status: String,

/// Health message
message: String,
}

/// Create a success response
fn success<T>(message: &str, data: Option<T>) -> ApiResponse<T> {
ApiResponse {
    success: true,
    message: message.to_string(),
    data,
}
}

/// Create an error response
fn error<T>(message: &str) -> ApiResponse<T> {
ApiResponse {
    success: false,
    message: message.to_string(),
    data: None,
}
}

/// Handle errors
async fn handle_rejection(err: Rejection) -> Result<impl Reply, Rejection> {
let message = if err.is_not_found() {
    "Not found".to_string()
} else if let Some(e) = err.find::<warp::filters::body::BodyDeserializeError>() {
    format!("Invalid request data: {}", e)
} else {
    "Internal server error".to_string()
};

let json = warp::reply::json(&error::<()>(&message));
let status = if err.is_not_found() {
    warp::http::StatusCode::NOT_FOUND
} else if err.find::<warp::filters::body::BodyDeserializeError>().is_some() {
    warp::http::StatusCode::BAD_REQUEST
} else {
    warp::http::StatusCode::INTERNAL_SERVER_ERROR
};

Ok(warp::reply::with_status(json, status))
}

/// Handle listing all services
async fn handle_list_services(
registry: Arc<ServiceRegistry>,
) -> Result<impl Reply, Rejection> {
let services = registry.get_services().await;
Ok(warp::reply::with_status(
    warp::reply::json(&success("Services retrieved", Some(services))),
    warp::http::StatusCode::OK
))
}

/// Handle getting a specific service
async fn handle_get_service(
name: String,
registry: Arc<ServiceRegistry>,
) -> Result<impl Reply, Rejection> {
match registry.get_service(&name).await {
    Some(service) => Ok(warp::reply::with_status(
        warp::reply::json(&success("Service retrieved", Some(service))),
        warp::http::StatusCode::OK
    )),
    None => Ok(warp::reply::with_status(
        warp::reply::json(&error::<()>(&format!("Service not found: {}", name))),
        warp::http::StatusCode::NOT_FOUND
    ))
}
}

/// Handle registering a service
async fn handle_register_service(
request: RegisterServiceRequest,
registry: Arc<ServiceRegistry>,
) -> Result<impl Reply, Rejection> {
// Convert tags and metadata
let tags = request.tags.unwrap_or_default();
let metadata = request.metadata.unwrap_or_default();

// Register service
match registry.register_service(
    &request.name,
    &request.host,
    request.port,
    &request.protocol,
    tags,
    request.health_check_path,
    Some(metadata),
).await {
    Ok(instance_id) => {
        let response_data = HashMap::from([
            ("service_name", request.name),
            ("instance_id", instance_id),
        ]);
        
        Ok(warp::reply::with_status(
            warp::reply::json(&success("Service registered successfully", Some(response_data))),
            warp::http::StatusCode::CREATED
        ))
    }
    Err(e) => Ok(warp::reply::with_status(
        warp::reply::json(&error::<()>(&format!("Failed to register service: {}", e))),
        warp::http::StatusCode::INTERNAL_SERVER_ERROR
    ))
}
}

/// Handle deregistering a service
async fn handle_deregister_service(
service_name: String,
instance_id: String,
registry: Arc<ServiceRegistry>,
) -> Result<impl Reply, Rejection> {
match registry.deregister_service(&service_name, &instance_id).await {
    Ok(_) => Ok(warp::reply::with_status(
        warp::reply::json(&success::<()>("Service deregistered successfully", None)),
        warp::http::StatusCode::OK
    )),
    Err(e) => Ok(warp::reply::with_status(
        warp::reply::json(&error::<()>(&format!("Failed to deregister service: {}", e))),
        warp::http::StatusCode::INTERNAL_SERVER_ERROR
    ))
}
}

/// Handle updating service health
async fn handle_update_health(
service_name: String,
instance_id: String,
request: HealthUpdateRequest,
registry: Arc<ServiceRegistry>,
) -> Result<impl Reply, Rejection> {
// Convert health string to enum
let health = match request.health.to_lowercase().as_str() {
    "healthy" => ServiceHealth::Healthy,
    "unhealthy" => ServiceHealth::Unhealthy,
    "unknown" => ServiceHealth::Unknown,
    "starting" => ServiceHealth::Starting,
    "maintenance" => ServiceHealth::Maintenance,
    "deregistering" => ServiceHealth::Deregistering,
    _ => return Ok(warp::reply::with_status(
        warp::reply::json(&error::<()>(&format!("Invalid health status: {}", request.health))),
        warp::http::StatusCode::BAD_REQUEST
    )),
};

// Update health
match registry.update_instance_health(&service_name, &instance_id, health).await {
    Ok(_) => Ok(warp::reply::with_status(
        warp::reply::json(&success::<()>("Health updated successfully", None)),
        warp::http::StatusCode::OK
    )),
    Err(e) => Ok(warp::reply::with_status(
        warp::reply::json(&error::<()>(&format!("Failed to update health: {}", e))),
        warp::http::StatusCode::INTERNAL_SERVER_ERROR
    ))
}
}

/// Handle service heartbeat
async fn handle_heartbeat(
service_name: String,
instance_id: String,
registry: Arc<ServiceRegistry>,
) -> Result<impl Reply, Rejection> {
match registry.heartbeat(&service_name, &instance_id).await {
    Ok(_) => Ok(warp::reply::with_status(
        warp::reply::json(&success::<()>("Heartbeat recorded successfully", None)),
        warp::http::StatusCode::OK
    )),
    Err(e) => Ok(warp::reply::with_status(
        warp::reply::json(&error::<()>(&format!("Failed to record heartbeat: {}", e))),
        warp::http::StatusCode::INTERNAL_SERVER_ERROR
    ))
}
}

/// Handle getting all routes
async fn handle_get_routes(
_router: Arc<tokio::sync::Mutex<Router>>,
) -> Result<impl Reply, Rejection> {
// Placeholder implementation
Ok(warp::reply::with_status(
    warp::reply::json(&success("Routes retrieved", Some(HashMap::<String, String>::new()))),
    warp::http::StatusCode::OK
))
}

/// Handle getting a specific route
async fn handle_get_route(
_name: String,
_router: Arc<tokio::sync::Mutex<Router>>,
) -> Result<impl Reply, Rejection> {
// Placeholder implementation
Ok(warp::reply::with_status(
    warp::reply::json(&success("Route retrieved", Some(HashMap::<String, String>::new()))),
    warp::http::StatusCode::OK
))
}

#[derive(Debug, Deserialize)]
struct RouteConfigRequest {
    /// Upstream URL (can now include service:// prefix)
    upstream: String,
    
    /// Timeout in milliseconds
    timeout_ms: Option<u64>,
    
    /// Number of retry attempts
    retry_count: Option<u32>,
    
    /// Route priority
    priority: Option<i32>,
    
    /// Preserve host header
    preserve_host_header: Option<bool>,
}

/// Handle upserting a route
async fn handle_upsert_route(
    name: String,
    request: RouteConfigRequest,
    router: Arc<tokio::sync::Mutex<Router>>,
    service_registry: Arc<ServiceRegistry>,
) -> Result<impl Reply, Rejection> {
    // Check if this is a service-based route
    let upstream = if request.upstream.starts_with("service://") {
        let service_name = request.upstream.replace("service://", "");
        
        // Find healthy instances of the service
        let instances = service_registry.get_healthy_instances(&service_name).await;
        
        if instances.is_empty() {
            return Ok(warp::reply::with_status(
                warp::reply::json(&error::<()>(&format!("No healthy instances for service {}", service_name))),
                warp::http::StatusCode::BAD_REQUEST
            ));
        }
        
        // For now, just use the first healthy instance
        // In a real implementation, you'd want more sophisticated load balancing
        let instance = instances.first().unwrap();
        format!("{}://{}:{}", instance.protocol, instance.host, instance.port)
    } else {
        request.upstream
    };

    // Convert request to route configuration
    let mut route_config = harbr_router::RouteConfig::new(&upstream);

    if let Some(timeout) = request.timeout_ms {
        route_config = route_config.with_timeout(timeout);
    }

    if let Some(retry_count) = request.retry_count {
        route_config = route_config.with_retry_count(retry_count);
    }

    if let Some(priority) = request.priority {
        route_config = route_config.with_priority(priority);
    }

    if let Some(preserve) = request.preserve_host_header {
        route_config = route_config.preserve_host_header(preserve);
    }

    // Add route to router
    let mut router_lock = router.lock().await;
    router_lock.add_route(&name, route_config);

    Ok(warp::reply::with_status(
        warp::reply::json(&success::<()>("Route updated successfully", None)),
        warp::http::StatusCode::OK
    ))
}

/// Handle deleting a route
async fn handle_delete_route(
_name: String,
_router: Arc<tokio::sync::Mutex<Router>>,
) -> Result<impl Reply, Rejection> {
// Placeholder implementation
Ok(warp::reply::with_status(
    warp::reply::json(&success::<()>("Route deleted successfully", None)),
    warp::http::StatusCode::OK
))
}

/// Handle updating TCP configuration
async fn handle_update_tcp(
_request: TcpConfigRequest,
_router: Arc<tokio::sync::Mutex<Router>>,
) -> Result<impl Reply, Rejection> {
// Placeholder implementation
Ok(warp::reply::with_status(
    warp::reply::json(&success::<()>("TCP configuration updated successfully", None)),
    warp::http::StatusCode::OK
))
}

/// Handle updating global router settings
async fn handle_update_settings(
_request: RouterSettingsRequest,
_router: Arc<tokio::sync::Mutex<Router>>,
) -> Result<impl Reply, Rejection> {
// Placeholder implementation
Ok(warp::reply::with_status(
    warp::reply::json(&success::<()>("Router settings updated successfully", None)),
    warp::http::StatusCode::OK
))
}