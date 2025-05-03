use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use std::collections::HashMap;
use tokio::sync::Mutex;
use lazy_static::lazy_static;
use prometheus::{
    IntCounterVec, Counter, Gauge, Histogram, HistogramOpts, HistogramVec, Registry,
    register_int_counter_vec, register_counter, register_gauge, register_histogram,
};
use tracing::info;

// Define a global registry for Prometheus metrics
lazy_static! {
    static ref REGISTRY: Mutex<Option<Registry>> = Mutex::new(None);

    // Service discovery metrics
    static ref SERVICE_REGISTERED_COUNTER: IntCounterVec = register_int_counter_vec!(
        "lodestone_service_registered_total",
        "Total number of service instances registered",
        &["service_name"]
    ).expect("Failed to register SERVICE_REGISTERED_COUNTER");

    static ref SERVICE_DEREGISTERED_COUNTER: IntCounterVec = register_int_counter_vec!(
        "lodestone_service_deregistered_total", 
        "Total number of service instances deregistered",
        &["service_name"]
    ).expect("Failed to register SERVICE_DEREGISTERED_COUNTER");

    static ref SERVICE_HEALTH_CHECK_COUNTER: IntCounterVec = register_int_counter_vec!(
        "lodestone_service_health_check_total",
        "Total number of service health checks",
        &["service_name", "result"]
    ).expect("Failed to register SERVICE_HEALTH_CHECK_COUNTER");

    static ref SERVICE_INSTANCE_GAUGE: Gauge = register_gauge!(
        "lodestone_service_instances",
        "Current number of registered service instances"
    ).expect("Failed to register SERVICE_INSTANCE_GAUGE");

    static ref SERVICE_HEALTHY_GAUGE: Gauge = register_gauge!(
        "lodestone_service_healthy_instances",
        "Current number of healthy service instances"
    ).expect("Failed to register SERVICE_HEALTHY_GAUGE");

    // Router metrics
    static ref ROUTER_REQUEST_COUNTER: IntCounterVec = register_int_counter_vec!(
        "lodestone_router_requests_total",
        "Total number of requests processed by the router",
        &["route", "status"]
    ).expect("Failed to register ROUTER_REQUEST_COUNTER");

    static ref ROUTER_REQUEST_DURATION: HistogramVec = {
        let opts = HistogramOpts::new(
            "lodestone_router_request_duration_seconds",
            "Request duration in seconds"
        )
        .buckets(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]);
        
        prometheus::register_histogram_vec!(opts, &["route"]).expect("Failed to register ROUTER_REQUEST_DURATION")
    };

    static ref ROUTER_ACTIVE_CONNECTIONS: Gauge = register_gauge!(
        "lodestone_router_active_connections",
        "Current number of active connections"
    ).expect("Failed to register ROUTER_ACTIVE_CONNECTIONS");

    // Raft metrics
    static ref RAFT_LEADER_CHANGES: Counter = register_counter!(
        "lodestone_raft_leader_changes_total",
        "Total number of Raft leader changes"
    ).expect("Failed to register RAFT_LEADER_CHANGES");

    static ref RAFT_COMMIT_INDEX: Gauge = register_gauge!(
        "lodestone_raft_commit_index",
        "Current Raft commit index"
    ).expect("Failed to register RAFT_COMMIT_INDEX");

    static ref RAFT_LOG_SIZE: Gauge = register_gauge!(
        "lodestone_raft_log_size",
        "Current size of the Raft log in entries"
    ).expect("Failed to register RAFT_LOG_SIZE");

    static ref RAFT_APPLY_DURATION: Histogram = register_histogram!(
        HistogramOpts::new(
            "lodestone_raft_apply_duration_seconds",
            "Time taken to apply Raft operations"
        )
        .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5])
    ).expect("Failed to register RAFT_APPLY_DURATION");
}

/// Initialize metrics
pub async fn init_metrics() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut registry_guard = REGISTRY.lock().await;
    
    if registry_guard.is_none() {
        let registry = Registry::new();
        
        // Register default metrics collectors
        *registry_guard = Some(registry);
        
        info!("Metrics initialized");
    }
    
    Ok(())
}

/// Expose metrics in Prometheus format
pub fn gather_metrics() -> String {
    // This would use prometheus client to gather metrics
    // For simplicity, we just return an example metrics string
    let mut output = String::new();
    
    // Headers and samples
    output.push_str("# HELP lodestone_service_instances Current number of registered service instances\n");
    output.push_str("# TYPE lodestone_service_instances gauge\n");
    output.push_str("lodestone_service_instances ");
    output.push_str(&SERVICE_INSTANCE_GAUGE.get().to_string());
    output.push_str("\n");
    
    // More metrics...
    
    output
}

/// Record a service registration
pub fn record_service_registered(service_name: &str) {
    SERVICE_REGISTERED_COUNTER.with_label_values(&[service_name]).inc();
    SERVICE_INSTANCE_GAUGE.inc();
}

/// Record a service deregistration
pub fn record_service_deregistered(service_name: &str) {
    SERVICE_DEREGISTERED_COUNTER.with_label_values(&[service_name]).inc();
    SERVICE_INSTANCE_GAUGE.dec();
}

/// Record a service health check
pub fn record_service_health_check(service_name: &str, healthy: bool) {
    let result = if healthy { "healthy" } else { "unhealthy" };
    SERVICE_HEALTH_CHECK_COUNTER.with_label_values(&[service_name, result]).inc();
}

/// Update service health gauges
pub fn update_service_health_gauges(healthy_count: usize) {
    SERVICE_HEALTHY_GAUGE.set(healthy_count as f64);
}

/// Record a router request
pub fn record_router_request(route: &str, status_code: u16) {
    let status_range = match status_code {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "unknown",
    };
    
    ROUTER_REQUEST_COUNTER.with_label_values(&[route, status_range]).inc();
}

/// Track router request duration
pub struct RequestTimer {
    route: String,
    start: Instant,
}

impl RequestTimer {
    /// Create a new request timer
    pub fn new(route: &str) -> Self {
        Self {
            route: route.to_string(),
            start: Instant::now(),
        }
    }
    
    /// Record the request duration
    pub fn observe(self) {
        let duration = self.start.elapsed();
        ROUTER_REQUEST_DURATION
            .with_label_values(&[&self.route])
            .observe(duration.as_secs_f64());
    }
}

/// Increment active connections
pub fn increment_active_connections() {
    ROUTER_ACTIVE_CONNECTIONS.inc();
}

/// Decrement active connections
pub fn decrement_active_connections() {
    ROUTER_ACTIVE_CONNECTIONS.dec();
}

/// Record a Raft leader change
pub fn record_raft_leader_change() {
    RAFT_LEADER_CHANGES.inc();
}

/// Update Raft commit index
pub fn update_raft_commit_index(index: u64) {
    RAFT_COMMIT_INDEX.set(index as f64);
}

/// Update Raft log size
pub fn update_raft_log_size(size: usize) {
    RAFT_LOG_SIZE.set(size as f64);
}

/// Measure Raft apply operation duration
pub struct RaftApplyTimer {
    start: Instant,
}

impl RaftApplyTimer {
    /// Create a new Raft apply timer
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }
    
    /// Record the apply duration
    pub fn observe(self) {
        let duration = self.start.elapsed();
        RAFT_APPLY_DURATION.observe(duration.as_secs_f64());
    }
}

/// Reset all metrics to their initial state
pub fn reset_metrics() {
    // Since we can't easily reset Prometheus metrics, 
    // this is more for testing purposes
    SERVICE_INSTANCE_GAUGE.set(0.0);
    SERVICE_HEALTHY_GAUGE.set(0.0);
    ROUTER_ACTIVE_CONNECTIONS.set(0.0);
    RAFT_COMMIT_INDEX.set(0.0);
    RAFT_LOG_SIZE.set(0.0);
}

/// Utility struct for tracking metrics in a single node
#[derive(Debug, Clone)]
pub struct NodeMetrics {
    /// Node identifier
    pub node_id: String,
    
    /// Number of registered services
    pub registered_services: Arc<AtomicU64>,
    
    /// Number of healthy instances
    pub healthy_instances: Arc<AtomicU64>,
    
    /// Number of active connections
    pub active_connections: Arc<AtomicU64>,
    
    /// Is node leader
    pub is_leader: bool,
    
    /// Uptime in seconds
    pub uptime: u64,
    
    /// CPU usage percentage
    pub cpu_usage: f64,
    
    /// Memory usage in MB
    pub memory_usage: f64,
}

impl NodeMetrics {
    /// Create new node metrics
    pub fn new(node_id: &str) -> Self {
        Self {
            node_id: node_id.to_string(),
            registered_services: Arc::new(AtomicU64::new(0)),
            healthy_instances: Arc::new(AtomicU64::new(0)),
            active_connections: Arc::new(AtomicU64::new(0)),
            is_leader: false,
            uptime: 0,
            cpu_usage: 0.0,
            memory_usage: 0.0,
        }
    }
    
    /// Increment registered services
    pub fn increment_services(&self) {
        self.registered_services.fetch_add(1, Ordering::SeqCst);
    }
    
    /// Decrement registered services
    pub fn decrement_services(&self) {
        self.registered_services.fetch_sub(1, Ordering::SeqCst);
    }
    
    /// Update healthy instances
    pub fn set_healthy_instances(&self, count: u64) {
        self.healthy_instances.store(count, Ordering::SeqCst);
    }
    
    /// Increment active connections
    pub fn increment_connections(&self) {
        self.active_connections.fetch_add(1, Ordering::SeqCst);
    }
    
    /// Decrement active connections
    pub fn decrement_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::SeqCst);
    }
    
    /// Get all metrics as a HashMap
    pub fn as_hashmap(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        
        map.insert("node_id".to_string(), self.node_id.clone());
        map.insert("registered_services".to_string(), self.registered_services.load(Ordering::SeqCst).to_string());
        map.insert("healthy_instances".to_string(), self.healthy_instances.load(Ordering::SeqCst).to_string());
        map.insert("active_connections".to_string(), self.active_connections.load(Ordering::SeqCst).to_string());
        map.insert("is_leader".to_string(), self.is_leader.to_string());
        map.insert("uptime".to_string(), self.uptime.to_string());
        map.insert("cpu_usage".to_string(), format!("{:.2}", self.cpu_usage));
        map.insert("memory_usage".to_string(), format!("{:.2}", self.memory_usage));
        
        map
    }
}