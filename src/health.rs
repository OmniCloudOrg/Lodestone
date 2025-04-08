// src/health.rs
use crate::service::{Service, ServiceInstance, ServiceHealth};
use crate::metrics;
use anyhow::{Result, Context};
use reqwest::Client;
use std::net::SocketAddr;
use std::time::Duration;
use std::fmt;
use tracing::{debug, info, warn, error};

/// Health check types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthCheckType {
    /// HTTP health check
    Http,
    
    /// TCP health check
    Tcp,
    
    /// Script-based health check
    Script,
    
    /// TTL-based health check
    Ttl,
}

impl fmt::Display for HealthCheckType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HealthCheckType::Http => write!(f, "http"),
            HealthCheckType::Tcp => write!(f, "tcp"),
            HealthCheckType::Script => write!(f, "script"),
            HealthCheckType::Ttl => write!(f, "ttl"),
        }
    }
}

/// Health check configuration
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// Health check type
    pub check_type: HealthCheckType,
    
    /// Health check interval in seconds
    pub interval_secs: u64,
    
    /// Health check timeout in milliseconds
    pub timeout_ms: u64,
    
    /// Path for HTTP health checks
    pub http_path: Option<String>,
    
    /// Expected response codes for HTTP health checks
    pub http_expected_codes: Option<Vec<u16>>,
    
    /// Expected response body for HTTP health checks
    pub http_expected_body: Option<String>,
    
    /// Headers for HTTP health checks
    pub http_headers: Option<Vec<(String, String)>>,
    
    /// Deregistration behavior on failure
    pub deregister_on_failure: bool,
    
    /// Number of failures before marking unhealthy
    pub failure_threshold: u32,
    
    /// Number of successes before marking healthy
    pub success_threshold: u32,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            check_type: HealthCheckType::Http,
            interval_secs: 10,
            timeout_ms: 2000,
            http_path: Some("/health".to_string()),
            http_expected_codes: Some(vec![200]),
            http_expected_body: None,
            http_headers: None,
            deregister_on_failure: true,
            failure_threshold: 3,
            success_threshold: 1,
        }
    }
}

/// Health checker result
#[derive(Debug, Clone)]
pub struct HealthCheckResult {
    /// Instance ID that was checked
    pub instance_id: String,
    
    /// Service name
    pub service_name: String,
    
    /// Health check type
    pub check_type: HealthCheckType,
    
    /// Is instance healthy
    pub is_healthy: bool,
    
    /// Output from the health check
    pub output: String,
    
    /// Time taken for the check in milliseconds
    pub duration_ms: u64,
    
    /// Check timestamp
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Health checker for services
pub struct HealthChecker {
    /// HTTP client
    http_client: Client,
    
    /// Default configuration
    default_config: HealthCheckConfig,
}

impl HealthChecker {
    /// Create a new health checker
    pub fn new(default_config: Option<HealthCheckConfig>) -> Self {
        let http_client = Client::builder()
            .timeout(Duration::from_millis(30000)) // 30 second default timeout
            .build()
            .expect("Failed to create HTTP client");
            
        Self {
            http_client,
            default_config: default_config.unwrap_or_default(),
        }
    }
    
    /// Check a service instance
    pub async fn check_instance(&self, instance: &ServiceInstance) -> Result<HealthCheckResult> {
        let start = std::time::Instant::now();
        let timestamp = chrono::Utc::now();
        
        // Determine check type based on protocol
        let check_type = match instance.protocol.as_str() {
            "http" | "https" => HealthCheckType::Http,
            "tcp" | "udp" => HealthCheckType::Tcp,
            _ => HealthCheckType::Ttl,
        };
        
        // Perform health check
        let (is_healthy, output) = match check_type {
            HealthCheckType::Http => {
                self.perform_http_check(instance).await?
            }
            HealthCheckType::Tcp => {
                self.perform_tcp_check(instance).await?
            }
            HealthCheckType::Script => {
                self.perform_script_check(instance).await?
            }
            HealthCheckType::Ttl => {
                self.perform_ttl_check(instance)?
            }
        };
        
        let duration_ms = start.elapsed().as_millis() as u64;
        
        // Record metrics
        metrics::record_service_health_check(&instance.service_name, is_healthy);
        
        Ok(HealthCheckResult {
            instance_id: instance.id.clone(),
            service_name: instance.service_name.clone(),
            check_type,
            is_healthy,
            output,
            duration_ms,
            timestamp,
        })
    }
    
    /// Perform HTTP health check
    async fn perform_http_check(&self, instance: &ServiceInstance) -> Result<(bool, String)> {
        // Get health check path
        let health_path = instance.health_check_path.as_deref()
            .unwrap_or(&self.default_config.http_path.as_deref().unwrap_or("/health"));
        
        // Build URL
        let url = format!("{}://{}:{}{}", 
            instance.protocol, 
            instance.host, 
            instance.port, 
            health_path
        );
        
        // Set timeout
        let timeout = Duration::from_millis(
            self.default_config.timeout_ms
        );
        
        // Create request
        let mut req_builder = self.http_client.get(&url).timeout(timeout);
        
        // Add headers if configured
        if let Some(headers) = &self.default_config.http_headers {
            for (name, value) in headers {
                req_builder = req_builder.header(name, value);
            }
        }
        
        // Execute request
        let response = match req_builder.send().await {
            Ok(resp) => resp,
            Err(e) => {
                return Ok((false, format!("HTTP request failed: {}", e)));
            }
        };
        
        // Check status code
        let status = response.status();
        let expected_codes = self.default_config.http_expected_codes.as_ref()
            .map(|codes| codes.as_slice())
            .unwrap_or(&[200]);
        
        let status_ok = expected_codes.contains(&status.as_u16());
        
        // Check body if expected
        let body_ok = if let Some(expected_body) = &self.default_config.http_expected_body {
            let body = response.text().await?;
            body.contains(expected_body)
        } else {
            true
        };
        
        // Instance is healthy if both status and body checks pass
        let is_healthy = status_ok && body_ok;
        
        let output = if is_healthy {
            format!("HTTP check passed with status {}", status)
        } else if !status_ok {
            format!("HTTP check failed: expected status code {:?}, got {}", expected_codes, status)
        } else {
            "HTTP check failed: expected body content not found".to_string()
        };
        
        Ok((is_healthy, output))
    }
    
    /// Perform TCP health check
    async fn perform_tcp_check(&self, instance: &ServiceInstance) -> Result<(bool, String)> {
        // Parse address
        let addr = format!("{}:{}", instance.host, instance.port);
        let addr = addr.parse::<SocketAddr>()
            .with_context(|| format!("Invalid socket address: {}", addr))?;
        
        // Set timeout
        let timeout_duration = Duration::from_millis(
            self.default_config.timeout_ms
        );
        
        // Try to connect
        match tokio::time::timeout(timeout_duration, tokio::net::TcpStream::connect(addr)).await {
            Ok(Ok(_)) => {
                Ok((true, format!("TCP connection successful to {}:{}", instance.host, instance.port)))
            }
            Ok(Err(e)) => {
                Ok((false, format!("TCP connection failed: {}", e)))
            }
            Err(_) => {
                Ok((false, format!("TCP connection timed out after {}ms", self.default_config.timeout_ms)))
            }
        }
    }
    
    /// Perform script-based health check
    async fn perform_script_check(&self, instance: &ServiceInstance) -> Result<(bool, String)> {
        // Script-based health checks would typically execute an external command or script
        // For this example, we'll simulate it with a dummy implementation
        
        // Check if a script is defined in the metadata
        if let Some(script) = instance.metadata.get("health_check_script") {
            // In a real implementation, this would execute the script
            // For now, we'll just return success if the script is defined
            
            Ok((true, format!("Script health check would run: {}", script)))
        } else {
            Ok((false, "No health check script defined".to_string()))
        }
    }
    
    /// Perform TTL-based health check
    fn perform_ttl_check(&self, instance: &ServiceInstance) -> Result<(bool, String)> {
        // Check if TTL is defined
        if let Some(ttl) = instance.ttl {
            // Check if last heartbeat is within TTL
            if let Some(last_heartbeat) = instance.last_heartbeat {
                let now = chrono::Utc::now();
                let duration = now.signed_duration_since(last_heartbeat);
                
                let is_healthy = duration.num_seconds() < ttl as i64;
                
                if is_healthy {
                    Ok((true, format!("TTL check passed: last heartbeat {} seconds ago", duration.num_seconds())))
                } else {
                    Ok((false, format!("TTL check failed: last heartbeat {} seconds ago, TTL is {} seconds", 
                        duration.num_seconds(), ttl)))
                }
            } else {
                Ok((false, "TTL check failed: no heartbeat recorded".to_string()))
            }
        } else {
            // No TTL defined, assume healthy
            Ok((true, "No TTL defined, assuming healthy".to_string()))
        }
    }
}

/// Health manager for tracking instance health
pub struct HealthManager {
    /// Health checker
    checker: HealthChecker,
    
    /// Health check counts
    check_counts: dashmap::DashMap<String, (u32, u32)>, // (failures, successes)
    
    /// Health check config
    config: HealthCheckConfig,
}

impl HealthManager {
    /// Create a new health manager
    pub fn new(config: Option<HealthCheckConfig>) -> Self {
        let config = config.unwrap_or_default();
        
        Self {
            checker: HealthChecker::new(Some(config.clone())),
            check_counts: dashmap::DashMap::new(),
            config,
        }
    }
    
    /// Check a service instance and update health status
    pub async fn check_and_update(&self, instance: &ServiceInstance) -> Result<(ServiceHealth, HealthCheckResult)> {
        // Perform health check
        let result = self.checker.check_instance(instance).await?;
        
        // Get current counts
        let mut counts = self.check_counts
            .entry(instance.id.clone())
            .or_insert((0, 0));
            
        // Update counts based on check result
        let (failures, successes) = if result.is_healthy {
            // Reset failures on success
            (0, counts.1 + 1)
        } else {
            // Reset successes on failure
            (counts.0 + 1, 0)
        };
        
        *counts = (failures, successes);
        
        // Determine new health status
        let new_health = if failures >= self.config.failure_threshold {
            // Too many failures
            ServiceHealth::Unhealthy
        } else if successes >= self.config.success_threshold {
            // Enough successes
            ServiceHealth::Healthy
        } else {
            // Not enough data yet, keep current status
            instance.health
        };
        
        Ok((new_health, result))
    }
    
    /// Reset health counts for an instance
    pub fn reset_counts(&self, instance_id: &str) {
        self.check_counts.remove(instance_id);
    }
}