// src/client.rs
use crate::service::{Service, ServiceInstance, ServiceHealth};
use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use reqwest::Client;
use tracing::debug;

/// Standard API response
#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    /// Success flag
    success: bool,
    
    /// Response message
    message: String,
    
    /// Response data
    #[serde(default)]
    data: Option<T>,
}

/// Service registration response
#[derive(Debug, Deserialize, Default)]
struct RegisterResponse {
    /// Service name
    service_name: String,
    
    /// Instance ID
    instance_id: String,
}

/// Options for configuring a route
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct RouteOptions {
    /// Request timeout in milliseconds
    pub timeout_ms: Option<u64>,
    
    /// Number of retry attempts
    pub retry_count: Option<u32>,
    
    /// Preserve the original host header
    pub preserve_host_header: Option<bool>,
}

/// Client for interacting with Lodestone
pub struct LodestoneClient {
    /// HTTP client
    client: Client,
    
    /// Base URL
    base_url: String,
}

impl LodestoneClient {
    /// Create a new Lodestone client
    pub fn new(base_url: &str) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("Failed to create HTTP client");
        
        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }
    
    /// Register a service
    pub async fn register_service(
        &self,
        name: &str,
        address: &str,
        tags: Vec<String>,
    ) -> Result<String> {
        debug!("Registering service {} at {}", name, address);
        
        // Parse address into host and port
        let parts: Vec<&str> = address.split(':').collect();
        if parts.len() != 2 {
            return Err(anyhow::anyhow!("Invalid address format (expected host:port)"));
        }
        
        let host = parts[0];
        let port = parts[1].parse::<u16>()
            .context("Invalid port number")?;
        
        // Prepare request
        let url = format!("{}/v1/services", self.base_url);
        
        let protocol = if address.starts_with("https://") {
            "https"
        } else {
            "http"
        };
        
        let request = serde_json::json!({
            "name": name,
            "host": host,
            "port": port,
            "protocol": protocol,
            "tags": tags,
        });
        
        // Send request
        let response = self.client.post(&url)
            .json(&request)
            .send()
            .await
            .context("Failed to send request")?;
        
        // Parse response
        let body: ApiResponse<RegisterResponse> = response.json()
            .await
            .context("Failed to parse response")?;
        
        if !body.success {
            return Err(anyhow::anyhow!("API error: {}", body.message));
        }
        
        // Extract instance ID
        if let Some(data) = body.data {
            Ok(data.instance_id)
        } else {
            Err(anyhow::anyhow!("No instance ID in response"))
        }
    }
    
    /// Deregister a service
    pub async fn deregister_service(&self, instance_id: &str) -> Result<()> {
        debug!("Deregistering service instance {}", instance_id);
        
        // Prepare request
        let parts: Vec<&str> = instance_id.split('-').collect();
        if parts.len() < 2 {
            return Err(anyhow::anyhow!("Invalid instance ID format"));
        }
        
        let service_name = parts[0];
        let url = format!("{}/v1/services/{}/{}", self.base_url, service_name, instance_id);
        
        // Send request
        let response = self.client.delete(&url)
            .send()
            .await
            .context("Failed to send request")?;
        
        // Parse response
        let body: ApiResponse<()> = response.json()
            .await
            .context("Failed to parse response")?;
        
        if !body.success {
            return Err(anyhow::anyhow!("API error: {}", body.message));
        }
        
        Ok(())
    }
    
    /// Add a route that proxies to a service in the registry
    pub async fn add_service_route(
        &self, 
        path: &str,     // Local path to expose
        service_name: &str, // Name of service in registry to proxy to
        options: Option<RouteOptions>
    ) -> Result<()> {
        debug!("Adding route {} -> service {}", path, service_name);
        
        // Prepare request
        let url = format!("{}/v1/routes/{}", self.base_url, path);
        
        // Default options if not provided
        let opts = options.unwrap_or_default();
        
        let request = serde_json::json!({
            "upstream": format!("service://{}", service_name),
            "timeout_ms": opts.timeout_ms.unwrap_or(30000),
            "retry_count": opts.retry_count.unwrap_or(3),
            "preserve_host_header": opts.preserve_host_header.unwrap_or(true),
        });
        
        // Send request
        let response = self.client.put(&url)
            .json(&request)
            .send()
            .await
            .context("Failed to send route request")?;
        
        // Parse response
        let body: ApiResponse<()> = response.json()
            .await
            .context("Failed to parse response")?;
        
        if !body.success {
            return Err(anyhow::anyhow!("API error: {}", body.message));
        }
        
        Ok(())
    }
    
    // Rest of the previous implementation remains the same...
    
    /// Get all services
    pub async fn get_services(&self) -> Result<HashMap<String, Service>> {
        debug!("Fetching all services");
        
        // Prepare request
        let url = format!("{}/v1/services", self.base_url);
        
        // Send request
        let response = self.client.get(&url)
            .send()
            .await
            .context("Failed to send request")?;
        
        // Parse response
        let body: ApiResponse<HashMap<String, Service>> = response.json()
            .await
            .context("Failed to parse response")?;
        
        if !body.success {
            return Err(anyhow::anyhow!("API error: {}", body.message));
        }
        
        // Extract services
        if let Some(services) = body.data {
            Ok(services)
        } else {
            Ok(HashMap::new())
        }
    }
    
    /// Get a specific service
    pub async fn get_service(&self, name: &str) -> Result<Service> {
        debug!("Fetching service {}", name);
        
        // Prepare request
        let url = format!("{}/v1/services/{}", self.base_url, name);
        
        // Send request
        let response = self.client.get(&url)
            .send()
            .await
            .context("Failed to send request")?;
        
        // Parse response
        if response.status().is_client_error() {
            return Err(anyhow::anyhow!("Service not found"));
        }
        
        let body: ApiResponse<Service> = response.json()
            .await
            .context("Failed to parse response")?;
        
        if !body.success {
            return Err(anyhow::anyhow!("API error: {}", body.message));
        }
        
        // Extract service
        if let Some(service) = body.data {
            Ok(service)
        } else {
            Err(anyhow::anyhow!("No service data in response"))
        }
    }
    
    /// Discover services with a specific tag
    pub async fn discover_by_tag(&self, tag: &str) -> Result<Vec<ServiceInstance>> {
        debug!("Discovering services with tag {}", tag);
        
        // Get all services first
        let services = self.get_services().await?;
        
        // Filter by tag
        let mut instances = Vec::new();
        for (_, service) in services {
            for instance in service.instances {
                if instance.tags.contains(&tag.to_string()) && instance.health == ServiceHealth::Healthy {
                    instances.push(instance);
                }
            }
        }
        
        Ok(instances)
    }
    
    /// Discover healthy instances of a specific service
    pub async fn discover_service(&self, name: &str) -> Result<Vec<ServiceInstance>> {
        debug!("Discovering healthy instances of service {}", name);
        
        // Try to get the specific service
        match self.get_service(name).await {
            Ok(service) => {
                // Filter for healthy instances
                let instances: Vec<ServiceInstance> = service.instances
                    .into_iter()
                    .filter(|i| i.health == ServiceHealth::Healthy)
                    .collect();
                
                Ok(instances)
            }
            Err(_) => Ok(Vec::new()),
        }
    }
}