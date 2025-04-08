// src/util.rs
use anyhow::{Result, Context};
use chrono::{DateTime, Duration, Utc};
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::iter;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{debug, info, warn, error};
use uuid::Uuid;

/// Create a random ID
pub fn random_id() -> String {
    Uuid::new_v4().to_string()
}

/// Create a random string of specific length
pub fn random_string(length: usize) -> String {
    let mut rng = thread_rng();
    iter::repeat(())
        .map(|()| rng.sample(Alphanumeric))
        .map(char::from)
        .take(length)
        .collect()
}

/// Generate a weighted random index based on weights
pub fn weighted_random_index(weights: &[u32]) -> Option<usize> {
    if weights.is_empty() {
        return None;
    }
    
    let sum: u32 = weights.iter().sum();
    if sum == 0 {
        return None;
    }
    
    let mut rng = thread_rng();
    let rand_val = rng.gen_range(0..sum);
    
    let mut cumulative = 0;
    for (i, &weight) in weights.iter().enumerate() {
        cumulative += weight;
        if rand_val < cumulative {
            return Some(i);
        }
    }
    
    // Fallback (should not happen)
    Some(0)
}

/// Round-robin load balancer implementation
pub struct RoundRobinBalancer {
    /// Current index
    current: RwLock<usize>,
    
    /// Number of targets
    targets: usize,
}

impl RoundRobinBalancer {
    /// Create a new round-robin balancer
    pub fn new(targets: usize) -> Self {
        Self {
            current: RwLock::new(0),
            targets,
        }
    }
    
    /// Get the next index
    pub async fn next(&self) -> Option<usize> {
        if self.targets == 0 {
            return None;
        }
        
        let mut current = self.current.write().await;
        let idx = *current;
        *current = (*current + 1) % self.targets;
        
        Some(idx)
    }
    
    /// Update the number of targets
    pub async fn update_targets(&mut self, targets: usize) {
        self.targets = targets;
        
        let mut current = self.current.write().await;
        if self.targets > 0 {
            *current = *current % self.targets;
        } else {
            *current = 0;
        }
    }
}

/// Weighted round-robin load balancer implementation
pub struct WeightedRoundRobinBalancer {
    /// Current running sum
    current_sum: RwLock<i64>,
    
    /// Maximum weight
    max_weight: i64,
    
    /// Greatest common divisor of all weights
    gcd_weight: i64,
    
    /// Weights
    weights: Vec<i64>,
}

impl WeightedRoundRobinBalancer {
    /// Create a new weighted round-robin balancer
    pub fn new(weights: Vec<u32>) -> Self {
        let weights_i64: Vec<i64> = weights.iter().map(|&w| w as i64).collect();
        
        // Find max weight
        let max_weight = *weights_i64.iter().max().unwrap_or(&0);
        
        // Calculate GCD of all weights
        let gcd_weight = if weights_i64.len() > 1 {
            weights_i64
                .iter()
                .skip(1)
                .fold(weights_i64[0], |gcd, &weight| gcd_of_two(gcd, weight))
        } else {
            weights_i64.get(0).copied().unwrap_or(1)
        };
        
        Self {
            current_sum: RwLock::new(0),
            max_weight,
            gcd_weight,
            weights: weights_i64,
        }
    }
    
    /// Get the next index
    pub async fn next(&self) -> Option<usize> {
        if self.weights.is_empty() {
            return None;
        }
        
        let mut i = 0;
        loop {
            let mut current_sum = self.current_sum.write().await;
            
            i = i % self.weights.len();
            
            if i == 0 {
                *current_sum -= self.gcd_weight;
                if *current_sum <= 0 {
                    *current_sum = self.max_weight;
                    if *current_sum == 0 {
                        return Some(0);
                    }
                }
            }
            
            if self.weights[i] >= *current_sum {
                return Some(i);
            }
            
            i += 1;
        }
    }
}

/// Calculate the greatest common divisor of two numbers
fn gcd_of_two(a: i64, b: i64) -> i64 {
    if b == 0 {
        a.abs()
    } else {
        gcd_of_two(b, a % b)
    }
}

/// Calculate exponential backoff delay
pub fn exponential_backoff(attempt: u32, base_ms: u64, max_ms: u64) -> u64 {
    let delay = base_ms * (2_u64.pow(attempt as u32));
    delay.min(max_ms)
}

/// Check if a port is available
pub async fn is_port_available(port: u16) -> bool {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
    tokio::net::TcpListener::bind(addr).await.is_ok()
}

/// Find an available port
pub async fn find_available_port(start_port: u16, end_port: u16) -> Option<u16> {
    for port in start_port..=end_port {
        if is_port_available(port).await {
            return Some(port);
        }
    }
    None
}

/// Create directory if it doesn't exist
pub async fn ensure_directory(dir: impl AsRef<Path>) -> Result<()> {
    let path = dir.as_ref();
    if !path.exists() {
        fs::create_dir_all(path).await
            .with_context(|| format!("Failed to create directory: {}", path.display()))?;
    }
    Ok(())
}

/// Write to a file atomically
pub async fn atomic_write_file(path: impl AsRef<Path>, data: &[u8]) -> Result<()> {
    // Write to a temporary file first
    let path = path.as_ref();
    let temp_path = path.with_extension("tmp");
    
    // Create parent directory if needed
    if let Some(parent) = path.parent() {
        ensure_directory(parent).await?;
    }
    
    // Write to temporary file
    let mut file = fs::File::create(&temp_path).await
        .with_context(|| format!("Failed to create temporary file: {}", temp_path.display()))?;
    
    file.write_all(data).await
        .with_context(|| format!("Failed to write to temporary file: {}", temp_path.display()))?;
    
    file.flush().await
        .with_context(|| format!("Failed to flush temporary file: {}", temp_path.display()))?;
    
    // Rename temporary file to target file
    fs::rename(&temp_path, path).await
        .with_context(|| format!("Failed to rename temporary file to {}", path.display()))?;
    
    Ok(())
}

/// Get current timestamp in milliseconds
pub fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Format timestamp as ISO 8601
pub fn format_timestamp(timestamp: u64) -> String {
    let dt = DateTime::<Utc>::from_utc(
        chrono::NaiveDateTime::from_timestamp_opt((timestamp / 1000) as i64, 0).unwrap_or_default(),
        Utc,
    );
    dt.to_rfc3339()
}

/// Parse ISO 8601 timestamp
pub fn parse_timestamp(timestamp: &str) -> Result<u64> {
    let dt = DateTime::parse_from_rfc3339(timestamp)
        .with_context(|| format!("Invalid timestamp format: {}", timestamp))?;
    
    Ok(dt.timestamp_millis() as u64)
}

/// Calculate time difference in seconds
pub fn time_diff_seconds(from: &DateTime<Utc>, to: &DateTime<Utc>) -> i64 {
    (*to - *from).num_seconds()
}

/// System information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
    /// Node ID
    pub node_id: String,
    
    /// Hostname
    pub hostname: String,
    
    /// CPU count
    pub cpu_count: u32,
    
    /// Total memory in MB
    pub total_memory_mb: u64,
    
    /// Available memory in MB
    pub available_memory_mb: u64,
    
    /// Uptime in seconds
    pub uptime_seconds: u64,
    
    /// OS type
    pub os_type: String,
    
    /// OS version
    pub os_version: String,
}

impl SystemInfo {
    /// Get system information
    pub fn get() -> Result<Self> {
        // Note: In a real implementation, this would use system-specific APIs
        // For this example, we'll create dummy values
        Ok(Self {
            node_id: random_id(),
            hostname: hostname::get()
                .map(|h| h.to_string_lossy().to_string())
                .unwrap_or_else(|_| "unknown".to_string()),
            cpu_count: num_cpus::get() as u32,
            total_memory_mb: 16384, // 16 GB
            available_memory_mb: 8192, // 8 GB
            uptime_seconds: 3600, // 1 hour
            os_type: std::env::consts::OS.to_string(),
            os_version: "1.0.0".to_string(),
        })
    }
    
    /// Get as HashMap
    pub fn as_hashmap(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        
        map.insert("node_id".to_string(), self.node_id.clone());
        map.insert("hostname".to_string(), self.hostname.clone());
        map.insert("cpu_count".to_string(), self.cpu_count.to_string());
        map.insert("total_memory_mb".to_string(), self.total_memory_mb.to_string());
        map.insert("available_memory_mb".to_string(), self.available_memory_mb.to_string());
        map.insert("uptime_seconds".to_string(), self.uptime_seconds.to_string());
        map.insert("os_type".to_string(), self.os_type.clone());
        map.insert("os_version".to_string(), self.os_version.clone());
        
        map
    }
}

/// Retry with exponential backoff
pub async fn retry_with_backoff<F, Fut, T, E>(
    f: F,
    attempts: u32,
    base_delay_ms: u64,
    max_delay_ms: u64,
) -> Result<T, E>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let mut attempt = 0;
    loop {
        match f().await {
            Ok(result) => return Ok(result),
            Err(err) => {
                attempt += 1;
                if attempt >= attempts {
                    return Err(err);
                }
                
                let delay_ms = exponential_backoff(attempt, base_delay_ms, max_delay_ms);
                sleep(tokio::time::Duration::from_millis(delay_ms)).await;
            }
        }
    }
}

/// Simple retry
pub async fn retry<F, Fut, T, E>(f: F, attempts: u32, delay_ms: u64) -> Result<T, E>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let mut attempt = 0;
    loop {
        match f().await {
            Ok(result) => return Ok(result),
            Err(err) => {
                attempt += 1;
                if attempt >= attempts {
                    return Err(err);
                }
                
                sleep(tokio::time::Duration::from_millis(delay_ms)).await;
            }
        }
    }
}

/// Circularly increment counter with wraparound
#[derive(Debug)]
pub struct CircularCounter {
    value: RwLock<u64>,
    max: u64,
}

impl CircularCounter {
    /// Create a new circular counter
    pub fn new(start: u64, max: u64) -> Self {
        Self {
            value: RwLock::new(start % (max + 1)),
            max,
        }
    }
    
    /// Get the current value
    pub async fn get(&self) -> u64 {
        *self.value.read().await
    }
    
    /// Increment and get the new value
    pub async fn increment(&self) -> u64 {
        let mut value = self.value.write().await;
        *value = (*value + 1) % (self.max + 1);
        *value
    }
    
    /// Set to a specific value
    pub async fn set(&self, new_value: u64) {
        let mut value = self.value.write().await;
        *value = new_value % (self.max + 1);
    }
}