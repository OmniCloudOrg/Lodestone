// src/store.rs
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;
use tracing::{debug, info, warn, error};

/// Store interface for persisting data
#[async_trait]
pub trait Store: Send + Sync {
    /// Get a value by key
    async fn get(&self, key: &str) -> Result<Option<Bytes>>;
    
    /// Set a value by key
    async fn set(&self, key: &str, value: &[u8]) -> Result<()>;
    
    /// Delete a key
    async fn delete(&self, key: &str) -> Result<()>;
    
    /// List keys with a prefix
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
    
    /// Check if a key exists
    async fn exists(&self, key: &str) -> Result<bool>;
    
    /// Clear all data
    async fn clear(&self) -> Result<()>;
}

/// Memory store for transient storage
pub struct MemoryStore {
    data: RwLock<HashMap<String, Bytes>>,
}

impl MemoryStore {
    /// Create a new memory store
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        let data = self.data.read().await;
        Ok(data.get(key).cloned())
    }
    
    async fn set(&self, key: &str, value: &[u8]) -> Result<()> {
        let mut data = self.data.write().await;
        data.insert(key.to_string(), Bytes::copy_from_slice(value));
        Ok(())
    }
    
    async fn delete(&self, key: &str) -> Result<()> {
        let mut data = self.data.write().await;
        data.remove(key);
        Ok(())
    }
    
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let data = self.data.read().await;
        let keys: Vec<String> = data.keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        Ok(keys)
    }
    
    async fn exists(&self, key: &str) -> Result<bool> {
        let data = self.data.read().await;
        Ok(data.contains_key(key))
    }
    
    async fn clear(&self) -> Result<()> {
        let mut data = self.data.write().await;
        data.clear();
        Ok(())
    }
}

/// File system store for persistent storage
pub struct FileStore {
    /// Base directory to store data
    base_dir: PathBuf,
    
    /// Cache
    cache: RwLock<HashMap<String, Bytes>>,
}

impl FileStore {
    /// Create a new file store
    pub async fn new<P: AsRef<Path>>(base_dir: P) -> Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        
        // Create directory if it doesn't exist
        if !base_dir.exists() {
            fs::create_dir_all(&base_dir).await?;
        }
        
        Ok(Self {
            base_dir,
            cache: RwLock::new(HashMap::new()),
        })
    }
    
    /// Get the path for a key
    fn key_path(&self, key: &str) -> PathBuf {
        // Sanitize key to be a valid filename
        let safe_key = key.replace("/", "_").replace(":", "_");
        self.base_dir.join(safe_key)
    }
    
    /// Load all keys into cache
    pub async fn load_cache(&self) -> Result<()> {
        let mut cache = self.cache.write().await;
        
        let mut dir = fs::read_dir(&self.base_dir).await?;
        
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            
            if path.is_file() {
                if let Some(filename) = path.file_name() {
                    if let Some(key) = filename.to_str() {
                        // Load file content
                        let data = fs::read(&path).await?;
                        cache.insert(key.to_string(), Bytes::from(data));
                    }
                }
            }
        }
        
        info!("Loaded {} keys into file store cache", cache.len());
        
        Ok(())
    }
}

#[async_trait]
impl Store for FileStore {
    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        // Try cache first
        let cache = self.cache.read().await;
        if let Some(value) = cache.get(key) {
            return Ok(Some(value.clone()));
        }
        drop(cache);
        
        // Check if file exists
        let path = self.key_path(key);
        if !path.exists() {
            return Ok(None);
        }
        
        // Read from file
        let data = fs::read(&path).await?;
        let bytes = Bytes::from(data);
        
        // Update cache
        let mut cache = self.cache.write().await;
        cache.insert(key.to_string(), bytes.clone());
        
        Ok(Some(bytes))
    }
    
    async fn set(&self, key: &str, value: &[u8]) -> Result<()> {
        // Update file
        let path = self.key_path(key);
        fs::write(&path, value).await?;
        
        // Update cache
        let mut cache = self.cache.write().await;
        cache.insert(key.to_string(), Bytes::copy_from_slice(value));
        
        Ok(())
    }
    
    async fn delete(&self, key: &str) -> Result<()> {
        // Delete file
        let path = self.key_path(key);
        if path.exists() {
            fs::remove_file(&path).await?;
        }
        
        // Update cache
        let mut cache = self.cache.write().await;
        cache.remove(key);
        
        Ok(())
    }
    
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        
        let mut dir = fs::read_dir(&self.base_dir).await?;
        
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            
            if path.is_file() {
                if let Some(filename) = path.file_name() {
                    if let Some(key) = filename.to_str() {
                        if key.starts_with(prefix) {
                            keys.push(key.to_string());
                        }
                    }
                }
            }
        }
        
        Ok(keys)
    }
    
    async fn exists(&self, key: &str) -> Result<bool> {
        // Check cache first
        let cache = self.cache.read().await;
        if cache.contains_key(key) {
            return Ok(true);
        }
        drop(cache);
        
        // Check file system
        let path = self.key_path(key);
        Ok(path.exists())
    }
    
    async fn clear(&self) -> Result<()> {
        // Clear all files
        let mut dir = fs::read_dir(&self.base_dir).await?;
        
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            
            if path.is_file() {
                fs::remove_file(&path).await?;
            }
        }
        
        // Clear cache
        let mut cache = self.cache.write().await;
        cache.clear();
        
        Ok(())
    }
}

/// Factory function to create the appropriate store based on configuration
pub async fn create_store(
    data_dir: &Path,
) -> Result<Arc<Box<dyn Store>>> {
    // Use file store directly
    let file_store = FileStore::new(data_dir).await?;
    let store: Box<dyn Store> = Box::new(file_store);
    
    Ok(Arc::new(store))
}