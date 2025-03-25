// src/store/mod.rs
use crate::{prelude::*, service::Service};
use sled::Db;
use std::path::Path;
use serde_json5;

pub struct Store {
    db: Db,
}

impl Store {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = sled::open(path)?;
        Ok(Self { db })
    }

    pub fn set(&self, key: &str, value: &Service) -> Result<()> {
        let serialized = serde_json::to_vec(value)
            .map_err(|e| Error::Storage(e.to_string()))?;
        
        self.db
            .insert(key.as_bytes(), serialized)
            .map_err(|e| Error::Storage(e.to_string()))?;
            
        self.db
            .flush()
            .map_err(|e| Error::Storage(e.to_string()))?;
            
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<Service>> {
        if let Some(data) = self.db.get(key.as_bytes())
            .map_err(|e| Error::Storage(e.to_string()))? {
            let service: Service = serde_json5::from_slice(&data)
                .map_err(|e| Error::Storage(e.to_string()))?;
            Ok(Some(service))
        } else {
            Ok(None)
        }
    }

    pub fn list(&self) -> Result<Vec<Service>> {
        let mut services = Vec::new();
        
        for item in self.db.iter() {
            let (_, value) = item.map_err(|e| Error::Storage(e.to_string()))?;
            let service: Service = serde_json5::from_slice(&value)
                .map_err(|e| Error::Storage(e.to_string()))?;
            services.push(service);
        }
        
        Ok(services)
    }

    pub fn delete(&self, key: &str) -> Result<()> {
        self.db
            .remove(key.as_bytes())
            .map_err(|e| Error::Storage(e.to_string()))?;
            
        self.db
            .flush()
            .map_err(|e| Error::Storage(e.to_string()))?;
            
        Ok(())
    }

    pub fn scan_prefix(&self, prefix: &str) -> Result<Vec<Service>> {
        let mut services = Vec::new();
        
        for item in self.db.scan_prefix(prefix.as_bytes()) {
            let (_, value) = item.map_err(|e| Error::Storage(e.to_string()))?;
            let service: Service = serde_json5::from_slice(&value)
                .map_err(|e| Error::Storage(e.to_string()))?;
            services.push(service);
        }
        
        Ok(services)
    }
}