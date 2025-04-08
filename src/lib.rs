// src/lib.rs
//! Lodestone - High-Performance Service Discovery and Routing System
//!
//! Lodestone combines a distributed service registry with a high-performance
//! router, allowing for dynamic service discovery and zero-downtime routing
//! in microservice environments.

use std::sync::Arc;

pub mod config;
pub mod discovery;
pub mod router;
pub mod api;
pub mod client;
pub mod service;
pub mod store;
pub mod health;
pub mod metrics;
pub mod util;

/// Re-export key types for easier usage
pub use config::Config;
pub use service::{Service, ServiceInstance, ServiceHealth};
pub use discovery::ServiceRegistry;
pub use router::Router;

/// Version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");