<div align="right">
<img align="right" src="https://github.com/user-attachments/assets/0396c028-f92c-4fbe-9b0e-b142d12144d1" alt="Lodestone Logo" width="200"/>
</div>

# Lodestone: High-Performance Service Discovery and Routing System

<p align="center">
  <strong>A lightweight service registry and intelligent routing system built on Harbr-Router</strong>
</p>

<p align="center">
  <a href="#features">Features</a> •
  <a href="#quick-start">Quick Start</a> •
  <a href="#concepts">Concepts</a> •
  <a href="#architecture">Architecture</a> •
  <a href="#api">API</a> •
  <a href="#configuration">Configuration</a> •
  <a href="#deployment">Deployment</a> •
  <a href="#contributing">Contributing</a> •
  <a href="#license">License</a>
</p>

## Features

- 🔍 **Service Discovery**: Automatic registration, health checking, and deregistration
- 🔀 **Dynamic Routing**: Zero-downtime route updates across HTTP, TCP, and UDP protocols
- 🩺 **Health Checking**: HTTP, TCP, custom script, and TTL-based health checks
- 🚦 **Load Balancing**: Round-robin, weighted, and health-aware routing
- 📊 **Metrics & Monitoring**: Prometheus-compatible metrics for observability
- 🔑 **Security**: TLS encryption and authentication
- 📚 **API & CLI**: Fully featured HTTP API and command-line interface
- 🧩 **Client Libraries**: Easy integration for applications
- 🏢 **Multi-Protocol**: Support for HTTP, TCP, UDP, and database protocols

## Quick Start

### Install with Cargo

```bash
cargo install lodestone
```

### Start a Node

```bash
lodestone --node-id node1 --bind-addr 127.0.0.1 --data-dir ./data
```

### Register a Service

```bash
lodestone service register my-service 127.0.0.1:8080 --tags web,api
```

### Using the Client Library

```rust
use lodestone::client::LodestoneClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = LodestoneClient::new("http://127.0.0.1:8081");
    
    // Register a service
    let instance_id = client.register_service(
        "my-service", 
        "127.0.0.1:8080", 
        vec!["web".to_string()]
    ).await?;
    
    println!("Service registered with ID: {}", instance_id);
    
    // Discover services
    let instances = client.discover_service("my-service").await?;
    println!("Found {} service instances", instances.len());
    
    Ok(())
}
```

### Service Discovery and Dynamic Routing

```rust
use lodestone::client::LodestoneClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = LodestoneClient::new("http://127.0.0.1:8081");

    // Register multiple database service instances
    let db_instance1 = client.register_service(
        "database", 
        "192.168.1.100:5432", 
        vec!["postgres".to_string(), "primary".to_string()]
    ).await?;

    let db_instance2 = client.register_service(
        "database", 
        "192.168.1.101:5432", 
        vec!["postgres".to_string(), "replica".to_string()]
    ).await?;

    // Create a route that dynamically proxies to registered database services
    client.add_service_route(
        "/api/data",   // Local path to expose
        "database",    // Service to proxy to
        Some(RouteOptions {
            timeout_ms: Some(5000),
            retry_count: Some(2),
            preserve_host_header: Some(true),
        })
    ).await?;

    // Now any request to /api/data will:
    // 1. Automatically route to a healthy "database" service
    // 2. Use load balancing across registered instances
    // 3. Fall back to another instance if one fails

    Ok(())
}
```

## Concepts

### Service Registry

The **Service Registry** is a catalog of available services and their instances. It provides:

- **Service Registration**: Add new service instances to the registry
- **Service Discovery**: Find service instances by name or tags
- **Health Monitoring**: Track the health status of services
- **TTL and Heartbeats**: Automatic deregistration of failed services

### Dynamic Router

The **Dynamic Router** directs traffic to healthy services with zero-downtime configuration changes:

- **Path-Based Routing**: Route HTTP requests based on URL paths
- **Protocol Support**: Handle HTTP, TCP, UDP, and database traffic
- **Health-Aware**: Only route to healthy instances
- **Load Balancing**: Distribute traffic optimally across instances
- **Retries and Timeouts**: Ensure request reliability

## Architecture

Lodestone consists of several key components:

- **Service Registry**: Maintains the catalog of services and instances
- **Health Checker**: Monitors service health using various strategies
- **Router**: Directs traffic to healthy service instances
- **API Server**: Provides HTTP API for management and integration
- **Storage Layer**: Persists data using local file storage

## Configuration

Lodestone uses a TOML configuration file:

```toml
# General node configuration
[node]
id = "node1"
name = "primary-node"
bind_ip = "0.0.0.0"
router_port = 8080
api_port = 8081
data_dir = "./data"
tags = ["primary"]

# Router configuration
[router]
global_timeout_ms = 30000
max_connections = 10000
enable_retries = true
default_retry_count = 3

# Service discovery configuration
[discovery]
health_check_interval_secs = 10
health_check_timeout_ms = 2000
default_health_check_path = "/health"
service_ttl_secs = 60
deregistration_delay_secs = 30

# Security configuration
[security]
tls_enabled = false
api_auth_enabled = false
```

## Deployment

### Docker

```dockerfile
FROM rust:1.70 as builder
WORKDIR /usr/src/lodestone
COPY . .
RUN cargo build --release

FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y ca-certificates tzdata && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/lodestone/target/release/lodestone /usr/local/bin/
RUN mkdir -p /etc/lodestone /data/lodestone
ENV CONFIG_FILE="/etc/lodestone/config.toml"
EXPOSE 8080 8081
ENTRYPOINT ["lodestone"]
CMD ["-c", "/etc/lodestone/config.toml"]
```

## Production Best Practices

1. **Data Persistence**: Use persistent storage for the data directory
2. **Security**: Enable TLS and API authentication
3. **Monitoring**: Configure Prometheus and Grafana dashboards
4. **Health Checks**: Custom health checks for accurate service status
5. **Load Balancing**: Use weighted load balancing for heterogeneous environments
6. **Graceful Shutdown**: Properly deregister services during shutdown
7. **Timeouts**: Configure appropriate timeouts for your services

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Harbr-Router](https://github.com/harbr-foundation/harbr-router) - High-performance routing library
- [Tokio](https://tokio.rs/) - Async runtime
- [Warp](https://github.com/seanmonstar/warp) - Web framework
