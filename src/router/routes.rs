use axum::{
    Router as AxumRouter,
    routing::{get, post, delete},
    extract::{State, Path},
    Json,
    response::IntoResponse,
    http::StatusCode,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::{
    discovery_old::ServiceRegistry, error::Error, service::Service
};

pub struct Router {
    registry: Arc<RwLock<ServiceRegistry>>,
}

impl Router {
    pub fn new(registry: Arc<RwLock<ServiceRegistry>>) -> AxumRouter {
        let shared_state = Arc::new(Self { registry });

        AxumRouter::new()
            .route("/services", post(Self::register_service))
            .route("/services", get(Self::list_services))  // Add this line
            .route("/services/:id", get(Self::get_service))
            .route("/services/:id", delete(Self::deregister_service))
            .with_state(shared_state)
    }

    async fn register_service(
        State(state): State<Arc<Router>>,
        Json(service): Json<Service>,
    ) -> Result<(StatusCode, Json<Service>), Error> {
        state.registry.write().await.register(service.clone()).await?;
        Ok((StatusCode::CREATED, Json(service)))
    }

    async fn get_service(
        State(state): State<Arc<Router>>,
        Path(id): Path<String>,
    ) -> Result<Json<Option<Service>>, Error> {
        let service = state.registry.read().await.get_service(&id).await?;
        Ok(Json(service))
    }

    async fn deregister_service(
        State(state): State<Arc<Router>>,
        Path(id): Path<String>,
    ) -> Result<StatusCode, Error> {
        state.registry.write().await.deregister(&id).await?;
        Ok(StatusCode::NO_CONTENT)
    }

    async fn list_services(
        State(state): State<Arc<Router>>,
    ) -> Result<Json<Vec<Service>>, Error> {
        let services = state.registry.read().await.list_services().await?;
        Ok(Json(services))
    }
}

// Implement IntoResponse for Error to properly handle errors
impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let status = match self {
            Error::ServiceNotFound(_) => StatusCode::NOT_FOUND,
            Error::BadRequest(_) => StatusCode::BAD_REQUEST,
            Error::Auth(_) => StatusCode::UNAUTHORIZED,
            Error::RateLimit => StatusCode::TOO_MANY_REQUESTS,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, self.to_string()).into_response()
    }
}