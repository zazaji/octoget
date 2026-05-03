// src/api/mod.rs
pub mod middleware;
pub mod peers;
pub mod sys;
pub mod tasks;

use crate::config::Config;
use crate::coordinator::Coordinator;
use crate::frontend::serve_frontend;
use crate::utils::LogReloadHandle;
use axum::{middleware as axum_middleware, routing::{get, post}, Router};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

#[derive(Clone)]
pub struct ApiState {
    pub coordinator: Arc<Coordinator>,
    pub my_token: String,
    pub node_id: String,
    pub config: Arc<RwLock<Config>>,
    pub config_path: String,
    pub log_reload_handle: Arc<LogReloadHandle>,
}

pub async fn start_api_server(
    coordinator: Arc<Coordinator>, 
    port: u16, 
    my_token: String, 
    node_id: String,
    config: Arc<RwLock<Config>>,
    config_path: String,
    log_reload_handle: Arc<LogReloadHandle>,
) {
    let state = ApiState { coordinator, my_token: my_token.clone(), node_id, config, config_path, log_reload_handle };

    let api_routes = Router::new()
        .route("/v1/peers", post(peers::handle_add_peer).get(peers::handle_list_peers))
        .route("/v1/tasks", post(tasks::handle_submit_task).get(tasks::handle_list_tasks))
        .route("/v1/tasks/batch", post(tasks::handle_batch_submit))
        .route("/v1/tasks/:id", get(tasks::handle_get_task).delete(tasks::handle_delete_task))
        .route("/v1/tasks/:id/pause", post(tasks::handle_pause_task))
        .route("/v1/tasks/:id/resume", post(tasks::handle_resume_task))
        .route("/v1/tasks/:id/open", post(tasks::handle_open_dir))
        .route("/v1/tasks/:id/checksums", post(tasks::handle_recalc_checksums))
        .route("/v1/internal/resolve", post(sys::handle_internal_resolve))
        .route("/v1/sys/config", get(sys::handle_get_sys_config).put(sys::handle_update_sys_config))
        .route("/v1/sys/info", get(sys::handle_get_sys_info))
        .route_layer(axum_middleware::from_fn_with_state(state.clone(), middleware::auth_middleware))
        .with_state(state.clone());

    let app = Router::new()
        .route("/", get(serve_frontend))
        .route("/style.css", get(crate::frontend::serve_css))
        .route("/app.js", get(crate::frontend::serve_js))
        .route("/i18n.js", get(crate::frontend::serve_i18n))
        .route("/vendor/vue.global.prod.js", get(crate::frontend::serve_vue))
        .route("/vendor/tailwindcss-browser.js", get(crate::frontend::serve_tailwind))
        .with_state(my_token)
        .nest("/api", api_routes)
        .layer(axum_middleware::from_fn(middleware::logging_middleware));

    let addr = format!("0.0.0.0:{}", port);
    info!("Starting REST API & Web UI server on {}", addr);
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("Failed to bind API server to {}: {}", addr, e);
            return;
        }
    };
    axum::serve(listener, app).await.unwrap();
}