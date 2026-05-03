// src/api/sys.rs
use crate::api::middleware::ApiError;
use crate::api::ApiState;
use crate::config::Config;
use crate::coordinator::models::SysInfoResp;
use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
}

pub async fn handle_internal_resolve(State(state): State<ApiState>, Json(payload): Json<crate::utils::ResolveReq>) -> Result<impl IntoResponse, ApiError> {
    let use_proxy = state.config.read().await.use_proxy.unwrap_or(false);
    let client = crate::utils::http_client_builder(use_proxy)
        .redirect(reqwest::redirect::Policy::limited(10))
        .timeout(std::time::Duration::from_secs(10))
        .build()?;
    let res = crate::utils::resolve_url(&client, &payload.url).await?;
    Ok((StatusCode::OK, Json(ApiResponse { message: "Resolved".to_string(), data: Some(res) })))
}

pub async fn handle_get_sys_config(State(state): State<ApiState>) -> Result<impl IntoResponse, ApiError> {
    let config = state.config.read().await.clone();
    Ok((StatusCode::OK, Json(ApiResponse { message: "Success".to_string(), data: Some(config) })))
}

pub async fn handle_update_sys_config(State(state): State<ApiState>, Json(payload): Json<Config>) -> Result<impl IntoResponse, ApiError> {
    let new_log_level = payload.log_level.clone().unwrap_or_else(|| "info".to_string());
    {
        let mut config = state.config.write().await;
        *config = payload.clone();
    }
    payload.save(&state.config_path).await?;
    
    if let Ok(new_filter) = tracing_subscriber::EnvFilter::try_new(&new_log_level) {
        let _ = state.log_reload_handle.modify(|filter| *filter = new_filter);
    }

    crate::coordinator::task_ops::check_pending_tasks(&state.coordinator).await;

    Ok((StatusCode::OK, Json(ApiResponse { message: "Config updated".to_string(), data: None::<()> })))
}

pub async fn handle_get_sys_info(State(state): State<ApiState>) -> Result<impl IntoResponse, ApiError> {
    Ok((StatusCode::OK, Json(ApiResponse {
        message: "Success".to_string(),
        data: Some(SysInfoResp { 
            node_id: state.node_id.clone(), 
            version: env!("CARGO_PKG_VERSION").to_string(),
            os: std::env::consts::OS.to_string(),
        }),
    })))
}