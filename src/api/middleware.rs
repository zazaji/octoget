// src/api/middleware.rs
use axum::{
    body::{Body, Bytes},
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use http_body_util::BodyExt;
use serde_json::json;
use tracing::info;

pub struct ApiError(pub anyhow::Error);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(json!({
            "message": self.0.to_string(),
            "data": serde_json::Value::Null
        }));
        (StatusCode::INTERNAL_SERVER_ERROR, body).into_response()
    }
}

impl<E> From<E> for ApiError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

fn format_body(headers: &HeaderMap, bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "<empty>".to_string();
    }
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if content_type.contains("application/json") 
        || content_type.contains("text/") 
        || content_type.contains("application/x-www-form-urlencoded") 
    {
        String::from_utf8_lossy(bytes).into_owned()
    } else {
        format!("<binary data, size: {} bytes>", bytes.len())
    }
}

pub async fn logging_middleware(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let headers = req.headers().clone();

    let (parts, body) = req.into_parts();
    let req_bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => Bytes::new(),
    };

    let req_body_str = format_body(&headers, &req_bytes);
    info!(
        method = %method,
        uri = %uri,
        headers = ?headers,
        body = %req_body_str,
        "Incoming API Request"
    );

    let req = Request::from_parts(parts, Body::from(req_bytes));
    let res = next.run(req).await;

    let (parts, body) = res.into_parts();
    let res_bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => Bytes::new(),
    };

    let res_body_str = format_body(&parts.headers, &res_bytes);
    info!(
        status = %parts.status,
        headers = ?parts.headers,
        body = %res_body_str,
        "Outgoing API Response"
    );

    Response::from_parts(parts, Body::from(res_bytes))
}

pub async fn auth_middleware(
    State(state): State<crate::api::ApiState>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if let Some(auth_header) = req.headers().get(axum::http::header::AUTHORIZATION) {
        if let Ok(auth_str) = auth_header.to_str() {
            if auth_str == format!("Bearer {}", state.my_token) {
                return Ok(next.run(req).await);
            }
        }
    }
    Err(StatusCode::UNAUTHORIZED)
}