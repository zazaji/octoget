// src/api/peers.rs
use crate::api::middleware::ApiError;
use crate::api::sys::ApiResponse;
use crate::api::ApiState;
use crate::config::PeerConfig;
use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct AddPeerReq {
    pub address: String,
    pub token: String,
    pub shareable: Option<bool>,
}

#[derive(Deserialize, Default)]
pub struct ListPeersQuery {
    pub node_id: Option<String>,
}

pub async fn handle_add_peer(State(state): State<ApiState>, Json(payload): Json<AddPeerReq>) -> Result<impl IntoResponse, ApiError> {
    let address = payload.address.trim().to_string();
    let token = payload.token.trim().to_string();
    let shareable = payload.shareable.unwrap_or(true);

    state.coordinator.add_peer(address.clone(), token.clone(), shareable).await;

    let config_to_save = {
        let mut config = state.config.write().await;
        let peers = config.peers.get_or_insert_with(Vec::new);
        if let Some(peer) = peers.iter_mut().find(|peer| peer.address == address) {
            peer.token = token.clone();
        } else {
            peers.push(PeerConfig { address: address.clone(), token: token.clone() });
        }
        config.clone()
    };
    config_to_save.save(&state.config_path).await?;

    Ok((StatusCode::CREATED, Json(ApiResponse { message: "Peer added".to_string(), data: None::<()> })))
}

pub async fn handle_list_peers(
    State(state): State<ApiState>,
    req: axum::extract::Request,
) -> Result<impl IntoResponse, ApiError> {
    let query = axum::extract::Query::<ListPeersQuery>::try_from_uri(req.uri()).unwrap_or_default();
    let req_node_id = query.node_id.clone().unwrap_or_default();

    let auth_header = req.headers().get(axum::http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    let req_token = auth_header.replace("Bearer ", "");
    let is_admin = req_token == state.my_token;

    let config = state.config.read().await;
    
    if !is_admin && !config.share_node.unwrap_or(true) {
        return Ok((StatusCode::OK, Json(ApiResponse { message: "Success".to_string(), data: Some(vec![]) })));
    }

    let mut filtered_peers = Vec::new();

    filtered_peers.push(crate::coordinator::models::PeerStatusResp {
        node_id: state.node_id.clone(),
        best_address: config.public_address.clone().unwrap_or_else(|| format!("127.0.0.1:{}", config.grpc_port.unwrap_or(50051))),
        addresses: vec![],
        token: state.my_token.clone(),
        status: "Local".to_string(),
        latency_ms: 0,
        download_speed: 0.0,
        shareable: config.shareable.unwrap_or(true),
        is_self: true,
        speed_limit_kb: config.global_speed_limit_kb.unwrap_or(0),
        active_connections: 0,
    });

    let my_best_address = config.public_address.clone().unwrap_or_else(|| format!("127.0.0.1:{}", config.grpc_port.unwrap_or(50051)));
    
    for kv in state.coordinator.peers.iter() {
        let p = kv.value();
        
        if p.node_id == req_node_id {
            continue;
        }
        
        // Skip peers that have the same address as the current node to avoid duplicates
        if p.best_address == my_best_address || p.addresses.contains(&my_best_address) {
            continue;
        }
        
        if is_admin || p.shareable {
            filtered_peers.push(crate::coordinator::models::PeerStatusResp {
                node_id: p.node_id.clone(),
                best_address: p.best_address.clone(),
                addresses: p.addresses.clone(),
                token: p.token.clone(),
                status: p.status.clone(),
                latency_ms: p.latency_ms,
                download_speed: p.download_speed,
                shareable: p.shareable,
                is_self: false,
                speed_limit_kb: config.peer_speed_limit_kb.unwrap_or(0),
                active_connections: p.active_connections,
            });
        }
    }

    Ok((StatusCode::OK, Json(ApiResponse { message: "Success".to_string(), data: Some(filtered_peers) })))
}