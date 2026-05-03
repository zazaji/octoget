// src/worker.rs
use crate::pb::octoget::peer_service_server::PeerService;
use crate::pb::octoget::{ChunkData, ChunkRequest, DispatchTaskRequest, DispatchTaskResponse, AssignChunksRequest, AssignChunksResponse, TaskStatusRequest, TaskStatusResponse, ResolveUrlRequest, ResolveUrlResponse, GetPeersRequest, GetPeersResponse, SharePeerRequest, SharePeerResponse, PeerInfo as ProtoPeerInfo};
use crate::config::Config;
use crate::utils::{RateLimiter, http_client_builder, resolve_url};
use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, HeaderValue, RANGE};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock, Mutex as TokioMutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{error, instrument};
use md5::{Md5, Digest};

#[derive(Clone)]
struct PeerTask {
    url: String,
    final_url: String,
    file_size: u64,
    support_range: bool,
    chunks: Vec<(u32, u64, u64)>,
    downloaded: u64,
}

pub struct WorkerNode {
    my_token: String,
    config: Arc<RwLock<Config>>,
    client_state: Arc<RwLock<(bool, reqwest::Client)>>,
    tasks: Arc<RwLock<HashMap<String, PeerTask>>>,
    peers: Option<Arc<DashMap<String, crate::coordinator::models::PeerInfo>>>,
    my_node_id: String,
    rate_limiter: Arc<TokioMutex<RateLimiter>>,
    connection_semaphore: Arc<tokio::sync::Semaphore>,
}

impl WorkerNode {
    pub fn new(my_token: String, config: Arc<RwLock<Config>>, max_connections: u32) -> Self {
        let initial_client = http_client_builder(false)
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .connect_timeout(std::time::Duration::from_secs(10))
            .tcp_nodelay(true)
            .build()
            .unwrap();

        Self {
            my_token,
            config,
            client_state: Arc::new(RwLock::new((false, initial_client))),
            tasks: Arc::new(RwLock::new(HashMap::new())),
            peers: None,
            my_node_id: String::new(),
            rate_limiter: Arc::new(TokioMutex::new(RateLimiter::new())),
            connection_semaphore: Arc::new(tokio::sync::Semaphore::new(max_connections as usize)),
        }
    }

    pub fn with_peers(mut self, peers: Arc<DashMap<String, crate::coordinator::models::PeerInfo>>, my_node_id: String) -> Self {
        self.peers = Some(peers);
        self.my_node_id = my_node_id;
        self
    }

    fn inject_node_id<T>(&self, mut resp: Response<T>) -> Response<T> {
        if let Ok(val) = tonic::metadata::MetadataValue::try_from(&self.my_node_id) {
            resp.metadata_mut().insert("x-node-id", val);
        }
        resp
    }
}

#[tonic::async_trait]
impl PeerService for WorkerNode {
    type FetchChunkStream = ReceiverStream<Result<ChunkData, Status>>;

    #[instrument(skip(self, request), fields(task_id = %request.get_ref().task_id))]
    async fn fetch_chunk(&self, request: Request<ChunkRequest>) -> Result<Response<Self::FetchChunkStream>, Status> {
        if let Some(meta_token) = request.metadata().get("authorization") {
            if meta_token.to_str().unwrap_or("") != format!("Bearer {}", self.my_token) {
                return Err(Status::unauthenticated("Invalid token"));
            }
        } else {
            return Err(Status::unauthenticated("Missing token"));
        }

        let permit = match self.connection_semaphore.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => return Err(Status::resource_exhausted("Max connections reached")),
        };

        let requester_node_id = request.metadata().get("x-node-id").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
        let is_self = requester_node_id == self.my_node_id;

        let req = request.into_inner();
        let (tx, rx) = mpsc::channel(32);
        let config = self.config.clone();
        let client_state = self.client_state.clone();
        let rate_limiter = self.rate_limiter.clone();

        tokio::spawn(async move {
            let _permit = permit; 
            let use_proxy = config.read().await.use_proxy.unwrap_or(false);
            let client = {
                let mut state = client_state.write().await;
                if state.0 != use_proxy {
                    state.0 = use_proxy;
                    state.1 = http_client_builder(use_proxy)
                        .pool_idle_timeout(std::time::Duration::from_secs(90))
                        .connect_timeout(std::time::Duration::from_secs(10))
                        .tcp_nodelay(true)
                        .build()
                        .unwrap();
                }
                state.1.clone()
            };

            if let Err(e) = process_chunk(req, tx.clone(), client, config, rate_limiter, is_self).await {
                error!("Chunk processing failed: {:?}", e);
                let _ = tx.send(Ok(ChunkData { data: vec![], offset: 0, error_msg: e.to_string() })).await;
            }
        });

        Ok(self.inject_node_id(Response::new(ReceiverStream::new(rx))))
    }

    async fn dispatch_task(&self, request: Request<DispatchTaskRequest>) -> Result<Response<DispatchTaskResponse>, Status> {
        if let Some(meta_token) = request.metadata().get("authorization") {
            if meta_token.to_str().unwrap_or("") != format!("Bearer {}", self.my_token) {
                return Err(Status::unauthenticated("Invalid token"));
            }
        } else {
            return Err(Status::unauthenticated("Missing token"));
        }

        let req = request.into_inner();
        let mut tasks = self.tasks.write().await;

        if tasks.contains_key(&req.task_id) {
            return Ok(self.inject_node_id(Response::new(DispatchTaskResponse {
                success: false,
                message: format!("Task {} already exists", req.task_id),
            })));
        }

        tasks.insert(req.task_id.clone(), PeerTask {
            url: req.url.clone(),
            final_url: req.final_url.clone(),
            file_size: req.file_size,
            support_range: req.support_range,
            chunks: Vec::new(),
            downloaded: 0,
        });

        Ok(self.inject_node_id(Response::new(DispatchTaskResponse {
            success: true,
            message: format!("Task {} dispatched successfully", req.task_id),
        })))
    }

    async fn assign_chunks(&self, request: Request<AssignChunksRequest>) -> Result<Response<AssignChunksResponse>, Status> {
        if let Some(meta_token) = request.metadata().get("authorization") {
            if meta_token.to_str().unwrap_or("") != format!("Bearer {}", self.my_token) {
                return Err(Status::unauthenticated("Invalid token"));
            }
        } else {
            return Err(Status::unauthenticated("Missing token"));
        }

        let req = request.into_inner();
        let chunk_count = req.chunks.len();
        let mut tasks = self.tasks.write().await;

        let task = match tasks.get_mut(&req.task_id) {
            Some(t) => t,
            None => {
                return Ok(self.inject_node_id(Response::new(AssignChunksResponse {
                    success: false,
                    message: format!("Task {} not found", req.task_id),
                })));
            }
        };

        for chunk in req.chunks {
            task.chunks.push((chunk.index, chunk.start, chunk.end));
        }

        Ok(self.inject_node_id(Response::new(AssignChunksResponse {
            success: true,
            message: format!("Assigned {} chunks to task {}", chunk_count, req.task_id),
        })))
    }

    async fn get_task_status(&self, request: Request<TaskStatusRequest>) -> Result<Response<TaskStatusResponse>, Status> {
        if let Some(meta_token) = request.metadata().get("authorization") {
            if meta_token.to_str().unwrap_or("") != format!("Bearer {}", self.my_token) {
                return Err(Status::unauthenticated("Invalid token"));
            }
        } else {
            return Err(Status::unauthenticated("Missing token"));
        }

        let req = request.into_inner();
        let tasks = self.tasks.read().await;

        let task = match tasks.get(&req.task_id) {
            Some(t) => t,
            None => {
                return Ok(self.inject_node_id(Response::new(TaskStatusResponse {
                    status: "NotFound".to_string(),
                    downloaded: 0,
                    file_size: 0,
                })));
            }
        };

        let status = if task.chunks.is_empty() {
            "Pending"
        } else if task.downloaded >= task.file_size {
            "Completed"
        } else {
            "Downloading"
        };

        Ok(self.inject_node_id(Response::new(TaskStatusResponse {
            status: status.to_string(),
            downloaded: task.downloaded,
            file_size: task.file_size,
        })))
    }

    async fn resolve_url(&self, request: Request<ResolveUrlRequest>) -> Result<Response<ResolveUrlResponse>, Status> {
        if let Some(meta_token) = request.metadata().get("authorization") {
            if meta_token.to_str().unwrap_or("") != format!("Bearer {}", self.my_token) {
                return Err(Status::unauthenticated("Invalid token"));
            }
        } else {
            return Err(Status::unauthenticated("Missing token"));
        }

        let req = request.into_inner();
        let use_proxy = self.config.read().await.use_proxy.unwrap_or(false);
        let client = http_client_builder(use_proxy)
            .redirect(reqwest::redirect::Policy::limited(10))
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| Status::internal(format!("Failed to create client: {}", e)))?;

        match resolve_url(&client, &req.url).await {
            Ok(info) => Ok(self.inject_node_id(Response::new(ResolveUrlResponse {
                final_url: info.final_url,
                file_size: info.file_size,
                support_range: info.support_range,
            }))),
            Err(e) => Err(Status::internal(format!("Failed to resolve URL: {}", e))),
        }
    }

    async fn get_peers(&self, request: Request<GetPeersRequest>) -> Result<Response<GetPeersResponse>, Status> {
        if let Some(meta_token) = request.metadata().get("authorization") {
            if meta_token.to_str().unwrap_or("") != format!("Bearer {}", self.my_token) {
                return Err(Status::unauthenticated("Invalid token"));
            }
        } else {
            return Err(Status::unauthenticated("Missing token"));
        }

        let req = request.into_inner();
        let requester_node_id = req.requester_node_id;

        let config = self.config.read().await;
        let share_node = config.share_node.unwrap_or(true);

        let mut peers_list = Vec::new();

        if share_node {
            if let Some(peers_map) = &self.peers {
                for kv in peers_map.iter() {
                    let p = kv.value();

                    if p.node_id == self.my_node_id || p.node_id == requester_node_id {
                        continue;
                    }

                    if p.shareable {
                        peers_list.push(ProtoPeerInfo {
                            node_id: p.node_id.clone(),
                            best_address: p.best_address.clone(),
                            addresses: p.addresses.clone(),
                            api_addresses: Vec::new(),
                            token: p.token.clone(),
                            status: p.status.clone(),
                            latency_ms: p.latency_ms,
                            download_speed: p.download_speed,
                            shareable: p.shareable,
                        });
                    }
                }
            }
        }

        Ok(self.inject_node_id(Response::new(GetPeersResponse { peers: peers_list })))
    }

    async fn share_peer(&self, request: Request<SharePeerRequest>) -> Result<Response<SharePeerResponse>, Status> {
        if let Some(meta_token) = request.metadata().get("authorization") {
            if meta_token.to_str().unwrap_or("") != format!("Bearer {}", self.my_token) {
                return Err(Status::unauthenticated("Invalid token"));
            }
        } else {
            return Err(Status::unauthenticated("Missing token"));
        }

        let req = request.into_inner();

        if let Some(peers_map) = &self.peers {
            if let Some(mut existing) = peers_map.get_mut(&req.node_id) {
                if !existing.addresses.contains(&req.address) {
                    existing.addresses.push(req.address.clone());
                }
                existing.shareable = req.shareable;
                println!("[gRPC SharePeer] Updated peer: {} (shareable: {})", req.node_id, req.shareable);
            } else {
                peers_map.insert(req.node_id.clone(), crate::coordinator::models::PeerInfo {
                    node_id: req.node_id.clone(),
                    addresses: vec![req.address.clone()],
                    best_address: req.address.clone(),
                    token: req.token.clone(),
                    status: "Discovered".to_string(),
                    latency_ms: 0,
                    download_speed: 0.0,
                    fail_count: 0,
                    shareable: req.shareable,
                    active_connections: 0,
                });
                println!("[gRPC SharePeer] Added new peer: {} (shareable: {})", req.node_id, req.shareable);
            }

            Ok(self.inject_node_id(Response::new(SharePeerResponse {
                success: true,
                message: format!("Peer {} shared successfully", req.node_id),
            })))
        } else {
            Ok(self.inject_node_id(Response::new(SharePeerResponse {
                success: false,
                message: "Peers map not available".to_string(),
            })))
        }
    }
}

async fn process_chunk(
    req: ChunkRequest,
    tx: mpsc::Sender<Result<ChunkData, Status>>,
    client: reqwest::Client,
    config: Arc<RwLock<Config>>,
    rate_limiter: Arc<TokioMutex<RateLimiter>>,
    is_self: bool,
) -> Result<()> {
    let range_val = format!("bytes={}-{}", req.start, req.end);
    let mut headers = HeaderMap::new();
    headers.insert(RANGE, HeaderValue::from_str(&range_val)?);

    let resp = crate::utils::send_http_request(&client, reqwest::Method::GET, &req.url, headers, None).await?;

    if !resp.status().is_success() {
        anyhow::bail!("HTTP request failed with status: {}", resp.status());
    }

    let mut stream = resp.bytes_stream();
    let mut current_offset = req.start;
    let limit = if is_self { 0 } else { config.read().await.peer_speed_limit_kb.unwrap_or(0) };

    let mut md5_ctx = Md5::new();

    while let Some(chunk_res) = stream.next().await {
        let chunk: Bytes = chunk_res?;
        let len = chunk.len();
        
        let sleep_dur = {
            let mut rl = rate_limiter.lock().await;
            rl.acquire(len as usize, limit)
        };
        if sleep_dur.as_secs_f64() > 0.0 {
            tokio::time::sleep(sleep_dur).await;
        }

        md5_ctx.update(&chunk);

        let data = ChunkData { data: chunk.to_vec(), offset: current_offset, error_msg: String::new() };
        
        if tx.send(Ok(data)).await.is_err() {
            anyhow::bail!("Peer disconnected during stream");
        }
        current_offset += len as u64;
    }

    let hash = format!("{:x}", md5_ctx.finalize());
    let eof_data = ChunkData { data: vec![], offset: current_offset, error_msg: format!("MD5:{}", hash) };
    let _ = tx.send(Ok(eof_data)).await;

    Ok(())
}