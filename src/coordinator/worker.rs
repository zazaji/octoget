// src/coordinator/worker.rs
use crate::coordinator::models::{Chunk, ChunkStatus, SpeedMonitor, TaskContext, TaskStatus};
use crate::pb::octoget::peer_service_client::PeerServiceClient;
use crate::pb::octoget::ChunkRequest;
use crate::utils::RateLimiter;
use anyhow::Result;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Mutex as TokioMutex};
use tokio_util::sync::CancellationToken;
use tonic::transport::Endpoint;
use md5::{Md5, Digest};

const TAIL_LATENCY_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_RETRIES: u32 = 5;
const MAX_CONSECUTIVE_FAILURES: u32 = 10;

struct ConnectionGuard {
    coordinator: Arc<crate::coordinator::Coordinator>,
    node_id: String,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.coordinator.dec_peer_connection(&self.node_id);
    }
}

pub async fn peer_worker_loop(
    coordinator: Arc<crate::coordinator::Coordinator>,
    node_id: String,
    peer_token: String,
    ctx: Arc<TaskContext>,
    file: Arc<std::fs::File>,
    cancel_token: CancellationToken,
) {
    let concurrency = 3;
    let mut join_set = tokio::task::JoinSet::new();

    for _ in 0..concurrency {
        let coord = coordinator.clone();
        let nid = node_id.clone();
        let ptoken = peer_token.clone();
        let c = ctx.clone();
        let f = file.clone();
        let ct = cancel_token.clone();

        join_set.spawn(async move {
            single_worker_task(coord, nid, ptoken, c, f, ct).await;
        });
    }

    while let Some(_) = join_set.join_next().await {}
}

async fn single_worker_task(
    coordinator: Arc<crate::coordinator::Coordinator>,
    node_id: String,
    peer_token: String,
    ctx: Arc<TaskContext>,
    file: Arc<std::fs::File>,
    cancel_token: CancellationToken,
) {
    coordinator.inc_peer_connection(&node_id);
    let _guard = ConnectionGuard {
        coordinator: coordinator.clone(),
        node_id: node_id.clone(),
    };

    let mut consecutive_failures = 0;
    let peer_speed_monitor = ctx.peer_speeds.entry(node_id.clone())
        .or_insert_with(|| Arc::new(Mutex::new(SpeedMonitor::new()))).value().clone();

    loop {
        if cancel_token.is_cancelled() || *ctx.status.read().await != TaskStatus::Running { break; }
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES { break; }

        let best_addr = coordinator.get_peer_best_address(&node_id).await;
        if best_addr.is_empty() { tokio::time::sleep(Duration::from_secs(1)).await; continue; }

        let my_speed = coordinator.get_peer_speed(&node_id).await;
        let avg_speed = coordinator.get_avg_peer_speed().await;
        if my_speed > 0.0 && avg_speed > 0.0 && my_speed < avg_speed * 0.3 {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        let permit = match ctx.semaphore.clone().acquire_owned().await { Ok(p) => p, Err(_) => break };
        let endpoint = match Endpoint::from_shared(format!("http://{}", best_addr)) { Ok(e) => e, Err(_) => break };
        let channel = match tokio::time::timeout(Duration::from_secs(10), endpoint.connect()).await {
            Ok(Ok(c)) => c,
            _ => {
                consecutive_failures += 1;
                crate::utils::exponential_backoff(consecutive_failures, 500).await;
                drop(permit); continue;
            }
        };

        let token_clone = peer_token.clone();
        let my_node_id = coordinator.node_id.clone();
        let mut client = PeerServiceClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
            if let Ok(meta_val) = tonic::metadata::MetadataValue::try_from(format!("Bearer {}", token_clone)) {
                req.metadata_mut().insert("authorization", meta_val);
            }
            if let Ok(meta_val) = tonic::metadata::MetadataValue::try_from(&my_node_id) {
                req.metadata_mut().insert("x-node-id", meta_val);
            }
            Ok(req)
        });

        loop {
            if cancel_token.is_cancelled() || *ctx.status.read().await != TaskStatus::Running { break; }

            let chunk_opt = { let mut s = ctx.chunks.lock().await; get_next_chunk(&mut s) };
            match chunk_opt {
                Some(chunk) => {
                    if chunk.retries > MAX_RETRIES {
                        let mut s = ctx.chunks.lock().await;
                        s[chunk.index].status = ChunkStatus::Idle; s[chunk.index].retries = 0;
                        consecutive_failures += 1; break; 
                    }

                    let req = ChunkRequest { task_id: ctx.task_id.clone(), url: ctx.final_url.read().await.clone(), start: chunk.start, end: chunk.end };
                    let start_time = Instant::now();

                    tokio::select! {
                        _ = cancel_token.cancelled() => break,
                        res = async {
                            let stream = client.fetch_chunk(req).await?.into_inner();
                            process_chunk_stream(stream, file.clone(), ctx.speed_monitor.clone(), peer_speed_monitor.clone(), coordinator.config.clone(), coordinator.rate_limiter.clone()).await
                        } => {
                            match res {
                                Ok(bytes_downloaded) => {
                                    ctx.chunks.lock().await[chunk.index].status = ChunkStatus::Done;
                                    consecutive_failures = 0; 
                                    let elapsed = start_time.elapsed().as_secs_f64();
                                    if elapsed > 0.0 { coordinator.update_peer_speed(&node_id, bytes_downloaded as f64 / elapsed).await; }
                                }
                                Err(e) => {
                                    tracing::warn!("Chunk download failed: {}", e);
                                    let mut s = ctx.chunks.lock().await;
                                    if let ChunkStatus::InProgress(_) = s[chunk.index].status { s[chunk.index].status = ChunkStatus::Idle; }
                                    
                                    let save_path = ctx.save_path.read().await.clone();
                                    if !save_path.exists() {
                                        println!("[Task] Error: Save file deleted during download for task {}", ctx.task_id);
                                        *ctx.status.write().await = TaskStatus::Failed("Save file deleted during download".to_string());
                                        ctx.cancel_token.read().await.cancel();
                                        break;
                                    }

                                    consecutive_failures += 1;
                                    crate::utils::exponential_backoff(chunk.retries, 1000).await;
                                    break;
                                }
                            }
                        }
                    }
                }
                None => {
                    if ctx.chunks.lock().await.iter().all(|c| c.status == ChunkStatus::Done) { break; }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
        drop(permit);
        if ctx.chunks.lock().await.iter().all(|c| c.status == ChunkStatus::Done) || cancel_token.is_cancelled() { break; }
    }
}

fn get_next_chunk(chunks: &mut Vec<Chunk>) -> Option<Chunk> {
    let (mut target_idx, mut oldest_time) = (None, Instant::now());
    for (i, chunk) in chunks.iter_mut().enumerate() {
        match chunk.status {
            ChunkStatus::Idle => {
                chunk.status = ChunkStatus::InProgress(Instant::now()); chunk.retries += 1;
                return Some(chunk.clone());
            }
            ChunkStatus::InProgress(start_time) => {
                if start_time < oldest_time { oldest_time = start_time; target_idx = Some(i); }
            }
            ChunkStatus::Done => {}
        }
    }
    if let Some(idx) = target_idx {
        if oldest_time.elapsed() > TAIL_LATENCY_TIMEOUT {
            chunks[idx].status = ChunkStatus::InProgress(Instant::now()); chunks[idx].retries += 1;
            return Some(chunks[idx].clone());
        }
    }
    None
}

async fn process_chunk_stream(
    mut stream: tonic::Streaming<crate::pb::octoget::ChunkData>,
    file: Arc<std::fs::File>,
    total_speed_monitor: Arc<Mutex<SpeedMonitor>>,
    peer_speed_monitor: Arc<Mutex<SpeedMonitor>>,
    config: Arc<tokio::sync::RwLock<crate::config::Config>>,
    rate_limiter: Arc<TokioMutex<RateLimiter>>,
) -> Result<u64> {
    let mut total_bytes = 0;
    let limit = config.read().await.global_speed_limit_kb.unwrap_or(0);
    
    let mut buffer = Vec::with_capacity(5 * 1024 * 1024);
    let mut current_write_offset = 0;
    let mut first_chunk = true;

    let mut md5_ctx = Md5::new();

    while let Some(chunk_res) = stream.message().await? {
        if chunk_res.error_msg.starts_with("MD5:") {
            let remote_md5 = &chunk_res.error_msg[4..];
            let local_md5 = format!("{:x}", md5_ctx.clone().finalize());
            if remote_md5 != local_md5 {
                anyhow::bail!("MD5 mismatch: remote {}, local {}", remote_md5, local_md5);
            }
            
            if !buffer.is_empty() {
                let file_clone = file.clone();
                let write_offset = current_write_offset;
                let buf_to_write = std::mem::replace(&mut buffer, Vec::new());
                tokio::task::spawn_blocking(move || crate::utils::write_at(&file_clone, &buf_to_write, write_offset)).await??;
            }
            continue;
        } else if !chunk_res.error_msg.is_empty() { 
            anyhow::bail!("Peer error: {}", chunk_res.error_msg); 
        }
        
        let len = chunk_res.data.len() as u64;
        if len == 0 { continue; }
        
        if first_chunk {
            current_write_offset = chunk_res.offset;
            first_chunk = false;
        }
        
        let sleep_dur = {
            let mut rl = rate_limiter.lock().await;
            rl.acquire(len as usize, limit)
        };
        if sleep_dur.as_secs_f64() > 0.0 {
            tokio::time::sleep(sleep_dur).await;
        }

        md5_ctx.update(&chunk_res.data);
        buffer.extend_from_slice(&chunk_res.data);
        total_bytes += len;
        total_speed_monitor.lock().await.add(len);
        peer_speed_monitor.lock().await.add(len);

        if buffer.len() > 10 * 1024 * 1024 {
            anyhow::bail!("Chunk size exceeded memory limit");
        }
    }

    Ok(total_bytes)
}