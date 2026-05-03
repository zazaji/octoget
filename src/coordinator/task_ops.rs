// src/coordinator/task_ops.rs
use crate::coordinator::{Coordinator, PIECE_SIZE};
use crate::coordinator::models::{Chunk, ChunkStatus, SpeedMonitor, TaskContext, TaskStatus};
use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex as TokioMutex, RwLock};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

fn spawn_resume_task(coordinator: Arc<Coordinator>, task_id: String) {
    tokio::spawn(async move {
        if let Err(e) = coordinator.do_resume_task(&task_id).await {
            tracing::error!("Failed to start pending task {}: {}", task_id, e);
        }
    });
}

pub async fn check_pending_tasks(coordinator: &Arc<Coordinator>) -> () {
    let max_tasks = coordinator.config.read().await.max_tasks.unwrap_or(16) as usize;
    let mut running_count = 0;
    let mut pending_tasks = Vec::new();

    let tasks: Vec<_> = coordinator.tasks.iter().map(|kv| (kv.key().clone(), kv.value().clone())).collect();

    for (task_id, ctx) in tasks {
        let status = ctx.status.read().await.clone();
        match status {
            TaskStatus::Running | TaskStatus::Resolving => running_count += 1,
            TaskStatus::Pending => pending_tasks.push(task_id),
            _ => {}
        }
    }

    for task_id in pending_tasks {
        if running_count >= max_tasks {
            break;
        }
        running_count += 1;
        spawn_resume_task(coordinator.clone(), task_id);
    }
}

impl Coordinator {

    pub async fn pause_task(self: &Arc<Self>, task_id: &str) -> Result<()> {
        let ctx = self.tasks.get(task_id).map(|r| r.value().clone());
        if let Some(ctx) = ctx {
            {
                let mut status = ctx.status.write().await;
                if *status == TaskStatus::Running || *status == TaskStatus::Pending || *status == TaskStatus::Resolving {
                    ctx.cancel_token.read().await.cancel();
                    *status = TaskStatus::Paused;
                } else {
                    anyhow::bail!("Task is not running or pending");
                }
            }
            
            {
                let mut chunks = ctx.chunks.lock().await;
                for chunk in chunks.iter_mut() {
                    if let ChunkStatus::InProgress(_) = chunk.status { chunk.status = ChunkStatus::Idle; }
                }
            }
            
            info!(task_id = %task_id, "Task paused successfully");
            println!("[Task] Paused task: {}", task_id);
            let _ = self.save_task(task_id).await;
            
            check_pending_tasks(self).await;
            
            Ok(())
        } else { anyhow::bail!("Task not found"); }
    }

    pub async fn resume_task(self: &Arc<Self>, task_id: &str) -> Result<()> {
        let ctx = self.tasks.get(task_id).map(|r| r.value().clone());
        if let Some(ctx) = ctx {
            {
                let mut status = ctx.status.write().await;
                if matches!(*status, TaskStatus::Paused | TaskStatus::Failed(_)) {
                    *status = TaskStatus::Pending;
                } else {
                    anyhow::bail!("Task is not paused or failed");
                }
            }
            let _ = self.save_task(task_id).await;
            check_pending_tasks(self).await;
            Ok(())
        } else {
            anyhow::bail!("Task not found");
        }
    }

    pub(crate) async fn do_resume_task(self: &Arc<Self>, task_id: &str) -> Result<()> {
        let ctx = self.tasks.get(task_id).map(|r| r.value().clone());
        let ctx = if let Some(ctx) = ctx { ctx } else { anyhow::bail!("Task not found"); };
        
        let save_path = ctx.save_path.read().await.clone();
        let file_size = *ctx.file_size.read().await;
        let url = ctx.original_url.clone();
        let save_dir = save_path.parent().unwrap().to_string_lossy().to_string();
        let custom_file_name = ctx.custom_file_name.read().await.clone();

        *ctx.status.write().await = TaskStatus::Resolving;
        let new_token = CancellationToken::new();
        *ctx.cancel_token.write().await = new_token;
        let _ = self.save_task(task_id).await;
        
        let coord = self.clone();
        tokio::spawn(async move {
            if let Err(e) = coord.do_resolve_and_start(ctx, url, custom_file_name, save_dir, file_size).await {
                error!("Failed to resume resolving task: {}", e);
            }
        });
        info!(task_id = %task_id, "Task resumed resolving successfully");
        println!("[Task] Resumed resolving task: {}", task_id);
        return Ok(());
    }

    pub async fn delete_task(self: &Arc<Self>, task_id: &str, delete_completed_file: bool) -> Result<()> {
        let ctx = self.tasks.remove(task_id).map(|(_, v)| v);
        if let Some(ctx) = ctx {
            ctx.cancel_token.read().await.cancel();
            if *ctx.status.read().await != TaskStatus::Completed || delete_completed_file {
                let save_path = ctx.save_path.read().await.clone();
                if let Err(e) = std::fs::remove_file(&save_path) { warn!(path = ?save_path, error = %e, "Failed to delete file"); }
            }
            let record_path = PathBuf::from(&self.record_dir).join(format!("{}.json", task_id));
            let _ = tokio::fs::remove_file(&record_path).await;
            info!(task_id = %task_id, "Task deleted successfully");
            println!("[Task] Deleted task: {}", task_id);
            
            check_pending_tasks(self).await;
            Ok(())
        } else { anyhow::bail!("Task not found"); }
    }

    pub async fn start_task(self: &Arc<Self>, task_id: String, url: String, custom_file_name: Option<String>, save_dir: String) -> Result<()> {
        let dir_path = PathBuf::from(&save_dir);
        if !dir_path.exists() {
            let dir = dir_path.clone();
            tokio::task::spawn_blocking(move || std::fs::create_dir_all(dir)).await??;
        }

        let initial_file_name = custom_file_name.clone()
            .filter(|n| !n.trim().is_empty())
            .unwrap_or_else(|| {
                let mut name = url.split('/').last().unwrap_or("download").split('?').next().unwrap_or("download").to_string();
                if name.is_empty() { name = "download".to_string(); }
                name
            });

        let cancel_token = CancellationToken::new();
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
        let max_connections = self.config.read().await.max_connections.unwrap_or(8) as usize;
        
        let task_ctx = Arc::new(TaskContext {
            task_id: task_id.clone(), original_url: url.clone(), final_url: RwLock::new(url.clone()),
            save_path: RwLock::new(dir_path.join(&initial_file_name)), file_size: RwLock::new(0),
            chunks: TokioMutex::new(Vec::new()), status: RwLock::new(TaskStatus::Pending),
            cancel_token: RwLock::new(cancel_token.clone()), semaphore: Arc::new(tokio::sync::Semaphore::new(max_connections)),
            speed_monitor: Arc::new(tokio::sync::Mutex::new(SpeedMonitor::new())), peer_speeds: Arc::new(dashmap::DashMap::new()),
            checksums: RwLock::new(None),
            created_at: RwLock::new(Some(now)),
            completed_at: RwLock::new(None),
            custom_file_name: RwLock::new(custom_file_name.clone()),
        });

        self.tasks.insert(task_id.clone(), task_ctx.clone());
        let _ = self.save_task(&task_id).await;

        check_pending_tasks(self).await;
        Ok(())
    }

    pub async fn do_resolve_and_start(self: &Arc<Self>, task_ctx: Arc<TaskContext>, url: String, custom_file_name: Option<String>, save_dir: String, old_file_size: u64) -> Result<()> {
        let task_id = task_ctx.task_id.clone();
        let cancel_token = task_ctx.cancel_token.read().await.clone();
        
        let use_proxy = self.config.read().await.use_proxy.unwrap_or(false);
        let client = crate::utils::http_client_builder(use_proxy)
            .redirect(reqwest::redirect::Policy::limited(10))
            .timeout(std::time::Duration::from_secs(10))
            .build()?;

        let coord = self.clone();
        let url_clone = url.clone();
        let resolve_future = async move {
            let mut resolved_info = crate::utils::resolve_url(&client, &url_clone).await.ok();
            if resolved_info.is_none() {
                println!("[Task] Local resolve failed for {}, trying via peers...", url_clone);
                for (grpc_addr, token) in coord.get_peers_grpc().await {
                    if let Ok(info) = coord.resolve_url_via_peer_grpc(&url_clone, &grpc_addr, &token).await {
                        println!("[Task] Resolved via peer {}: size {}", grpc_addr, info.file_size);
                        resolved_info = Some(info); break;
                    }
                }
            }
            resolved_info
        };

        let resolved_info = tokio::select! {
            _ = cancel_token.cancelled() => {
                *task_ctx.status.write().await = TaskStatus::Paused;
                let _ = self.save_task(&task_id).await;
                check_pending_tasks(self).await;
                anyhow::bail!("Task paused during resolving");
            }
            res = resolve_future => res,
        };

        let info = match resolved_info {
            Some(i) if i.file_size > 0 => i,
            _ => {
                *task_ctx.status.write().await = TaskStatus::Failed("Failed to resolve URL or size is 0".to_string());
                let _ = self.save_task(&task_id).await;
                check_pending_tasks(self).await;
                anyhow::bail!("Failed to resolve URL");
            }
        };

        let dir_path = PathBuf::from(&save_dir);
        let file_name = custom_file_name.clone().filter(|n| !n.trim().is_empty())
            .unwrap_or_else(|| {
                let mut name = url.split('/').last().unwrap_or("download").split('?').next().unwrap_or("download").to_string();
                if name.is_empty() || name == "download" { 
                    name = info.final_url.split('/').last().unwrap_or("download").split('?').next().unwrap_or("download").to_string();
                }
                if name.is_empty() { name = "download".to_string(); }
                name
            });
        let final_save_path = dir_path.join(file_name);
        let final_url = info.final_url.clone();

        *task_ctx.final_url.write().await = final_url.clone();
        *task_ctx.file_size.write().await = info.file_size;
        *task_ctx.save_path.write().await = final_save_path.clone();

        let mut reset_progress = false;
        if old_file_size > 0 && info.file_size != old_file_size {
            warn!("File size changed from {} to {}, resetting progress", old_file_size, info.file_size);
            reset_progress = true;
        } else if old_file_size == 0 {
            reset_progress = true;
        }

        if !reset_progress {
            if !final_save_path.exists() {
                warn!("Save file missing, resetting progress");
                reset_progress = true;
            } else {
                let actual_size = tokio::fs::metadata(&final_save_path).await?.len();
                if actual_size == 0 || (actual_size < info.file_size && actual_size < 1024) {
                    warn!("File is empty or too small, resetting progress");
                    reset_progress = true;
                }
            }
        }

        let piece_size = if info.support_range { PIECE_SIZE } else { info.file_size };
        
        if reset_progress {
            let mut chunks = Vec::new();
            let (mut current, mut index) = (0, 0);
            while current < info.file_size {
                let end = std::cmp::min(current + piece_size - 1, info.file_size - 1);
                chunks.push(Chunk { index, start: current, end, status: ChunkStatus::Idle, retries: 0 });
                current += piece_size; index += 1;
            }
            *task_ctx.chunks.lock().await = chunks.clone();
            
            let final_save_path_clone = final_save_path.clone();
            let file_size_clone = info.file_size;
            tokio::task::spawn_blocking(move || -> Result<()> {
                let f = std::fs::OpenOptions::new().write(true).create(true).truncate(true).open(&final_save_path_clone)?;
                f.set_len(file_size_clone)?;
                Ok(())
            }).await??;
        } else {
            let mut chunks = task_ctx.chunks.lock().await;
            for chunk in chunks.iter_mut() {
                if let ChunkStatus::InProgress(_) = chunk.status {
                    chunk.status = ChunkStatus::Idle;
                    chunk.retries = 0;
                }
            }
        }

        let peers_grpc = self.get_peers_grpc().await;
        for (grpc_addr, token) in &peers_grpc {
            if let Err(e) = self.dispatch_task_to_peer(&task_id, &url, &final_url, info.file_size, info.support_range, grpc_addr, token).await {
                warn!(task_id = %task_id, peer = %grpc_addr, error = %e, "Failed to dispatch task to peer");
            }
        }

        let final_save_path_clone = final_save_path.clone();
        let file = tokio::task::spawn_blocking(move || -> Result<std::fs::File> {
            Ok(std::fs::OpenOptions::new().write(true).open(&final_save_path_clone)?)
        }).await??;
        let shared_file = Arc::new(file);

        *task_ctx.status.write().await = TaskStatus::Running;
        let _ = self.save_task(&task_id).await;
        println!("[Task] Started task: {} (Save path: {:?})", task_id, final_save_path);

        let peers = self.get_peers().await;
        let mut join_set = JoinSet::new();
        for (node_id, peer_token) in peers {
            let (ctx, file_clone, token_clone, coord) = (task_ctx.clone(), shared_file.clone(), cancel_token.clone(), self.clone());
            join_set.spawn(async move { crate::coordinator::worker::peer_worker_loop(coord, node_id, peer_token, ctx, file_clone, token_clone).await; });
        }

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    join_set.abort_all();
                    break;
                }
                res = join_set.join_next() => {
                    match res {
                        Some(Err(e)) => error!("Worker task panicked: {:?}", e),
                        Some(Ok(_)) => {}
                        None => break,
                    }
                }
            }
        }

        if *task_ctx.status.read().await == TaskStatus::Running {
            let all_done = {
                let chunks = task_ctx.chunks.lock().await;
                chunks.iter().all(|c| c.status == ChunkStatus::Done)
            };
            
            if all_done {
                let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
                *task_ctx.completed_at.write().await = Some(now);
                *task_ctx.status.write().await = TaskStatus::Completed;
                info!(path = ?final_save_path, "Task completed successfully");
                println!("[Task] Completed task: {} (Path: {:?})", task_id, final_save_path);
                
                let ctx_clone = task_ctx.clone();
                let coord_clone = self.clone();
                let path_clone = final_save_path.clone();
                let tid_clone = task_id.clone();
                tokio::spawn(async move {
                    if let Ok(checksums) = crate::utils::calculate_checksums(&path_clone).await {
                        *ctx_clone.checksums.write().await = Some(checksums);
                        let _ = coord_clone.save_task(&tid_clone).await;
                    }
                });
            } else {
                *task_ctx.status.write().await = TaskStatus::Failed("All peers disconnected".to_string());
                error!(path = ?final_save_path, "Task failed: All peers disconnected");
                println!("[Task] Failed task: {} (All peers disconnected)", task_id);
            }
            let _ = self.save_task(&task_id).await;
            check_pending_tasks(self).await;
        }
        Ok(())
    }
}