// src/coordinator/state_ops.rs
use super::Coordinator;
use crate::coordinator::models::{
    Chunk, ChunkState, ChunkStatus, ChunkStatusState, SpeedMonitor, TaskContext, TaskProgressResp,
    TaskState, TaskStatus, TaskStatusState,
};
use anyhow::Result;
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::warn;

impl Coordinator {
    pub async fn save_task(&self, task_id: &str) -> Result<()> {
        let state = {
            if let Some(ctx) = self.tasks.get(task_id).map(|r| r.value().clone()) {
                let chunk_states: Vec<ChunkState> = {
                    let chunks = ctx.chunks.lock().await;
                    chunks.iter().map(|c| ChunkState {
                        index: c.index, start: c.start, end: c.end,
                        status: match c.status { ChunkStatus::Done => ChunkStatusState::Done, _ => ChunkStatusState::Idle }
                    }).collect()
                };

                let status_state = {
                    let status = ctx.status.read().await;
                    match &*status {
                        TaskStatus::Pending => TaskStatusState::Pending,
                        TaskStatus::Resolving => TaskStatusState::Resolving,
                        TaskStatus::Running => TaskStatusState::Running,
                        TaskStatus::Paused => TaskStatusState::Paused,
                        TaskStatus::Completed => TaskStatusState::Completed,
                        TaskStatus::Failed(e) => TaskStatusState::Failed(e.clone()),
                    }
                };

                Some(TaskState {
                    task_id: ctx.task_id.clone(), original_url: ctx.original_url.clone(),
                    final_url: ctx.final_url.read().await.clone(), save_path: ctx.save_path.read().await.clone(),
                    file_size: *ctx.file_size.read().await, chunks: chunk_states, status: status_state,
                    checksums: ctx.checksums.read().await.clone(),
                    custom_file_name: ctx.custom_file_name.read().await.clone(),
                })
            } else {
                None
            }
        };

        if let Some(state) = state {
            let dir_path = PathBuf::from(&self.record_dir);
            if !dir_path.exists() { tokio::fs::create_dir_all(&dir_path).await?; }
            let json = serde_json::to_string_pretty(&state)?;
            let path = dir_path.join(format!("{}.json", task_id));
            let tmp_path = path.with_extension("tmp");
            tokio::fs::write(&tmp_path, json).await?;
            tokio::fs::rename(tmp_path, path).await?;
        }
        Ok(())
    }

    pub async fn save_all_tasks(&self) -> Result<()> {
        let task_ids: Vec<String> = self.tasks.iter().map(|kv| kv.key().clone()).collect();
        for tid in task_ids { let _ = self.save_task(&tid).await; }
        Ok(())
    }

    pub async fn load_tasks(self: &Arc<Self>) -> Result<()> {
        let dir_path = PathBuf::from(&self.record_dir);
        if !dir_path.exists() { tokio::fs::create_dir_all(&dir_path).await?; return Ok(()); }

        let mut entries = tokio::fs::read_dir(&dir_path).await?;
        let max_connections = self.config.read().await.max_connections.unwrap_or(8) as usize;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
                let json = tokio::fs::read_to_string(&path).await?;
                if let Ok(state) = serde_json::from_str::<TaskState>(&json) {
                    let mut chunks: Vec<Chunk> = state.chunks.into_iter().map(|cs| Chunk {
                        index: cs.index, start: cs.start, end: cs.end, retries: 0,
                        status: match cs.status { ChunkStatusState::Done => ChunkStatus::Done, _ => ChunkStatus::Idle }
                    }).collect();

                    let mut status = match state.status {
                        TaskStatusState::Pending | TaskStatusState::Resolving | TaskStatusState::Running => {
                            TaskStatus::Pending
                        },
                        TaskStatusState::Paused => TaskStatus::Paused,
                        TaskStatusState::Completed => TaskStatus::Completed,
                        TaskStatusState::Failed(e) => TaskStatus::Failed(e),
                    };

                    if state.file_size == 0 {
                        status = TaskStatus::Pending;
                    } else if !tokio::fs::try_exists(&state.save_path).await.unwrap_or(false) {
                        warn!("File missing for task {}, resetting progress", state.task_id);
                        for c in &mut chunks { c.status = ChunkStatus::Idle; }
                        status = TaskStatus::Paused;
                    }

                    let ctx = Arc::new(TaskContext {
                        task_id: state.task_id.clone(), original_url: state.original_url,
                        final_url: RwLock::new(state.final_url), save_path: RwLock::new(state.save_path),
                        file_size: RwLock::new(state.file_size), chunks: Mutex::new(chunks),
                        status: RwLock::new(status.clone()), cancel_token: RwLock::new(CancellationToken::new()),
                        semaphore: Arc::new(tokio::sync::Semaphore::new(max_connections)),
                        speed_monitor: Arc::new(Mutex::new(SpeedMonitor::new())),
                        peer_speeds: Arc::new(DashMap::new()),
                        checksums: RwLock::new(state.checksums),
                        created_at: RwLock::new(None),
                        completed_at: RwLock::new(None),
                        custom_file_name: RwLock::new(state.custom_file_name),
                    });

                    self.tasks.insert(state.task_id.clone(), ctx);
                }
            }
        }
        println!("[System] Loaded {} tasks from persistence", self.tasks.len());
        Ok(())
    }

    pub async fn get_task_progress(&self, task_id: &str) -> Option<TaskProgressResp> {
        let ctx = self.tasks.get(task_id).map(|r| r.value().clone());
        if let Some(ctx) = ctx {
            let mut status = ctx.status.read().await.clone();
            let save_path = ctx.save_path.read().await.clone();

            let exists = tokio::fs::try_exists(&save_path).await.unwrap_or(false);
            if !exists && *ctx.file_size.read().await > 0 {
                if status == TaskStatus::Running || status == TaskStatus::Paused || status == TaskStatus::Pending {
                    drop(status);
                    let mut w_status = ctx.status.write().await;
                    *w_status = TaskStatus::Failed("Save file missing".to_string());
                    status = w_status.clone();
                    ctx.cancel_token.read().await.cancel();
                } else if status == TaskStatus::Completed {
                    drop(status);
                    let mut w_status = ctx.status.write().await;
                    *w_status = TaskStatus::Failed("Completed file missing".to_string());
                    status = w_status.clone();
                }
            }

            let downloaded: u64 = {
                let chunks = ctx.chunks.lock().await;
                chunks.iter().filter(|c| c.status == ChunkStatus::Done).map(|c| c.end - c.start + 1).sum()
            };
            let file_size = *ctx.file_size.read().await;
            let progress = if file_size > 0 { (downloaded as f64 / file_size as f64) * 100.0 } else { 0.0 };
            
            let custom_name = ctx.custom_file_name.read().await.clone();
            let file_name = custom_name
                .filter(|n| !n.trim().is_empty())
                .unwrap_or_else(|| save_path.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_else(|| "unknown".to_string()));
            let save_path_str = save_path.to_string_lossy().to_string();

            let status_str = match &status {
                TaskStatus::Pending => "Pending", TaskStatus::Resolving => "Resolving",
                TaskStatus::Running => "Running", TaskStatus::Paused => "Paused",
                TaskStatus::Completed => "Completed", TaskStatus::Failed(err) => return Some(TaskProgressResp {
                    task_id: ctx.task_id.clone(), url: ctx.original_url.clone(), final_url: ctx.final_url.read().await.clone(),
                    file_name, save_path: save_path_str,
                    status: format!("Failed: {}", err), file_size, downloaded, progress, download_speed: 0.0, peer_speeds: std::collections::HashMap::new(),
                    checksums: ctx.checksums.read().await.clone(),
                    created_at: *ctx.created_at.read().await,
                    completed_at: *ctx.completed_at.read().await,
                }),
            }.to_string();

            let download_speed = if status == TaskStatus::Running { ctx.speed_monitor.lock().await.get_speed() } else { 0.0 };
            let mut peer_speeds = std::collections::HashMap::new();
            if status == TaskStatus::Running {
                for kv in ctx.peer_speeds.iter() {
                    let speed = kv.value().lock().await.get_speed();
                    if speed > 0.0 { 
                        let node_id = kv.key();
                        let ip = if node_id == &self.node_id {
                            "Local".to_string()
                        } else if let Some(peer) = self.peers.get(node_id) {
                            let addr = peer.best_address.clone();
                            if addr.is_empty() {
                                peer.addresses.first().cloned().unwrap_or_else(|| node_id.clone())
                            } else {
                                addr
                            }
                        } else {
                            node_id.clone()
                        };
                        peer_speeds.insert(ip, speed); 
                    }
                }
            }

            Some(TaskProgressResp {
                task_id: ctx.task_id.clone(), url: ctx.original_url.clone(), final_url: ctx.final_url.read().await.clone(),
                file_name, save_path: save_path_str,
                status: status_str, file_size, downloaded, progress, download_speed, peer_speeds,
                checksums: ctx.checksums.read().await.clone(),
                created_at: *ctx.created_at.read().await,
                completed_at: *ctx.completed_at.read().await,
            })
        } else { None }
    }

    pub async fn get_all_tasks_progress(&self) -> Vec<TaskProgressResp> {
        let task_ids: Vec<String> = self.tasks.iter().map(|kv| kv.key().clone()).collect();
        let mut res = Vec::new();
        for tid in task_ids {
            if let Some(prog) = self.get_task_progress(&tid).await { res.push(prog); }
        }
        res
    }
}