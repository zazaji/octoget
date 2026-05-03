// src/coordinator/models.rs
use crate::utils::Checksums;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, Mutex as TokioMutex, RwLock};
use tokio_util::sync::CancellationToken;

pub struct SpeedMonitor {
    pub history: VecDeque<(Instant, u64)>,
}

impl SpeedMonitor {
    pub fn new() -> Self { Self { history: VecDeque::new() } }
    pub fn add(&mut self, bytes: u64) {
        let now = Instant::now();
        self.history.push_back((now, bytes));
        while let Some(&(t, _)) = self.history.front() {
            if now.duration_since(t).as_secs() > 5 { self.history.pop_front(); } else { break; }
        }
    }
    pub fn get_speed(&self) -> f64 {
        let now = Instant::now();
        let valid_history: Vec<_> = self.history.iter().filter(|(t, _)| now.duration_since(*t).as_secs() <= 5).collect();
        if valid_history.len() < 2 { return 0.0; }
        let total_bytes: u64 = valid_history.iter().map(|(_, b)| *b).sum();
        let duration = valid_history.last().unwrap().0.duration_since(valid_history.first().unwrap().0).as_secs_f64();
        if duration < 0.1 { 0.0 } else { total_bytes as f64 / duration }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ChunkStatus { Idle, InProgress(Instant), Done }

#[derive(Clone, Debug)]
pub struct Chunk { pub index: usize, pub start: u64, pub end: u64, pub status: ChunkStatus, pub retries: u32 }

#[derive(Clone, Debug, PartialEq)]
pub enum TaskStatus { Pending, Resolving, Running, Paused, Completed, Failed(String) }

pub struct TaskContext {
    pub task_id: String, pub original_url: String, pub final_url: RwLock<String>,
    pub save_path: RwLock<PathBuf>, pub file_size: RwLock<u64>, pub chunks: TokioMutex<Vec<Chunk>>,
    pub status: RwLock<TaskStatus>, pub cancel_token: RwLock<CancellationToken>,
    pub semaphore: Arc<tokio::sync::Semaphore>, pub speed_monitor: Arc<Mutex<SpeedMonitor>>,
    pub peer_speeds: Arc<DashMap<String, Arc<Mutex<SpeedMonitor>>>>,
    pub checksums: RwLock<Option<Checksums>>,
    pub created_at: RwLock<Option<i64>>,
    pub completed_at: RwLock<Option<i64>>,
    pub custom_file_name: RwLock<Option<String>>,
}

#[derive(Serialize)]
pub struct TaskProgressResp {
    pub task_id: String, pub url: String, pub final_url: String, pub file_name: String, pub save_path: String, pub status: String,
    pub file_size: u64, pub downloaded: u64, pub progress: f64, pub download_speed: f64,
    pub peer_speeds: HashMap<String, f64>,
    pub checksums: Option<Checksums>,
    pub created_at: Option<i64>,
    pub completed_at: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct PeerInfo {
    pub node_id: String, pub addresses: Vec<String>, pub best_address: String,
    pub token: String, pub status: String,
    pub latency_ms: u64, pub download_speed: f64, pub fail_count: u32,
    pub shareable: bool,
    pub active_connections: u32,
}

#[derive(Serialize, Deserialize)]
pub struct PeerStatusResp {
    pub node_id: String, pub best_address: String, pub addresses: Vec<String>,
    pub token: String,
    pub status: String, pub latency_ms: u64, pub download_speed: f64,
    pub shareable: bool,
    pub is_self: bool,
    pub speed_limit_kb: u32,
    pub active_connections: u32,
}

#[derive(Deserialize, Debug)]
pub struct TaskItem { pub url: String, pub file_name: Option<String>, pub force: Option<bool> }

#[derive(Deserialize, Debug)]
pub struct BatchSubmitReq { pub tasks: Vec<TaskItem>, pub save_dir: String }

#[derive(Serialize, Deserialize, Debug)]
pub struct SysInfoResp { pub node_id: String, pub version: String, pub os: String }

#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub enum ChunkStatusState { Idle, Done }

#[derive(Serialize, Deserialize)]
pub struct ChunkState { pub index: usize, pub start: u64, pub end: u64, pub status: ChunkStatusState }

#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub enum TaskStatusState { Pending, Resolving, Running, Paused, Completed, Failed(String) }

#[derive(Serialize, Deserialize)]
pub struct TaskState {
    pub task_id: String, pub original_url: String, pub final_url: String,
    pub save_path: PathBuf, pub file_size: u64, pub chunks: Vec<ChunkState>, pub status: TaskStatusState,
    pub checksums: Option<Checksums>,
    pub custom_file_name: Option<String>,
}