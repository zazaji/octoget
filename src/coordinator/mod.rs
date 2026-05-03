// src/coordinator/mod.rs
pub mod grpc_ops;
pub mod models;
pub mod peer_ops;
pub mod state_ops;
pub mod task_ops;
pub mod worker;

use crate::config::Config;
use crate::utils::RateLimiter;
use dashmap::DashMap;
use models::{PeerInfo, TaskContext};
use std::sync::Arc;
use tokio::sync::{Mutex as TokioMutex, RwLock};

pub const PIECE_SIZE: u64 = 5 * 1024 * 1024;

pub struct Coordinator {
    pub node_id: String,
    pub record_dir: String,
    pub peers: Arc<DashMap<String, PeerInfo>>,
    pub tasks: DashMap<String, Arc<TaskContext>>,
    pub config: Arc<RwLock<Config>>,
    pub rate_limiter: Arc<TokioMutex<RateLimiter>>,
}

impl Coordinator {
    pub fn new(node_id: String, record_dir: String, config: Arc<RwLock<Config>>) -> Self {
        Self {
            node_id,
            record_dir,
            peers: Arc::new(DashMap::new()),
            tasks: DashMap::new(),
            config,
            rate_limiter: Arc::new(TokioMutex::new(RateLimiter::new())),
        }
    }

    pub fn inc_peer_connection(&self, node_id: &str) {
        if let Some(mut peer) = self.peers.get_mut(node_id) {
            peer.active_connections += 1;
        }
    }

    pub fn dec_peer_connection(&self, node_id: &str) {
        if let Some(mut peer) = self.peers.get_mut(node_id) {
            if peer.active_connections > 0 {
                peer.active_connections -= 1;
            }
        }
    }

    pub async fn find_task_by_url(&self, url: &str) -> Option<String> {
        for kv in self.tasks.iter() {
            if kv.value().original_url == url {
                return Some(kv.key().clone());
            }
        }
        None
    }
}