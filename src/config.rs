// src/config.rs
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PeerConfig {
    pub address: String,
    pub token: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Config {
    pub grpc_port: Option<u16>,
    pub api_port: Option<u16>,
    pub record_dir: Option<String>,
    pub default_save_dir: Option<String>,
    pub my_token: Option<String>,
    pub share_node: Option<bool>,
    pub shareable: Option<bool>,
    pub nat_traversal: Option<bool>,
    pub public_address: Option<String>,
    pub max_connections: Option<u32>,
    pub max_tasks: Option<u32>,
    pub global_speed_limit_kb: Option<u32>,
    pub peer_speed_limit_kb: Option<u32>,
    pub use_proxy: Option<bool>,
    pub log_level: Option<String>,
    pub peers: Option<Vec<PeerConfig>>,
}

impl Config {
    pub async fn load<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        if tokio::fs::try_exists(&path_buf).await.unwrap_or(false) {
            let content = tokio::fs::read_to_string(&path_buf).await?;
            let config = toml::from_str(&content).map_err(|e| {
                tracing::error!("Failed to parse config file {:?}: {}", path_buf, e);
                anyhow::anyhow!("Config parse error: {}", e)
            })?;
            Ok(config)
        } else {
            Ok(Config::default())
        }
    }

    pub async fn save<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        let path_buf = path.as_ref().to_path_buf();
        let content = toml::to_string_pretty(self)?;
        tokio::fs::write(&path_buf, content).await?;
        Ok(())
    }
}