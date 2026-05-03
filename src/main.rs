// src/main.rs
mod api;
mod config;
mod coordinator;
mod frontend;
mod pb;
mod utils;
mod worker;

use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, reload};

use crate::utils::LogReloadHandle;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long)]
    config: Option<String>,
    #[arg(long)]
    grpc_port: Option<u16>,
    #[arg(long)]
    api_port: Option<u16>,
    #[arg(long)]
    record_dir: Option<String>,
    #[arg(long)]
    max_connections: Option<u32>,
    #[arg(long, env = "DISTGET_TOKEN")]
    token: Option<String>,
}

fn init_tracing(default_level: &str) -> LogReloadHandle {
    let filter = EnvFilter::try_new(default_level).unwrap_or_else(|_| EnvFilter::new("info"));
    let (filter_layer, reload_handle) = reload::Layer::new(filter);
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .init();
    reload_handle
}

fn default_config_path() -> String {
    let cwd_config = PathBuf::from("config.toml");
    if cwd_config.exists() {
        return cwd_config.to_string_lossy().to_string();
    }

    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            let sibling_config = exe_dir.join("config.toml");
            if sibling_config.exists() {
                return sibling_config.to_string_lossy().to_string();
            }
        }
    }

    cwd_config.to_string_lossy().to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let config_path = cli.config.unwrap_or_else(default_config_path);
    let mut file_config = config::Config::load(&config_path).await.unwrap_or_else(|e| {
        eprintln!("Configuration error: {}", e);
        std::process::exit(1);
    });

    let log_level = file_config.log_level.clone().unwrap_or_else(|| "info".to_string());
    let log_reload_handle = Arc::new(init_tracing(&log_level));

    println!("[System] Using config file: {}", config_path);

    let grpc_port = cli.grpc_port.or(file_config.grpc_port).unwrap_or(50051);
    let api_port = cli.api_port.or(file_config.api_port).unwrap_or(50052);
    let record_dir = cli.record_dir.clone().or(file_config.record_dir.clone()).unwrap_or_else(|| "./octoget_records".to_string());
    let my_token = cli.token.clone().or(file_config.my_token.clone()).unwrap_or_else(|| "default_secret_token".to_string());
    let max_connections = cli.max_connections.or(file_config.max_connections).unwrap_or(8);
    let max_tasks = file_config.max_tasks.unwrap_or(16);
    
    if file_config.default_save_dir.is_none() {
        file_config.default_save_dir = Some(
            dirs::download_dir()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|| "./downloads".to_string())
        );
    }

    let share_node = file_config.share_node.unwrap_or(true);
    let shareable = file_config.shareable.unwrap_or(true);
    let nat_traversal = file_config.nat_traversal.unwrap_or(false);
    let public_address = file_config.public_address.clone();
    let peers_config = file_config.peers.clone();

    file_config.grpc_port = Some(grpc_port);
    file_config.api_port = Some(api_port);
    file_config.record_dir = Some(record_dir.clone());
    file_config.my_token = Some(my_token.clone());
    file_config.share_node = Some(share_node);
    file_config.shareable = Some(shareable);
    file_config.nat_traversal = Some(nat_traversal);
    file_config.max_connections = Some(max_connections);
    file_config.max_tasks = Some(max_tasks);
    file_config.log_level = Some(log_level);

    let shared_config = Arc::new(RwLock::new(file_config));
    let node_id = utils::get_or_create_node_id(&record_dir).await?;

    println!("[System] Starting OctoGet Symmetric Node...");
    println!("[System] Node ID: {}", node_id);
    println!("[System] gRPC Port: {}, API Port: {}", grpc_port, api_port);

    if nat_traversal && public_address.is_none() {
        let port = grpc_port;
        tokio::spawn(async move {
            let upnp_future = tokio::task::spawn_blocking(move || utils::setup_upnp(port));
            if let Ok(Ok(Some(addr))) = tokio::time::timeout(std::time::Duration::from_secs(10), upnp_future).await {
                println!("[System] UPnP setup successful: {}", addr);
            } else {
                println!("[System] UPnP setup timed out or failed.");
            }
        });
    }

    let final_public_address = public_address.unwrap_or_else(|| format!("127.0.0.1:{}", grpc_port));

    let coordinator = Arc::new(coordinator::Coordinator::new(
        node_id.clone(), 
        record_dir.clone(),
        shared_config.clone()
    ));
    
    let coord_clone = coordinator.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            if let Err(e) = coord_clone.save_all_tasks().await {
                tracing::error!("Failed to save tasks: {}", e);
            }
        }
    });

    let coordinator_for_api = coordinator.clone();
    let my_token_for_api = my_token.clone();
    let node_id_for_api = node_id.clone();
    let shared_config_for_api = shared_config.clone();
    let config_path_for_api = config_path.clone();
    let log_reload_handle_for_api = log_reload_handle.clone();
    
    let api_server = tokio::spawn(async move {
        api::start_api_server(
            coordinator_for_api, 
            api_port, 
            my_token_for_api, 
            node_id_for_api, 
            shared_config_for_api, 
            config_path_for_api,
            log_reload_handle_for_api
        ).await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let worker_node = worker::WorkerNode::new(my_token.clone(), shared_config.clone(), max_connections)
        .with_peers(coordinator.peers.clone(), node_id.clone());
    let grpc_addr = format!("0.0.0.0:{}", grpc_port).parse()?;
    
    let grpc_server = tokio::spawn(async move {
        println!("[System] Starting gRPC Peer Service on {}", grpc_addr);
        Server::builder()
            .add_service(pb::octoget::peer_service_server::PeerServiceServer::new(worker_node))
            .serve(grpc_addr)
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    coordinator.clone().start_health_check();
    coordinator.clone().start_peer_discovery();

    let coord_for_self = coordinator.clone();
    let self_addr = final_public_address.clone();
    let self_token = my_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        coord_for_self.add_peer(self_addr, self_token, shareable).await;
    });

    if let Some(peers) = peers_config {
        println!("[System] Loading {} peers from config", peers.len());
        let final_public_address_clone = final_public_address.clone();
        let my_token_clone = my_token.clone();
        for peer in peers {
            let coord_clone = coordinator.clone();
            let p_addr = peer.address.clone();
            let p_tok = peer.token.clone();
            let my_addr = final_public_address_clone.clone();
            let my_tok = my_token_clone.clone();
            
            tokio::spawn(async move {
                println!("[System] Adding peer from config: {}", p_addr);
                coord_clone.add_peer(p_addr.clone(), p_tok.clone(), true).await;
                
                coord_clone.share_self_to_peer(&p_addr, &p_tok, &my_addr, &my_tok, shareable).await;
            });
        }
    }

    if let Err(e) = coordinator.load_tasks().await {
        tracing::warn!("Failed to load persisted tasks: {}", e);
        println!("[System] Failed to load persisted tasks: {}", e);
    }
    
    crate::coordinator::task_ops::check_pending_tasks(&coordinator).await;

    tokio::select! {
        _ = grpc_server => info!("gRPC server exited"),
        _ = api_server => info!("API server exited"),
        _ = tokio::signal::ctrl_c() => info!("Received Ctrl+C, shutting down gracefully..."),
    }

    Ok(())
}