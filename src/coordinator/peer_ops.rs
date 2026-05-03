// src/coordinator/peer_ops.rs
use super::Coordinator;
use crate::coordinator::models::{PeerInfo, PeerStatusResp};
use std::sync::Arc;
use tracing::info;

impl Coordinator {
    pub async fn add_peer(&self, address: String, token: String, shareable: bool) {
        let mut exists = false;
        for mut peer in self.peers.iter_mut() {
            if peer.addresses.contains(&address) || peer.best_address == address {
                peer.shareable = shareable;
                exists = true;
                break;
            }
        }
        if exists {
            info!("Peer address {} already exists, updated shareable", address);
            println!("[Node Discovery] Peer address {} already exists, updated shareable: {}", address, shareable);
            return;
        }

        let mut target_node_id = address.clone();
        let my_node_id = self.node_id.clone();
        
        let resolve_id = async {
            if let Ok(endpoint) = tonic::transport::Endpoint::from_shared(format!("http://{}", address)) {
                if let Ok(channel) = endpoint.connect_timeout(std::time::Duration::from_secs(3)).connect().await {
                    let token_clone = token.clone();
                    let mut client = crate::pb::octoget::peer_service_client::PeerServiceClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
                        if let Ok(meta_val) = tonic::metadata::MetadataValue::try_from(format!("Bearer {}", token_clone)) {
                            req.metadata_mut().insert("authorization", meta_val);
                        }
                        if let Ok(meta_val) = tonic::metadata::MetadataValue::try_from(&my_node_id) {
                            req.metadata_mut().insert("x-node-id", meta_val);
                        }
                        Ok(req)
                    });
                    let req = crate::pb::octoget::GetPeersRequest { requester_node_id: self.node_id.clone(), requester_token: String::new() };
                    if let Ok(resp) = client.get_peers(req).await {
                        if let Some(id) = resp.metadata().get("x-node-id") {
                            if let Ok(id_str) = id.to_str() {
                                return Some(id_str.to_string());
                            }
                        }
                    }
                }
            }
            None
        };

        if let Ok(Some(id)) = tokio::time::timeout(std::time::Duration::from_secs(5), resolve_id).await {
            target_node_id = id;
        }

        if let Some(mut peer) = self.peers.get_mut(&target_node_id) {
            if !peer.addresses.contains(&address) { peer.addresses.push(address.clone()); }
            peer.shareable = shareable;
            info!("Updated existing peer: {}", target_node_id);
            println!("[Node Discovery] Updated existing peer: {} (Shareable: {})", target_node_id, shareable);
        } else {
            self.peers.insert(target_node_id.clone(), PeerInfo {
                node_id: target_node_id.clone(),
                addresses: vec![address.clone()],
                best_address: address.clone(),
                token,
                status: "Pending".to_string(),
                latency_ms: 0,
                download_speed: 0.0,
                fail_count: 0,
                shareable,
                active_connections: 0,
            });
            info!("Added new peer: {} (addresses: {:?})", target_node_id, self.peers.get(&target_node_id).map(|p| p.addresses.clone()));
            println!("[Node Added]  {} , {:?}, Shareable: {}", target_node_id, self.peers.get(&target_node_id).map(|p| p.addresses.clone()), shareable);
        }
    }

    pub async fn share_self_to_peer(&self, peer_addr: &str, peer_token: &str, my_addr: &str, my_token: &str, shareable: bool) {
        use crate::pb::octoget::peer_service_client::PeerServiceClient;
        use crate::pb::octoget::SharePeerRequest;
        use tonic::transport::Endpoint;

        if let Ok(endpoint) = Endpoint::from_shared(format!("http://{}", peer_addr)) {
            if let Ok(channel) = endpoint.connect_timeout(std::time::Duration::from_secs(5)).connect().await {
                let token_clone = peer_token.to_string();
                let my_node_id = self.node_id.clone();
                let mut client = PeerServiceClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
                    if let Ok(meta_val) = tonic::metadata::MetadataValue::try_from(format!("Bearer {}", token_clone)) {
                        req.metadata_mut().insert("authorization", meta_val);
                    }
                    if let Ok(meta_val) = tonic::metadata::MetadataValue::try_from(&my_node_id) {
                        req.metadata_mut().insert("x-node-id", meta_val);
                    }
                    Ok(req)
                });

                let req = SharePeerRequest {
                    node_id: self.node_id.clone(),
                    address: my_addr.to_string(),
                    api_address: String::new(),
                    token: my_token.to_string(),
                    shareable,
                };

                let _ = client.share_peer(req).await;
            }
        }
    }

    pub async fn get_peers(&self) -> Vec<(String, String)> {
        self.peers.iter().map(|kv| (kv.key().clone(), kv.value().token.clone())).collect()
    }

    pub async fn get_peer_best_address(&self, node_id: &str) -> String {
        self.peers.get(node_id).map(|p| p.best_address.clone()).unwrap_or_default()
    }

    pub async fn get_peers_grpc(&self) -> Vec<(String, String)> {
        let mut res = Vec::new();
        for kv in self.peers.iter() {
            if !kv.value().best_address.is_empty() {
                res.push((kv.value().best_address.clone(), kv.value().token.clone()));
            } else if let Some(addr) = kv.value().addresses.first() {
                res.push((addr.clone(), kv.value().token.clone()));
            }
        }
        res
    }

    pub async fn get_peers_status(&self) -> Vec<PeerStatusResp> {
        self.peers.iter().map(|kv| PeerStatusResp {
            node_id: kv.value().node_id.clone(),
            best_address: kv.value().best_address.clone(),
            addresses: kv.value().addresses.clone(),
            token: kv.value().token.clone(),
            status: kv.value().status.clone(),
            latency_ms: kv.value().latency_ms,
            download_speed: kv.value().download_speed,
            shareable: kv.value().shareable,
            is_self: false,
            speed_limit_kb: 0,
            active_connections: kv.value().active_connections,
        }).collect()
    }

    pub async fn update_peer_speed(&self, node_id: &str, speed: f64) {
        if let Some(mut peer) = self.peers.get_mut(node_id) {
            peer.download_speed = if peer.download_speed == 0.0 { speed } else { peer.download_speed * 0.7 + speed * 0.3 };
        }
    }

    pub async fn get_peer_speed(&self, node_id: &str) -> f64 {
        self.peers.get(node_id).map(|p| p.download_speed).unwrap_or(0.0)
    }

    pub async fn get_avg_peer_speed(&self) -> f64 {
        let (mut total, mut count) = (0.0, 0);
        for p in self.peers.iter() {
            if p.download_speed > 0.0 { total += p.download_speed; count += 1; }
        }
        if count > 0 { total / count as f64 } else { 0.0 }
    }

    pub fn start_health_check(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                
                for mut peer in self.peers.iter_mut() {
                    if peer.active_connections == 0 {
                        peer.download_speed = 0.0;
                    } else {
                        peer.download_speed *= 0.8;
                        if peer.download_speed < 0.1 {
                            peer.download_speed = 0.0;
                        }
                    }
                }

                let updates: Vec<_> = self.peers.iter().map(|kv| (kv.key().clone(), kv.value().addresses.clone(), kv.value().token.clone())).collect();
                let mut to_remove = Vec::new();
                let mut to_update = Vec::new();
                let mut status_updates = Vec::new();
                
                for (node_id, addresses, token) in updates {
                    let (mut best_addr, mut best_latency, mut is_online) = (String::new(), u64::MAX, false);
                    for addr in addresses {
                        let start = std::time::Instant::now();
                        let host = addr.replace("http://", "").replace("https://", "");
                        if let Ok(Ok(_)) = tokio::time::timeout(std::time::Duration::from_secs(3), tokio::net::TcpStream::connect(&host)).await {
                            let latency = start.elapsed().as_millis() as u64;
                            is_online = true;
                            if latency < best_latency { best_latency = latency; best_addr = addr.clone(); }
                        }
                    }
                    
                    let mut discovered_id = None;
                    if is_online {
                        if let Ok(endpoint) = tonic::transport::Endpoint::from_shared(format!("http://{}", best_addr)) {
                            if let Ok(channel) = endpoint.connect_timeout(std::time::Duration::from_secs(3)).connect().await {
                                let token_clone = token.clone();
                                let my_node_id = self.node_id.clone();
                                let mut client = crate::pb::octoget::peer_service_client::PeerServiceClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
                                    if let Ok(meta_val) = tonic::metadata::MetadataValue::try_from(format!("Bearer {}", token_clone)) {
                                        req.metadata_mut().insert("authorization", meta_val);
                                    }
                                    if let Ok(meta_val) = tonic::metadata::MetadataValue::try_from(&my_node_id) {
                                        req.metadata_mut().insert("x-node-id", meta_val);
                                    }
                                    Ok(req)
                                });
                                let req = crate::pb::octoget::GetPeersRequest { requester_node_id: self.node_id.clone(), requester_token: String::new() };
                                if let Ok(resp) = client.get_peers(req).await {
                                    if let Some(id) = resp.metadata().get("x-node-id") {
                                        if let Ok(id_str) = id.to_str() {
                                            if id_str != node_id {
                                                discovered_id = Some(id_str.to_string());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    status_updates.push((node_id.clone(), is_online, best_addr, best_latency, discovered_id));
                }
                
                for (node_id, is_online, best_addr, best_latency, discovered_id) in status_updates {
                    if let Some(mut peer) = self.peers.get_mut(&node_id) {
                        if is_online {
                            peer.status = "Online".to_string(); 
                            peer.latency_ms = best_latency; 
                            peer.best_address = best_addr.clone();
                            peer.fail_count = 0;
                            if let Some(new_id) = discovered_id {
                                to_update.push((node_id.clone(), new_id));
                            }
                        } else {
                            peer.status = "Offline".to_string(); 
                            peer.latency_ms = 0; 
                            peer.download_speed = 0.0;
                            peer.fail_count += 1;
                            if peer.fail_count > 30 {
                                to_remove.push(node_id.clone());
                            }
                        }
                    }
                }
                
                for (old_id, new_id) in to_update {
                    if new_id == self.node_id {
                        info!("Discovered peer is self, removing: {}", old_id);
                        println!("[Node Discovery] Discovered peer is self, removing: {}", old_id);
                        self.peers.remove(&old_id);
                        continue;
                    }
                    
                    if let Some((_, mut old_peer)) = self.peers.remove(&old_id) {
                        if let Some(mut existing_peer) = self.peers.get_mut(&new_id) {
                            for addr in old_peer.addresses {
                                if !existing_peer.addresses.contains(&addr) {
                                    existing_peer.addresses.push(addr);
                                }
                            }
                            existing_peer.status = "Online".to_string();
                            existing_peer.fail_count = 0;
                        } else {
                            old_peer.node_id = new_id.clone();
                            self.peers.insert(new_id.clone(), old_peer);
                        }
                        info!("Updated peer node_id: {} -> {}", old_id, new_id);
                        println!("[Node Discovery] Updated peer node_id: {} -> {}", old_id, new_id);
                    }
                }
                
                for id in to_remove {
                    info!("Removing offline peer: {}", id);
                    println!("[Node Discovery] Removing offline peer: {}", id);
                    self.peers.remove(&id);
                }
            }
        });
    }

    pub fn start_peer_discovery(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;

                let peers_for_discovery: Vec<(String, String)> = {
                    let mut addresses = Vec::new();
                    for kv in self.peers.iter() {
                        let peer = kv.value();
                        if !peer.best_address.is_empty() {
                            addresses.push((peer.best_address.clone(), peer.token.clone()));
                        }
                    }
                    addresses
                };

                for (grpc_addr, token) in peers_for_discovery {
                    use crate::pb::octoget::peer_service_client::PeerServiceClient;
                    use crate::pb::octoget::GetPeersRequest;
                    use tonic::transport::Endpoint;

                    let endpoint = match Endpoint::from_shared(format!("http://{}", grpc_addr.trim_end_matches('/'))) {
                        Ok(ep) => ep,
                        Err(e) => {
                            tracing::warn!("Failed to create endpoint for {}: {}", grpc_addr, e);
                            continue;
                        }
                    };

                    let channel = match tokio::time::timeout(std::time::Duration::from_secs(10), endpoint.connect()).await {
                        Ok(Ok(ch)) => ch,
                        Ok(Err(e)) => {
                            tracing::debug!("Failed to connect to {}: {}", grpc_addr, e);
                            continue;
                        }
                        Err(_) => {
                            tracing::debug!("Connection to {} timed out", grpc_addr);
                            continue;
                        }
                    };

                    let my_node_id = self.node_id.clone();
                    let mut client = PeerServiceClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
                        if let Ok(meta_val) = tonic::metadata::MetadataValue::try_from(format!("Bearer {}", token)) {
                            req.metadata_mut().insert("authorization", meta_val);
                        }
                        if let Ok(meta_val) = tonic::metadata::MetadataValue::try_from(&my_node_id) {
                            req.metadata_mut().insert("x-node-id", meta_val);
                        }
                        Ok(req)
                    });

                    let req = GetPeersRequest {
                        requester_node_id: self.node_id.clone(),
                        requester_token: String::new(),
                    };

                    match client.get_peers(req).await {
                        Ok(response) => {
                            let peer_list = response.into_inner().peers;
                            for p in peer_list {
                                if p.node_id != self.node_id && !self.peers.contains_key(&p.node_id) {
                                    if let Some(addr) = p.addresses.first() {
                                        info!("Discovered new peer {} via gRPC {}", p.node_id, grpc_addr);
                                        println!("[gRPC Node Discovery] Discovered new peer {} via {}", p.node_id, grpc_addr);
                                        self.add_peer(addr.clone(), p.token.clone(), p.shareable).await;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::debug!("Failed to get peers from {}: {}", grpc_addr, e);
                        }
                    }
                }
            }
        });
    }
}