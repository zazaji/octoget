// src/coordinator/grpc_ops.rs
use super::Coordinator;
use anyhow::Result;

impl Coordinator {
    pub async fn dispatch_task_to_peer(&self, task_id: &str, url: &str, final_url: &str, file_size: u64, support_range: bool, grpc_addr: &str, token: &str) -> Result<()> {
        use crate::pb::octoget::peer_service_client::PeerServiceClient;
        use crate::pb::octoget::DispatchTaskRequest;
        use tonic::transport::Endpoint;

        let endpoint = Endpoint::from_shared(format!("http://{}", grpc_addr.trim_end_matches('/')))?
            .connect_timeout(std::time::Duration::from_secs(5));
        let channel = tokio::time::timeout(std::time::Duration::from_secs(10), endpoint.connect()).await
            .map_err(|_| anyhow::anyhow!("Connection timeout"))??;

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

        let req = DispatchTaskRequest {
            task_id: task_id.to_string(),
            url: url.to_string(),
            final_url: final_url.to_string(),
            file_size,
            support_range,
        };

        let response = client.dispatch_task(req).await?;
        if !response.into_inner().success {
            anyhow::bail!("Failed to dispatch task to peer");
        }

        Ok(())
    }

    pub async fn assign_chunks_to_peer(&self, task_id: &str, chunks: Vec<(u32, u64, u64)>, grpc_addr: &str, token: &str) -> Result<()> {
        use crate::pb::octoget::peer_service_client::PeerServiceClient;
        use crate::pb::octoget::{AssignChunksRequest, ChunkAssignment};
        use tonic::transport::Endpoint;

        let endpoint = Endpoint::from_shared(format!("http://{}", grpc_addr.trim_end_matches('/')))?
            .connect_timeout(std::time::Duration::from_secs(5));
        let channel = tokio::time::timeout(std::time::Duration::from_secs(10), endpoint.connect()).await
            .map_err(|_| anyhow::anyhow!("Connection timeout"))??;

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

        let chunk_assignments: Vec<ChunkAssignment> = chunks.into_iter()
            .map(|(index, start, end)| ChunkAssignment { index, start, end })
            .collect();

        let req = AssignChunksRequest {
            task_id: task_id.to_string(),
            chunks: chunk_assignments,
        };

        let response = client.assign_chunks(req).await?;
        if !response.into_inner().success {
            anyhow::bail!("Failed to assign chunks to peer");
        }

        Ok(())
    }

    pub async fn resolve_url_via_peer_grpc(&self, url: &str, grpc_addr: &str, token: &str) -> Result<crate::utils::ResolveResp> {
        use crate::pb::octoget::peer_service_client::PeerServiceClient;
        use crate::pb::octoget::ResolveUrlRequest;
        use tonic::transport::Endpoint;

        let endpoint = Endpoint::from_shared(format!("http://{}", grpc_addr.trim_end_matches('/')))?
            .connect_timeout(std::time::Duration::from_secs(5));
        let channel = tokio::time::timeout(std::time::Duration::from_secs(10), endpoint.connect()).await
            .map_err(|_| anyhow::anyhow!("Connection timeout"))??;

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

        let mut req = tonic::Request::new(ResolveUrlRequest {
            url: url.to_string(),
        });
        req.set_timeout(std::time::Duration::from_secs(10));

        let response = client.resolve_url(req).await?;
        let resp = response.into_inner();

        Ok(crate::utils::ResolveResp {
            final_url: resp.final_url,
            file_size: resp.file_size,
            support_range: resp.support_range,
        })
    }
}