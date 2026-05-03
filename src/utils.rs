// src/utils.rs
use anyhow::Result;
use bytes::Bytes;
use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Result as IoResult;
use tokio::time::{sleep, Duration, Instant};
use tracing::info;
use tracing_subscriber::{reload, EnvFilter, Registry};

pub type LogReloadHandle = reload::Handle<EnvFilter, Registry>;

#[derive(Serialize, Deserialize, Debug)]
pub struct ResolveReq {
    pub url: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ResolveResp {
    pub final_url: String,
    pub file_size: u64,
    pub support_range: bool,
}

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct Checksums {
    pub md5: Option<String>,
    pub sha256: Option<String>,
}

#[cfg(unix)]
pub fn write_at(file: &File, buf: &[u8], offset: u64) -> IoResult<usize> {
    use std::os::unix::fs::FileExt;
    file.write_at(buf, offset)
}

#[cfg(windows)]
pub fn write_at(file: &File, buf: &[u8], offset: u64) -> IoResult<usize> {
    use std::os::windows::fs::FileExt;
    file.seek_write(buf, offset)
}

pub async fn exponential_backoff(retry_count: u32, base_delay_ms: u64) {
    let delay = base_delay_ms * (2_u64.pow(retry_count));
    let delay = std::cmp::min(delay, 30000);
    sleep(Duration::from_millis(delay)).await;
}

pub fn format_body_log(headers: &HeaderMap, bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "<empty>".to_string();
    }
    let content_type = headers
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if content_type.contains("application/json")
        || content_type.contains("text/")
        || content_type.contains("application/x-www-form-urlencoded")
    {
        String::from_utf8_lossy(bytes).into_owned()
    } else {
        format!("<binary data, size: {} bytes>", bytes.len())
    }
}

pub fn http_client_builder(use_proxy: bool) -> reqwest::ClientBuilder {
    let builder = reqwest::Client::builder()
        .use_rustls_tls()
        .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36");
    if !use_proxy {
        builder.no_proxy()
    } else {
        builder
    }
}

pub async fn send_http_request(
    client: &reqwest::Client,
    method: reqwest::Method,
    url: &str,
    headers: HeaderMap,
    body: Option<Bytes>,
) -> Result<reqwest::Response> {
    let req_body_str = match &body {
        Some(b) => format_body_log(&headers, b),
        None => "<empty>".to_string(),
    };

    info!(method = %method, url = %url, headers = ?headers, body = %req_body_str, "Sending External HTTP Request");

    let mut builder = client.request(method, url).headers(headers);
    if let Some(b) = body {
        builder = builder.body(b);
    }

    let resp = builder.send().await?;
    let content_length = resp.content_length().unwrap_or(0);
    let res_body_str = format!("<stream data, size: {} bytes>", content_length);

    info!(status = %resp.status(), headers = ?resp.headers(), body = %res_body_str, "Received External HTTP Response");
    Ok(resp)
}

pub async fn send_http_request_full(
    client: &reqwest::Client,
    method: reqwest::Method,
    url: &str,
    headers: HeaderMap,
    body: Option<Bytes>,
) -> Result<(reqwest::StatusCode, HeaderMap, Bytes)> {
    let req_body_str = match &body {
        Some(b) => format_body_log(&headers, b),
        None => "<empty>".to_string(),
    };

    info!(method = %method, url = %url, headers = ?headers, body = %req_body_str, "Sending External HTTP Request");

    let mut builder = client.request(method, url).headers(headers.clone());
    if let Some(b) = body {
        builder = builder.body(b);
    }

    let resp = builder.send().await?;
    let status = resp.status();
    let res_headers = resp.headers().clone();
    let res_bytes = resp.bytes().await?;

    let res_body_str = format_body_log(&res_headers, &res_bytes);
    info!(status = %status, headers = ?res_headers, body = %res_body_str, "Received External HTTP Response");

    Ok((status, res_headers, res_bytes))
}

pub async fn resolve_url(client: &reqwest::Client, url: &str) -> Result<ResolveResp> {
    println!("[Network] Resolving URL locally (HEAD): {}", url);
    match client.head(url).send().await {
        Ok(head_resp) => {
            let head_status = head_resp.status();
            let final_url = head_resp.url().to_string();
            
            if head_status.is_success() {
                let content_length = head_resp.content_length().unwrap_or(0);
                if content_length > 0 {
                    let mut range_headers = reqwest::header::HeaderMap::new();
                    range_headers.insert(reqwest::header::RANGE, reqwest::header::HeaderValue::from_static("bytes=0-0"));
                    match client.request(reqwest::Method::GET, &final_url).headers(range_headers).send().await {
                        Ok(range_resp) => {
                            if range_resp.status() == reqwest::StatusCode::PARTIAL_CONTENT {
                                return Ok(ResolveResp { final_url, file_size: content_length, support_range: true });
                            }
                        }
                        Err(_) => {}
                    }
                    return Ok(ResolveResp { final_url, file_size: content_length, support_range: false });
                }
            }
        }
        Err(e) => {
            if e.is_timeout() || e.is_connect() {
                anyhow::bail!("Connection failed or timed out: {}", e);
            }
        }
    }
    
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(reqwest::header::RANGE, reqwest::header::HeaderValue::from_static("bytes=0-0"));
    
    println!("[Network] Resolving URL locally (GET with range): {}", url);
    match send_http_request(client, reqwest::Method::GET, url, headers, None).await {
        Ok(resp) => {
            let final_url = resp.url().to_string();
            let status = resp.status();
            
            if status == reqwest::StatusCode::PARTIAL_CONTENT {
                let content_range = resp.headers().get(reqwest::header::CONTENT_RANGE)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("");
                if let Some(total_str) = content_range.split('/').last() {
                    if let Ok(total) = total_str.parse::<u64>() {
                        return Ok(ResolveResp { final_url, file_size: total, support_range: true });
                    }
                }
            } else if status == reqwest::StatusCode::OK {
                let content_length = resp.content_length().unwrap_or(0);
                if content_length > 0 {
                    return Ok(ResolveResp { final_url, file_size: content_length, support_range: false });
                }
            }
        }
        Err(e) => {
            if let Some(req_err) = e.downcast_ref::<reqwest::Error>() {
                if req_err.is_timeout() || req_err.is_connect() {
                    anyhow::bail!("Connection failed or timed out: {}", req_err);
                }
            }
        }
    }
    
    println!("[Network] Resolving URL locally (GET without range): {}", url);
    let plain_resp = tokio::time::timeout(Duration::from_secs(15), client.get(url).send()).await
        .map_err(|_| anyhow::anyhow!("Request timeout"))??;
    let plain_status = plain_resp.status();
    let final_url = plain_resp.url().to_string();
    
    if plain_status.is_success() {
        let content_length = plain_resp.content_length().unwrap_or(0);
        if content_length > 0 {
            return Ok(ResolveResp { final_url, file_size: content_length, support_range: false });
        }
    }
    
    anyhow::bail!("Failed to resolve URL. Status: {}", plain_status)
}

pub fn setup_upnp(local_port: u16) -> Option<String> {
    use igd::search_gateway;
    use std::net::SocketAddrV4;
    
    if let Ok(gateway) = search_gateway(Default::default()) {
        if let Ok(std::net::IpAddr::V4(v4_addr)) = local_ip_address::local_ip() {
            let local_socket = SocketAddrV4::new(v4_addr, local_port);
            if gateway.add_port(igd::PortMappingProtocol::TCP, local_port, local_socket, 0, "OctoGet").is_ok() {
                if let Ok(ext_ip) = gateway.get_external_ip() {
                    return Some(format!("{}:{}", ext_ip, local_port));
                }
            }
        }
    }
    None
}

pub async fn get_or_create_node_id(cache_dir: &str) -> Result<String> {
    let path = std::path::PathBuf::from(cache_dir).join("node_id");
    if path.exists() {
        let id = tokio::fs::read_to_string(&path).await?.trim().to_string();
        if !id.is_empty() { return Ok(id); }
    }
    let new_id = uuid::Uuid::new_v4().to_string();
    tokio::fs::create_dir_all(cache_dir).await?;
    tokio::fs::write(&path, &new_id).await?;
    Ok(new_id)
}

pub struct RateLimiter {
    tokens: f64,
    last_update: Instant,
}

impl RateLimiter {
    pub fn new() -> Self {
        Self { tokens: 0.0, last_update: Instant::now() }
    }

    pub fn acquire(&mut self, bytes: usize, limit_kb: u32) -> Duration {
        if limit_kb == 0 { 
            self.last_update = Instant::now();
            return Duration::from_secs(0); 
        }
        let rate_bytes_per_sec = (limit_kb as f64) * 1024.0;
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update).as_secs_f64();
        
        self.tokens += elapsed * rate_bytes_per_sec;
        if self.tokens > rate_bytes_per_sec {
            self.tokens = rate_bytes_per_sec;
        }

        let cost = bytes as f64;
        if self.tokens >= cost {
            self.tokens -= cost;
            self.last_update = Instant::now();
            Duration::from_secs(0)
        } else {
            let deficit = cost - self.tokens;
            let sleep_time = deficit / rate_bytes_per_sec;
            self.tokens = 0.0;
            self.last_update = Instant::now() + Duration::from_secs_f64(sleep_time);
            Duration::from_secs_f64(sleep_time)
        }
    }
}

pub async fn calculate_checksums(path: &std::path::Path) -> Result<Checksums> {
    if !path.exists() {
        anyhow::bail!("File not found");
    }
    
    let path_buf = path.to_path_buf();
    
    let checksums = tokio::task::spawn_blocking(move || -> Result<Checksums> {
        use md5::{Md5, Digest as Md5Digest};
        use sha2::Sha256;
        use std::fs::File;
        use std::io::Read;

        let mut file = File::open(&path_buf)?;
        let mut md5_hasher = Md5::new();
        let mut sha256_hasher = Sha256::new();
        
        // Use a heap-allocated 8MB buffer to prevent stack overflow in the worker thread
        let mut buffer = vec![0u8; 8 * 1024 * 1024];

        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            md5_hasher.update(&buffer[..n]);
            sha256_hasher.update(&buffer[..n]);
        }

        Ok(Checksums {
            md5: Some(format!("{:x}", md5_hasher.finalize())),
            sha256: Some(format!("{:x}", sha256_hasher.finalize())),
        })
    }).await??;

    Ok(checksums)
}