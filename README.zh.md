<!-- README.zh.md -->
# OctoGet - 分布式下载加速器

[English](README.md)

OctoGet 是一个使用 Rust 编写的高性能分布式下载加速器。
它允许您在不同的服务器上部署多个节点，并利用它们组合的网络带宽来加速文件下载。

- 场景1: 如果本地不能访问目标地址，某个节点可以访问，则可直接下载。

- 场景2: 如果本地访问目标节点速度很慢，可以通过多节点进行下载加速。

## 核心特性
- **对称架构**：每个节点既可以作为节点发起者，也可以作为工作节点（Worker）。
- **动态负载均衡**：使用细粒度切片自然平衡负载。
- **智能优选节点**：检测长尾延迟，实时测速，并动态将流量路由到最快的节点。
- **自动注册分享**：节点启动时可自动向对等节点注册自己，实现双向带宽共享。
- **细粒度安全**：为每个对等节点提供独立的 Token 配置。

## 界面截图

### 下载列表界面
![下载列表](images/downloading.png)

### 节点界面
![节点](images/nodes.png)

### 设置界面
![设置](images/settings.png)

## 编译说明
确保您已安装 Rust 和 Cargo。
```bash
cargo build --release
```

## 配置文件 (`config.toml`)
您可以通过 `config.toml` 文件配置节点。
```toml
grpc_port = 50051
api_port = 50052
record_dir = "./octoget_records"
my_token = "xx50051xx"
share_node = true
shareable = true
nat_traversal = true
max_connections = 16
global_speed_limit_kb = 5000
peer_speed_limit_kb = 500
log_level = "warn"

[[peers]]
address = "10.10.10.100:50051"
token = "xx9123xx"
```

## 运行
```bash
# 使用配置文件运行
octoget --config config.toml
或者直接运行`octoget`，自动从当前目录查找`config.toml`文件
