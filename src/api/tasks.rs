// src/api/tasks.rs
use crate::api::middleware::ApiError;
use crate::api::sys::ApiResponse;
use crate::api::ApiState;
use crate::coordinator::models::BatchSubmitReq;
use axum::{extract::{Path, State}, http::StatusCode, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use tracing::error;
use uuid::Uuid;

#[derive(Deserialize, Debug)]
pub struct SubmitTaskReq { pub url: String, pub file_name: Option<String>, pub save_dir: String, pub force: Option<bool> }

#[derive(Serialize)]
pub struct TaskSubmitResp { pub task_id: String }

#[derive(Serialize)]
pub struct BatchSubmitResp { pub task_ids: Vec<String> }

pub async fn handle_submit_task(State(state): State<ApiState>, Json(payload): Json<SubmitTaskReq>) -> Result<impl IntoResponse, ApiError> {
    if payload.force.unwrap_or(false) {
        if let Some(existing_task_id) = state.coordinator.find_task_by_url(&payload.url).await {
            let _ = state.coordinator.delete_task(&existing_task_id, true).await;
        }
    } else {
        if let Some(existing_task_id) = state.coordinator.find_task_by_url(&payload.url).await {
            return Ok((StatusCode::CONFLICT, Json(ApiResponse { message: "Task already exists".to_string(), data: Some(TaskSubmitResp { task_id: existing_task_id }) })).into_response());
        }
    }

    let task_id = Uuid::new_v4().to_string();
    println!("[Task] Submitted new task: {} (URL: {})", task_id, payload.url);
    let save_dir = if payload.save_dir.trim().is_empty() {
        state.config.read().await.default_save_dir.clone().unwrap_or_else(|| "./downloads".to_string())
    } else {
        payload.save_dir.clone()
    };
    if let Err(e) = state.coordinator.start_task(task_id.clone(), payload.url, payload.file_name, save_dir).await {
        error!(task_id = %task_id, "Task start failed: {:?}", e);
    }
    Ok((StatusCode::ACCEPTED, Json(ApiResponse { message: "Task submitted".to_string(), data: Some(TaskSubmitResp { task_id }) })).into_response())
}

pub async fn handle_batch_submit(State(state): State<ApiState>, Json(payload): Json<BatchSubmitReq>) -> Result<impl IntoResponse, ApiError> {
    println!("[Task] Batch submitted {} tasks", payload.tasks.len());
    let save_dir = if payload.save_dir.trim().is_empty() {
        state.config.read().await.default_save_dir.clone().unwrap_or_else(|| "./downloads".to_string())
    } else {
        payload.save_dir.clone()
    };
    let mut task_ids = Vec::new();
    for item in payload.tasks {
        if item.url.trim().is_empty() { continue; }
        
        if item.force.unwrap_or(false) {
            if let Some(existing_task_id) = state.coordinator.find_task_by_url(&item.url).await {
                let _ = state.coordinator.delete_task(&existing_task_id, true).await;
            }
        } else {
            if let Some(_) = state.coordinator.find_task_by_url(&item.url).await {
                continue;
            }
        }

        let task_id = Uuid::new_v4().to_string();
        task_ids.push(task_id.clone());
        if let Err(e) = state.coordinator.start_task(task_id.clone(), item.url, item.file_name, save_dir.clone()).await {
            error!(task_id = %task_id, "Task start failed: {:?}", e);
        }
    }
    Ok((StatusCode::ACCEPTED, Json(ApiResponse { message: format!("{} tasks submitted", task_ids.len()), data: Some(BatchSubmitResp { task_ids }) })).into_response())
}

pub async fn handle_get_task(State(state): State<ApiState>, Path(id): Path<String>) -> Result<impl IntoResponse, ApiError> {
    if let Some(progress) = state.coordinator.get_task_progress(&id).await {
        Ok((StatusCode::OK, Json(ApiResponse { message: "Success".to_string(), data: Some(progress) })).into_response())
    } else {
        Ok((StatusCode::NOT_FOUND, Json(ApiResponse { message: "Task not found".to_string(), data: None::<()> })).into_response())
    }
}

pub async fn handle_list_tasks(State(state): State<ApiState>) -> Result<impl IntoResponse, ApiError> {
    let tasks = state.coordinator.get_all_tasks_progress().await;
    println!("[DEBUG] Listing {} tasks from memory", tasks.len());
    for task in &tasks {
        println!("[DEBUG] Task in memory: {} ({})", task.task_id, task.status);
    }
    Ok((StatusCode::OK, Json(ApiResponse { message: "Success".to_string(), data: Some(tasks) })).into_response())
}

pub async fn handle_pause_task(State(state): State<ApiState>, Path(id): Path<String>) -> Result<impl IntoResponse, ApiError> {
    state.coordinator.pause_task(&id).await?;
    Ok((StatusCode::OK, Json(ApiResponse { message: "Task paused".to_string(), data: None::<()> })).into_response())
}

pub async fn handle_resume_task(State(state): State<ApiState>, Path(id): Path<String>) -> Result<impl IntoResponse, ApiError> {
    println!("[DEBUG] Attempting to resume task: {}", id);
    match state.coordinator.resume_task(&id).await {
        Ok(_) => {
            println!("[DEBUG] Successfully resumed task: {}", id);
            Ok((StatusCode::OK, Json(ApiResponse { message: "Task resumed".to_string(), data: None::<()> })).into_response())
        }
        Err(e) => {
            println!("[DEBUG] Failed to resume task {}: {}", id, e);
            Err(e.into())
        }
    }
}

pub async fn handle_delete_task(State(state): State<ApiState>, Path(id): Path<String>) -> Result<impl IntoResponse, ApiError> {
    println!("[DEBUG] Attempting to delete task: {}", id);
    match state.coordinator.delete_task(&id, false).await {
        Ok(_) => {
            println!("[DEBUG] Successfully deleted task: {}", id);
            Ok((StatusCode::OK, Json(ApiResponse { message: "Task deleted".to_string(), data: None::<()> })).into_response())
        }
        Err(e) => {
            println!("[DEBUG] Failed to delete task {}: {}", id, e);
            Err(e.into())
        }
    }
}

pub async fn handle_open_dir(State(state): State<ApiState>, Path(id): Path<String>) -> Result<impl IntoResponse, ApiError> {
    if let Some(prog) = state.coordinator.get_task_progress(&id).await {
        let path = std::path::PathBuf::from(prog.save_path);
        if let Some(parent) = path.parent() {
            if let Err(e) = open::that(parent) {
                return Ok((StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse { message: format!("Failed: {}", e), data: None::<()> })).into_response());
            }
        }
        Ok((StatusCode::OK, Json(ApiResponse { message: "Directory opened".to_string(), data: None::<()> })).into_response())
    } else {
        Ok((StatusCode::NOT_FOUND, Json(ApiResponse { message: "Task not found".to_string(), data: None::<()> })).into_response())
    }
}

pub async fn handle_recalc_checksums(State(state): State<ApiState>, Path(id): Path<String>) -> Result<impl IntoResponse, ApiError> {
    let ctx = state.coordinator.tasks.get(&id).map(|r| r.value().clone());
    if let Some(ctx) = ctx {
        let path = ctx.save_path.read().await.clone();
        if !path.exists() {
            return Ok((StatusCode::NOT_FOUND, Json(ApiResponse { message: "File not found".to_string(), data: None::<()> })).into_response());
        }
        match crate::utils::calculate_checksums(&path).await {
            Ok(checksums) => {
                *ctx.checksums.write().await = Some(checksums.clone());
                let _ = state.coordinator.save_task(&id).await;
                Ok((StatusCode::OK, Json(ApiResponse { message: "Success".to_string(), data: Some(checksums) })).into_response())
            }
            Err(e) => {
                Ok((StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse { message: format!("Failed: {}", e), data: None::<()> })).into_response())
            }
        }
    } else {
        Ok((StatusCode::NOT_FOUND, Json(ApiResponse { message: "Task not found".to_string(), data: None::<()> })).into_response())
    }
}