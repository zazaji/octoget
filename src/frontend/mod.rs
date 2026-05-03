// src/frontend/mod.rs
use axum::extract::State;
use axum::response::Html;
use axum::http::header;
use axum::response::IntoResponse;

pub const INDEX_HTML: &str = include_str!("index.html");
pub const TASK_LIST_HTML: &str = include_str!("task_list.html");
pub const NETWORK_HTML: &str = include_str!("network.html");
pub const MODALS_HTML: &str = include_str!("modals.html");
pub const STYLE_CSS: &str = include_str!("style.css");
pub const APP_JS: &str = include_str!("app.js");
pub const I18N_JS: &str = include_str!("i18n.js");
pub const VUE_JS: &str = include_str!("vendor/vue.global.prod.js");
pub const TAILWIND_JS: &str = include_str!("vendor/tailwindcss-browser.js");

pub async fn serve_frontend(State(token): State<String>) -> Html<String> {
    let mut html = INDEX_HTML.replace("__DISTGET_TOKEN__", &token);
    html = html.replace("<!-- INJECT_TASK_LIST -->", TASK_LIST_HTML);
    html = html.replace("<!-- INJECT_NETWORK -->", NETWORK_HTML);
    html = html.replace("<!-- INJECT_MODALS -->", MODALS_HTML);
    Html(html)
}

pub async fn serve_css() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/css")], STYLE_CSS)
}

pub async fn serve_js(State(token): State<String>) -> impl IntoResponse {
    let js = APP_JS.replace("__DISTGET_TOKEN__", &token);
    ([(header::CONTENT_TYPE, "application/javascript")], js)
}

pub async fn serve_i18n() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "application/javascript")], I18N_JS)
}

pub async fn serve_vue() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "application/javascript")], VUE_JS)
}

pub async fn serve_tailwind() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "application/javascript")], TAILWIND_JS)
}
