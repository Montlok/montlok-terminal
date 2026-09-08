//! Typed-context bridge to the existing, complete operator API during migration.
use crate::{ApiError, GatewayState, auth::Principal};
use axum::{
    Extension, Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::header,
    response::Response,
    routing::get,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub fn routes() -> Router<GatewayState> {
    Router::new()
        .route("/api/v2/legacy/{*path}", get(forward).post(forward))
        .route("/api/v2/artifacts", get(forward_alias).post(forward_alias))
        .route("/api/v2/model-releases", get(forward_alias))
        .route("/api/v2/model-releases/{id}", get(forward_alias))
        .route("/api/v2/strategy-groups", get(forward_alias))
        .route("/api/v2/strategy-groups/{id}", get(forward_alias))
}

async fn forward_alias(
    State(state): State<GatewayState>,
    Extension(principal): Extension<Principal>,
    request: Request,
) -> Result<Response, ApiError> {
    let path = request
        .uri()
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or_default()
        .replacen("/api/v2/", "/api/", 1);
    send(state, principal, request, path).await
}
async fn forward(
    State(state): State<GatewayState>,
    Extension(principal): Extension<Principal>,
    request: Request,
) -> Result<Response, ApiError> {
    let path = request
        .uri()
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or_default()
        .replacen("/api/v2/legacy/", "/api/", 1);
    send(state, principal, request, path).await
}
async fn send(
    state: GatewayState,
    principal: Principal,
    request: Request,
    path: String,
) -> Result<Response, ApiError> {
    let resource = path
        .strip_prefix("/api/")
        .unwrap_or_default()
        .split(['/', '?'])
        .next()
        .unwrap_or_default();
    if !matches!(
        resource,
        "catalog"
            | "profiles"
            | "account"
            | "strategy-groups"
            | "artifacts"
            | "model-releases"
            | "query"
            | "prepare"
            | "execute"
            | "history"
            | "operations"
            | "market"
            | "host"
    ) {
        return Err(ApiError::NotFound("此业务接口不存在".into()));
    }
    if path.contains("..") || path.contains('\\') || path.len() > 4096 {
        return Err(ApiError::BadRequest("接口路径格式不正确".into()));
    }
    let method = request.method().clone();
    let body = to_bytes(request.into_body(), 1_048_576)
        .await
        .map_err(|_| ApiError::BadRequest("业务请求超过容量".into()))?;
    let mut upstream = state
        .auth
        .http
        .request(method.clone(), format!("{}{}", state.auth.bff_url, path))
        .timeout(std::time::Duration::from_secs(
            if resource == "execute" || resource == "prepare" {
                65
            } else {
                15
            },
        ))
        .header("content-type", "application/json")
        .body(body.clone());
    if let Some(cookie) = principal.cookie {
        if cookie.contains("__Host-operator_session=") {
            upstream = upstream
                .header("X-Operator-Public", "1")
                .header("X-Forwarded-Proto", "https");
        }
        upstream = upstream.header("cookie", cookie);
        if let Some(csrf) = principal.csrf {
            upstream = upstream.header("X-Operator-CSRF", csrf);
        }
    } else {
        let secret = state
            .auth
            .bridge_key
            .as_ref()
            .ok_or_else(|| ApiError::Unavailable("原生终端业务连接尚未配置".into()))?;
        let encoded = STANDARD.encode(
            serde_json::to_vec(&json!({"id":principal.id,"role":principal.role}))
                .map_err(|e| ApiError::Internal(e.into()))?,
        );
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .to_string();
        let nonce = Uuid::now_v7().simple().to_string();
        let material = format!(
            "{method}\n{path}\n{timestamp}\n{nonce}\n{encoded}\n{}",
            hex::encode(Sha256::digest(&body))
        );
        let mut signer = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
            .map_err(|e| ApiError::Internal(e.into()))?;
        signer.update(material.as_bytes());
        upstream = upstream
            .header("X-Montlok-Bridge-Time", timestamp)
            .header("X-Montlok-Bridge-Nonce", nonce)
            .header("X-Montlok-Bridge-Principal", encoded)
            .header(
                "X-Montlok-Bridge-Signature",
                hex::encode(signer.finalize().into_bytes()),
            );
    }
    let response = upstream
        .send()
        .await
        .map_err(|_| ApiError::Unavailable("业务请求结果待查询".into()))?;
    let status = response.status();
    let content_type = response.headers().get("content-type").cloned();
    let bytes = response
        .bytes()
        .await
        .map_err(|_| ApiError::Unavailable("业务响应未完整接收".into()))?;
    let mut output = Response::builder()
        .status(status)
        .header(header::CACHE_CONTROL, "no-store");
    if let Some(content_type) = content_type {
        output = output.header(header::CONTENT_TYPE, content_type);
    }
    output
        .body(Body::from(bytes))
        .map_err(|e| ApiError::Internal(e.into()))
}
