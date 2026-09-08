//! Device authorization, rotating tokens, signed requests, and existing Web sessions.
use anyhow::{Result, bail};
use axum::{
    Extension, Json, Router,
    body::{Body, to_bytes},
    extract::{Path as RoutePath, Request, State},
    http::{HeaderMap, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use ed25519_dalek::{Signature, VerifyingKey};
use rand::{RngCore, rngs::OsRng};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Principal {
    pub id: String,
    pub role: String,
    #[serde(skip)]
    pub cookie: Option<String>,
    #[serde(skip)]
    pub csrf: Option<String>,
}
#[derive(Clone)]
pub struct AuthState {
    pub store: Arc<Mutex<DeviceStore>>,
    pub http: reqwest::Client,
    pub bff_url: String,
    pub public_origin: String,
    pub read_token: Option<String>,
    pub bridge_key: Option<String>,
}

impl AuthState {
    pub fn open(path: &Path) -> Result<Self> {
        Ok(Self {
            store: Arc::new(Mutex::new(DeviceStore::open(path)?)),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            bff_url: "http://127.0.0.1:18081".into(),
            public_origin: "https://tokyo.montlok.com".into(),
            read_token: None,
            bridge_key: None,
        })
    }
    pub async fn validate_socket(&self, principal: &Principal, headers: &HeaderMap) -> bool {
        if let Some(token) = headers
            .get("authorization")
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.strip_prefix("Bearer "))
        {
            if principal.id == "service-reader" {
                return self
                    .read_token
                    .as_deref()
                    .is_some_and(|expected| constant_eq(expected.as_bytes(), token.as_bytes()));
            }
            let store = self.store.clone();
            let token = token.to_string();
            let id = principal.id.clone();
            let role = principal.role.clone();
            return tokio::task::spawn_blocking(move||{
                let Ok(store)=store.lock() else{return false;};
                store.db.query_row("SELECT COUNT(*) FROM devices d JOIN device_tokens t ON t.device_id=d.id WHERE t.hash=?1 AND t.kind='access' AND t.consumed=0 AND t.expires>?2 AND d.revoked=0 AND d.owner=?3 AND d.role=?4",
                    params![digest(&token),seconds(),id,role],|r|r.get::<_,i64>(0)).is_ok_and(|count|count==1)
            }).await.unwrap_or(false);
        }
        web_principal(self, headers, false)
            .await
            .is_ok_and(|current| current.id == principal.id && current.role == principal.role)
    }
}

pub fn routes() -> Router<crate::GatewayState> {
    Router::new()
        .route("/api/v2/device/authorizations", post(create_authorization))
        .route(
            "/api/v2/device/authorizations/{code}",
            get(authorization_details),
        )
        .route(
            "/api/v2/device/authorizations/{code}/approve",
            post(approve_authorization),
        )
        .route("/api/v2/device/tokens", post(exchange_token))
        .route("/api/v2/device/tokens/refresh", post(refresh_token))
        .route("/api/v2/devices", get(list_devices))
        .route("/api/v2/devices/{id}", delete(revoke_device))
}

pub async fn authenticate(
    State(state): State<crate::GatewayState>,
    mut request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path().to_owned();
    let public = path == "/healthz"
        || request.method() == Method::POST
            && matches!(
                path.as_str(),
                "/api/v2/device/authorizations"
                    | "/api/v2/device/tokens"
                    | "/api/v2/device/tokens/refresh"
            );
    if public {
        return next.run(request).await;
    }
    if let Some(origin) = request.headers().get("origin")
        && origin.to_str().ok() != Some(state.auth.public_origin.as_str())
    {
        return deny(StatusCode::FORBIDDEN, "来源与终端地址不一致");
    }
    let write = !matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    );
    let bearer = request
        .headers()
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .map(str::to_owned);
    let principal = if let Some(token) = bearer {
        if state
            .auth
            .read_token
            .as_deref()
            .is_some_and(|expected| constant_eq(expected.as_bytes(), token.as_bytes()))
        {
            if write && path != "/api/v2/query" {
                return deny(StatusCode::FORBIDDEN, "此连接用于事件查询");
            }
            Principal {
                id: "service-reader".into(),
                role: "viewer".into(),
                cookie: None,
                csrf: None,
            }
        } else {
            let method = request.method().to_string();
            let uri = request
                .uri()
                .path_and_query()
                .map(|p| p.as_str())
                .unwrap_or("/")
                .to_owned();
            let headers = request.headers().clone();
            let (parts, body) = request.into_parts();
            let bytes = match to_bytes(body, 2 * 1024 * 1024).await {
                Ok(bytes) => bytes,
                Err(_) => return deny(StatusCode::PAYLOAD_TOO_LARGE, "请求超过容量"),
            };
            request = Request::from_parts(parts, Body::from(bytes.clone()));
            let store = state.auth.store.clone();
            match tokio::task::spawn_blocking(move || {
                store
                    .lock()
                    .map_err(|_| anyhow::anyhow!("device store unavailable"))?
                    .verify_request(&token, &headers, &method, &uri, &bytes)
            })
            .await
            {
                Ok(Ok(principal)) => principal,
                _ => return deny(StatusCode::UNAUTHORIZED, "设备授权或签名无效"),
            }
        }
    } else {
        match web_principal(&state.auth, request.headers(), write).await {
            Ok(principal) => principal,
            Err(_) => return deny(StatusCode::UNAUTHORIZED, "请使用通行密钥登录"),
        }
    };
    if write
        && path != "/api/v2/query"
        && path != "/api/v2/legacy/query"
        && path != "/api/v2/workspaces"
        && principal.role == "viewer"
    {
        return deny(StatusCode::FORBIDDEN, "当前身份可查看此工作区");
    }
    request.extensions_mut().insert(principal);
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        "cache-control",
        axum::http::HeaderValue::from_static("no-store"),
    );
    response
}

async fn web_principal(state: &AuthState, headers: &HeaderMap, write: bool) -> Result<Principal> {
    let cookie = headers
        .get("cookie")
        .and_then(|h| h.to_str().ok())
        .filter(|c| c.contains("operator_session="))
        .ok_or_else(|| anyhow::anyhow!("session cookie missing"))?;
    let mut request = state
        .http
        .get(format!("{}/api/session", state.bff_url))
        .header("cookie", cookie);
    if cookie.contains("__Host-operator_session=") {
        request = request
            .header("X-Operator-Public", "1")
            .header("X-Forwarded-Proto", "https");
    }
    let response = request.send().await?.error_for_status()?;
    let session: Value = response.json().await?;
    let csrf = session["csrf"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("session csrf missing"))?;
    if write
        && !constant_eq(
            headers
                .get("X-Operator-CSRF")
                .map(|h| h.as_bytes())
                .unwrap_or(b""),
            csrf.as_bytes(),
        )
    {
        bail!("session csrf mismatch");
    }
    let id = session["operator"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("session principal missing"))?;
    let role = session["role"]
        .as_str()
        .filter(|role| matches!(*role, "admin" | "viewer" | "operator"))
        .ok_or_else(|| anyhow::anyhow!("session role invalid"))?;
    Ok(Principal {
        id: id.into(),
        role: role.into(),
        cookie: Some(cookie.into()),
        csrf: Some(csrf.into()),
    })
}
fn deny(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({"error":message}))).into_response()
}
fn seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
fn token() -> String {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}
fn digest(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}
fn constant_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |sum, (a, b)| sum | (a ^ b)) == 0
}

pub struct DeviceStore {
    db: Connection,
}
impl DeviceStore {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let db = Connection::open(path)?;
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.pragma_update(None, "synchronous", "FULL")?;
        db.execute_batch("CREATE TABLE IF NOT EXISTS device_requests(id TEXT PRIMARY KEY,code TEXT UNIQUE NOT NULL,name TEXT NOT NULL,platform TEXT NOT NULL,public_key BLOB NOT NULL,expires INTEGER NOT NULL,approved_by TEXT,role TEXT,consumed INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS devices(id TEXT PRIMARY KEY,owner TEXT NOT NULL,role TEXT NOT NULL,name TEXT NOT NULL,platform TEXT NOT NULL,public_key BLOB NOT NULL,created INTEGER NOT NULL,seen INTEGER NOT NULL,revoked INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS device_tokens(hash TEXT PRIMARY KEY,device_id TEXT NOT NULL,kind TEXT NOT NULL,expires INTEGER NOT NULL,consumed INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS device_nonces(device_id TEXT NOT NULL,nonce TEXT NOT NULL,seen INTEGER NOT NULL,PRIMARY KEY(device_id,nonce));")?;
        Ok(Self { db })
    }
    fn authorize(&mut self, request: AuthorizationRequest, origin: &str) -> Result<Value> {
        if request.device_name.is_empty()
            || request.device_name.len() > 120
            || !matches!(request.platform.as_str(), "macos" | "windows" | "linux")
        {
            bail!("device attributes invalid");
        }
        if request.public_key.len() != 44 {
            bail!("invalid Ed25519 public key encoding");
        }
        let key = STANDARD.decode(&request.public_key)?;
        let key: [u8; 32] = key
            .try_into()
            .map_err(|_| anyhow::anyhow!("Ed25519 key must be 32 bytes"))?;
        if VerifyingKey::from_bytes(&key)?.is_weak() {
            bail!("weak key");
        }
        let now = seconds();
        self.db
            .execute("DELETE FROM device_requests WHERE expires<?1", [now])?;
        let count: i64 = self.db.query_row(
            "SELECT COUNT(*) FROM device_requests WHERE expires>?1",
            [now],
            |r| r.get(0),
        )?;
        if count >= 60 {
            bail!("device authorization capacity reached");
        }
        let id = Uuid::now_v7().to_string();
        let random = token();
        let code = format!("{}-{}", &random[0..4], &random[4..8]).to_uppercase();
        self.db.execute("INSERT INTO device_requests(id,code,name,platform,public_key,expires) VALUES(?1,?2,?3,?4,?5,?6)",
            params![id,code,request.device_name,request.platform,key.as_slice(),now+600])?;
        Ok(
            json!({"authorization_id":id,"user_code":code,"verification_uri":format!("{origin}/device?code={code}"),"expires_in":600,"interval":5}),
        )
    }
    fn details(&self, code: &str) -> Result<Value> {
        Ok(self.db.query_row("SELECT id,code,name,platform,public_key,expires,approved_by FROM device_requests WHERE (code=?1 OR id=?1) AND expires>?2 AND consumed=0",
            params![code,seconds()],|row|Ok(json!({"authorization_id":row.get::<_,String>(0)?,"user_code":row.get::<_,String>(1)?,"device_name":row.get::<_,String>(2)?,
                "platform":row.get::<_,String>(3)?,"public_key_fingerprint":hex::encode(Sha256::digest(row.get::<_,Vec<u8>>(4)?)),"expires_at":row.get::<_,i64>(5)?,"approved":row.get::<_,Option<String>>(6)?.is_some()})))?)
    }
    fn approve(&mut self, code: &str, principal: &Principal) -> Result<Value> {
        let changed=self.db.execute("UPDATE device_requests SET approved_by=?2,role=?3 WHERE (code=?1 OR id=?1) AND expires>?4 AND consumed=0 AND approved_by IS NULL",
            params![code,principal.id,principal.role,seconds()])?;
        if changed != 1 {
            bail!("authorization expired or already decided");
        }
        self.details(code)
    }
    fn exchange(&mut self, request: TokenRequest) -> Result<Option<Value>> {
        let now = seconds();
        if request.timestamp.abs_diff(now) > 30 {
            bail!("signature timestamp expired");
        }
        let tx = self.db.transaction()?;
        let (name,platform,key,owner,role):(String,String,Vec<u8>,Option<String>,Option<String>)=tx.query_row(
            "SELECT name,platform,public_key,approved_by,role FROM device_requests WHERE id=?1 AND expires>?2 AND consumed=0",
            params![request.authorization_id,now],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?;
        verify(
            &key,
            format!(
                "device-authorize\n{}\n{}",
                request.authorization_id, request.timestamp
            )
            .as_bytes(),
            &request.signature,
        )?;
        let Some(owner) = owner else {
            return Ok(None);
        };
        let device = Uuid::now_v7().to_string();
        tx.execute("INSERT INTO devices(id,owner,role,name,platform,public_key,created,seen) VALUES(?1,?2,?3,?4,?5,?6,?7,?7)",
            params![device,owner,role.unwrap_or_else(||"viewer".into()),name,platform,key,now])?;
        tx.execute(
            "UPDATE device_requests SET consumed=1 WHERE id=?1",
            [request.authorization_id],
        )?;
        let tokens = issue_tokens(&tx, &device, now)?;
        tx.commit()?;
        Ok(Some(tokens))
    }
    fn refresh(&mut self, request: RefreshRequest) -> Result<Value> {
        let now = seconds();
        if now.abs_diff(request.timestamp) > 30
            || request.nonce.len() < 16
            || request.nonce.len() > 128
        {
            bail!("refresh signature context invalid");
        }
        let tx = self.db.transaction()?;
        let key: Vec<u8> = tx.query_row(
            "SELECT public_key FROM devices WHERE id=?1 AND revoked=0",
            [&request.device_id],
            |r| r.get(0),
        )?;
        verify(
            &key,
            format!(
                "device-refresh\n{}\n{}\n{}\n{}",
                request.device_id,
                request.nonce,
                digest(&request.refresh_token),
                request.timestamp
            )
            .as_bytes(),
            &request.signature,
        )?;
        let consumed:bool=tx.query_row("SELECT consumed FROM device_tokens WHERE hash=?1 AND device_id=?2 AND kind='refresh' AND expires>?3",
            params![digest(&request.refresh_token),request.device_id,now],|r|r.get(0))?;
        if consumed {
            tx.execute(
                "UPDATE devices SET revoked=1 WHERE id=?1",
                [request.device_id],
            )?;
            tx.commit()?;
            bail!("refresh token reuse revoked the device");
        }
        tx.execute(
            "INSERT INTO device_nonces VALUES(?1,?2,?3)",
            params![request.device_id, request.nonce, now],
        )?;
        tx.execute(
            "UPDATE device_tokens SET consumed=1 WHERE hash=?1",
            [digest(&request.refresh_token)],
        )?;
        let tokens = issue_tokens(&tx, &request.device_id, now)?;
        tx.commit()?;
        Ok(tokens)
    }
    fn verify_request(
        &mut self,
        token: &str,
        headers: &HeaderMap,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> Result<Principal> {
        let header = |name: &str| {
            headers
                .get(name)
                .and_then(|h| h.to_str().ok())
                .ok_or_else(|| anyhow::anyhow!("missing signature header"))
        };
        let device = header("X-Montlok-Device")?;
        let nonce = header("X-Montlok-Nonce")?;
        let timestamp = header("X-Montlok-Timestamp")?.parse::<i64>()?;
        let signature = header("X-Montlok-Signature")?;
        let now = seconds();
        if now.abs_diff(timestamp) > 30 || nonce.len() < 16 || nonce.len() > 128 {
            bail!("request nonce or time invalid");
        }
        let tx = self.db.transaction()?;
        let (id,role,key):(String,String,Vec<u8>)=tx.query_row("SELECT d.owner,d.role,d.public_key FROM devices d JOIN device_tokens t ON t.device_id=d.id
            WHERE d.id=?1 AND d.revoked=0 AND t.hash=?2 AND t.kind='access' AND t.expires>?3 AND t.consumed=0",params![device,digest(token),now],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
        verify(
            &key,
            format!(
                "v2\n{device}\n{nonce}\n{method}\n{path}\n{}\n{timestamp}",
                hex::encode(Sha256::digest(body))
            )
            .as_bytes(),
            signature,
        )?;
        tx.execute("DELETE FROM device_nonces WHERE seen<?1", [now - 60])?;
        tx.execute(
            "INSERT INTO device_nonces VALUES(?1,?2,?3)",
            params![device, nonce, now],
        )?;
        tx.execute(
            "UPDATE devices SET seen=?2 WHERE id=?1",
            params![device, now],
        )?;
        tx.commit()?;
        Ok(Principal {
            id,
            role,
            cookie: None,
            csrf: None,
        })
    }
}

fn verify(public_key: &[u8], message: &[u8], signature: &str) -> Result<()> {
    let key: [u8; 32] = public_key.try_into()?;
    let verifying = VerifyingKey::from_bytes(&key)?;
    let signature = Signature::from_slice(&STANDARD.decode(signature)?)?;
    verifying.verify_strict(message, &signature)?;
    Ok(())
}
fn issue_tokens(tx: &rusqlite::Transaction<'_>, device: &str, now: i64) -> Result<Value> {
    let access = token();
    let refresh = token();
    tx.execute(
        "INSERT INTO device_tokens(hash,device_id,kind,expires) VALUES(?1,?2,'access',?3)",
        params![digest(&access), device, now + 300],
    )?;
    tx.execute(
        "INSERT INTO device_tokens(hash,device_id,kind,expires) VALUES(?1,?2,'refresh',?3)",
        params![digest(&refresh), device, now + 2592000],
    )?;
    Ok(
        json!({"device_id":device,"access_token":access,"expires_in":300,"refresh_token":refresh,"refresh_expires_in":2592000}),
    )
}
#[derive(Deserialize)]
struct AuthorizationRequest {
    device_name: String,
    public_key: String,
    platform: String,
}
#[derive(Deserialize)]
struct TokenRequest {
    authorization_id: String,
    signature: String,
    timestamp: i64,
}
#[derive(Deserialize)]
struct RefreshRequest {
    device_id: String,
    refresh_token: String,
    nonce: String,
    signature: String,
    timestamp: i64,
}
fn as_api(error: anyhow::Error) -> crate::ApiError {
    tracing::debug!(?error, "device request rejected");
    crate::ApiError::BadRequest("设备授权无效、已过期或签名不匹配".into())
}

async fn create_authorization(
    State(state): State<crate::GatewayState>,
    Json(request): Json<AuthorizationRequest>,
) -> Result<(StatusCode, Json<Value>), crate::ApiError> {
    let auth = state.auth.clone();
    let value = tokio::task::spawn_blocking(move || {
        auth.store
            .lock()
            .expect("device store")
            .authorize(request, &auth.public_origin)
    })
    .await
    .map_err(|e| crate::ApiError::Internal(e.into()))?
    .map_err(as_api)?;
    Ok((StatusCode::CREATED, Json(value)))
}
async fn authorization_details(
    State(state): State<crate::GatewayState>,
    RoutePath(code): RoutePath<String>,
) -> Result<Json<Value>, crate::ApiError> {
    let store = state.auth.store;
    Ok(Json(
        tokio::task::spawn_blocking(move || store.lock().expect("device store").details(&code))
            .await
            .map_err(|e| crate::ApiError::Internal(e.into()))?
            .map_err(as_api)?,
    ))
}
async fn approve_authorization(
    State(state): State<crate::GatewayState>,
    Extension(principal): Extension<Principal>,
    RoutePath(code): RoutePath<String>,
) -> Result<Json<Value>, crate::ApiError> {
    if principal.cookie.is_none() {
        return Err(crate::ApiError::BadRequest(
            "请在已登录的浏览器中授权设备".into(),
        ));
    }
    let store = state.auth.store;
    Ok(Json(
        tokio::task::spawn_blocking(move || {
            store
                .lock()
                .expect("device store")
                .approve(&code, &principal)
        })
        .await
        .map_err(|e| crate::ApiError::Internal(e.into()))?
        .map_err(as_api)?,
    ))
}
async fn exchange_token(
    State(state): State<crate::GatewayState>,
    Json(request): Json<TokenRequest>,
) -> Result<(StatusCode, Json<Value>), crate::ApiError> {
    let store = state.auth.store;
    let tokens =
        tokio::task::spawn_blocking(move || store.lock().expect("device store").exchange(request))
            .await
            .map_err(|e| crate::ApiError::Internal(e.into()))?
            .map_err(as_api)?;
    Ok(match tokens {
        Some(tokens) => (StatusCode::OK, Json(tokens)),
        None => (
            StatusCode::ACCEPTED,
            Json(json!({"status":"authorization_pending"})),
        ),
    })
}
async fn refresh_token(
    State(state): State<crate::GatewayState>,
    Json(request): Json<RefreshRequest>,
) -> Result<Json<Value>, crate::ApiError> {
    let store = state.auth.store;
    Ok(Json(
        tokio::task::spawn_blocking(move || store.lock().expect("device store").refresh(request))
            .await
            .map_err(|e| crate::ApiError::Internal(e.into()))?
            .map_err(as_api)?,
    ))
}
async fn list_devices(
    State(state): State<crate::GatewayState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Value>, crate::ApiError> {
    let store = state.auth.store;
    let result=tokio::task::spawn_blocking(move||->Result<Value>{let store=store.lock().expect("device store");let mut query=store.db.prepare("SELECT id,name,platform,created,seen,revoked FROM devices WHERE owner=?1 ORDER BY created DESC")?;
        let rows=query.query_map([principal.id],|r|Ok(json!({"device_id":r.get::<_,String>(0)?,"device_name":r.get::<_,String>(1)?,"platform":r.get::<_,String>(2)?,"created_at":r.get::<_,i64>(3)?,"last_seen_at":r.get::<_,i64>(4)?,"revoked":r.get::<_,bool>(5)?})))?.collect::<rusqlite::Result<Vec<_>>>()?;Ok(json!(rows))}).await.map_err(|e|crate::ApiError::Internal(e.into()))?.map_err(as_api)?;
    Ok(Json(result))
}
async fn revoke_device(
    State(state): State<crate::GatewayState>,
    Extension(principal): Extension<Principal>,
    RoutePath(id): RoutePath<String>,
) -> Result<StatusCode, crate::ApiError> {
    let store = state.auth.store;
    let count = tokio::task::spawn_blocking(move || {
        store.lock().expect("device store").db.execute(
            "UPDATE devices SET revoked=1 WHERE id=?1 AND owner=?2",
            params![id, principal.id],
        )
    })
    .await
    .map_err(|e| crate::ApiError::Internal(e.into()))?
    .map_err(|e| as_api(e.into()))?;
    if count == 0 {
        return Err(crate::ApiError::NotFound("设备不存在".into()));
    }
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    #[test]
    fn authorized_device_requests_are_signed_and_cannot_replay() {
        let mut store = DeviceStore::open(Path::new(":memory:")).unwrap();
        let signing = SigningKey::from_bytes(&[7u8; 32]);
        let request = store
            .authorize(
                AuthorizationRequest {
                    device_name: "Mac".into(),
                    platform: "macos".into(),
                    public_key: STANDARD.encode(signing.verifying_key().as_bytes()),
                },
                "https://example.com",
            )
            .unwrap();
        let principal = Principal {
            id: "operator".into(),
            role: "admin".into(),
            cookie: Some("cookie".into()),
            csrf: None,
        };
        store
            .approve(request["user_code"].as_str().unwrap(), &principal)
            .unwrap();
        let id = request["authorization_id"].as_str().unwrap();
        let time = seconds();
        let tokens = store
            .exchange(TokenRequest {
                authorization_id: id.into(),
                timestamp: time,
                signature: STANDARD.encode(
                    signing
                        .sign(format!("device-authorize\n{id}\n{time}").as_bytes())
                        .to_bytes(),
                ),
            })
            .unwrap()
            .unwrap();
        let device = tokens["device_id"].as_str().unwrap();
        let access = tokens["access_token"].as_str().unwrap();
        let nonce = "random-unique-nonce-0123456789";
        let mut headers = HeaderMap::new();
        for (key, value) in [
            ("X-Montlok-Device", device.to_string()),
            ("X-Montlok-Nonce", nonce.into()),
            ("X-Montlok-Timestamp", time.to_string()),
            (
                "X-Montlok-Signature",
                STANDARD.encode(
                    signing
                        .sign(
                            format!(
                                "v2\n{device}\n{nonce}\nGET\n/api/v2/bootstrap\n{}\n{time}",
                                hex::encode(Sha256::digest([]))
                            )
                            .as_bytes(),
                        )
                        .to_bytes(),
                ),
            ),
        ] {
            headers.insert(
                axum::http::HeaderName::from_bytes(key.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        assert_eq!(
            store
                .verify_request(access, &headers, "GET", "/api/v2/bootstrap", b"")
                .unwrap()
                .id,
            "operator"
        );
        assert!(
            store
                .verify_request(access, &headers, "GET", "/api/v2/bootstrap", b"")
                .is_err()
        );
        assert!(
            store
                .verify_request(
                    access,
                    &headers,
                    "POST",
                    "/api/v2/operations/prepare",
                    b"{}"
                )
                .is_err()
        );
    }
}
