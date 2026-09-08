#![forbid(unsafe_code)]

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use axum::{
    Extension, Json, Router,
    extract::{
        Path as AxumPath, Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use futures_util::{SinkExt, StreamExt};
use montlok_contracts::{PROTOCOL_VERSION, v2};
use prost::Message as ProstMessage;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    sync::broadcast,
    time,
};
use tower_http::{compression::CompressionLayer, trace::TraceLayer};
use uuid::Uuid;

const OPERATION_TTL_SECONDS: i64 = 60;

pub mod auth;
pub mod bus;
pub mod legacy;
pub mod telemetry;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    Unavailable(String),
    #[error("internal service error")]
    Internal(#[source] anyhow::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, detail) = match &self {
            Self::BadRequest(detail) => (StatusCode::BAD_REQUEST, "BAD_REQUEST", detail.clone()),
            Self::NotFound(detail) => (StatusCode::NOT_FOUND, "NOT_FOUND", detail.clone()),
            Self::Conflict(detail) => (StatusCode::CONFLICT, "CONFLICT", detail.clone()),
            Self::Unavailable(detail) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "UNAVAILABLE",
                detail.clone(),
            ),
            Self::Internal(error) => {
                tracing::error!(error = ?error, "gateway request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL",
                    "服务暂时无法完成请求".into(),
                )
            }
        };
        (status, Json(json!({ "code": code, "error": detail }))).into_response()
    }
}

mod journal;

#[derive(Clone)]
pub struct EventHub {
    pub metrics: Arc<telemetry::Metrics>,
    journal: Arc<Mutex<journal::Journal>>,
    tx: broadcast::Sender<v2::EventEnvelope>,
}

impl Default for EventHub {
    fn default() -> Self {
        Self::new()
    }
}

impl EventHub {
    pub fn new() -> Self {
        Self::with_journal(journal::Journal::memory().expect("in-memory replay journal"))
    }
    pub fn open(path: &Path) -> Result<Self> {
        Ok(Self::with_journal(journal::Journal::open(path)?))
    }
    fn with_journal(journal: journal::Journal) -> Self {
        let (tx, _) = broadcast::channel(8_192);
        Self {
            metrics: Arc::new(telemetry::Metrics::default()),
            journal: Arc::new(Mutex::new(journal)),
            tx,
        }
    }
    pub async fn publish(&self, event: v2::EventEnvelope) -> Result<u64, ApiError> {
        let sequence = event.stream_seq;
        self.publish_batch(vec![event]).await?;
        Ok(sequence)
    }
    pub async fn publish_batch(&self, events: Vec<v2::EventEnvelope>) -> Result<(), ApiError> {
        let started = std::time::Instant::now();
        let metrics = self.metrics.clone();
        let journal = self.journal.clone();
        let tx = self.tx.clone();
        tokio::task::spawn_blocking(move || {
            let mut journal = journal
                .lock()
                .map_err(|_| ApiError::Unavailable("事件日志锁异常".into()))?;
            let inserted = journal
                .append_many(&events)
                .map_err(|e| ApiError::Conflict(e.to_string()))?;
            let new_count = inserted.iter().filter(|value| **value).count() as u64;
            let duplicate_count = inserted.len() as u64 - new_count;
            for (event, new) in events.into_iter().zip(inserted) {
                if new {
                    let _ = tx.send(event);
                }
            }
            metrics.published(new_count, duplicate_count, started.elapsed());
            Ok(())
        })
        .await
        .map_err(|e| ApiError::Internal(e.into()))?
    }
    pub async fn events_after(
        &self,
        stream: &str,
        after: u64,
        limit: usize,
    ) -> Result<Vec<v2::EventEnvelope>, ApiError> {
        self.replay(stream, after, u64::MAX, limit).await
    }
    pub async fn replay(
        &self,
        stream: &str,
        after: u64,
        through: u64,
        limit: usize,
    ) -> Result<Vec<v2::EventEnvelope>, ApiError> {
        let journal = self.journal.clone();
        let stream = stream.to_owned();
        tokio::task::spawn_blocking(move || {
            journal
                .lock()
                .map_err(|_| anyhow::anyhow!("event journal lock poisoned"))?
                .after(&stream, after, through, limit)
        })
        .await
        .map_err(|e| ApiError::Internal(e.into()))?
        .map_err(ApiError::Internal)
    }
    pub async fn watermarks(&self) -> Result<HashMap<String, u64>, ApiError> {
        let journal = self.journal.clone();
        tokio::task::spawn_blocking(move || {
            journal
                .lock()
                .map_err(|_| anyhow::anyhow!("event journal lock poisoned"))?
                .watermarks()
        })
        .await
        .map_err(|e| ApiError::Internal(e.into()))?
        .map_err(ApiError::Internal)
    }
    pub async fn snapshot(
        &self,
        topics: Vec<String>,
        context: v2::TerminalContext,
    ) -> Result<(HashMap<String, u64>, Vec<v2::EventEnvelope>), ApiError> {
        let journal = self.journal.clone();
        tokio::task::spawn_blocking(move || {
            journal
                .lock()
                .map_err(|_| anyhow::anyhow!("event journal lock poisoned"))?
                .snapshot(&topics, &context)
        })
        .await
        .map_err(|e| ApiError::Internal(e.into()))?
        .map_err(ApiError::Internal)
    }
    pub fn subscribe(&self) -> broadcast::Receiver<v2::EventEnvelope> {
        self.tx.subscribe()
    }
}

#[derive(Clone)]
pub struct GatewayState {
    pub events: EventHub,
    pub auth: auth::AuthState,
    operations: Arc<OperationStore>,
    workspaces: Arc<Vec<Value>>,
    fields: Arc<Value>,
    control: Arc<ControlBackend>,
}

impl GatewayState {
    pub fn new(database: impl AsRef<Path>, control_socket: Option<PathBuf>) -> Result<Self> {
        Ok(Self {
            auth: auth::AuthState::open(&database.as_ref().with_extension("devices.sqlite"))?,
            events: EventHub::open(&database.as_ref().with_extension("events.sqlite"))?,
            operations: Arc::new(OperationStore::open(database.as_ref())?),
            workspaces: Arc::new(built_in_workspaces()?),
            fields: Arc::new(built_in_fields()?),
            control: Arc::new(ControlBackend {
                socket: control_socket,
            }),
        })
    }

    pub fn ephemeral() -> Result<Self> {
        Ok(Self {
            auth: auth::AuthState::open(Path::new(":memory:"))?,
            events: EventHub::new(),
            operations: Arc::new(OperationStore::memory()?),
            workspaces: Arc::new(built_in_workspaces()?),
            fields: Arc::new(built_in_fields()?),
            control: Arc::new(ControlBackend { socket: None }),
        })
    }
}

pub fn router(state: GatewayState) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/metrics", get(metrics))
        .route("/api/v2/catalog", get(catalog))
        .route("/api/v2/fields", get(fields))
        .route("/api/v2/bootstrap", get(bootstrap))
        .route("/api/v2/runs/{run_id}/snapshot", get(run_snapshot))
        .route("/api/v2/events", get(events))
        .route("/api/v2/query", post(query))
        .route("/api/v2/workspaces", get(workspaces))
        .route("/api/v2/workspaces/{workspace_id}", put(save_workspace))
        .route("/api/v2/stream", get(stream))
        .route("/api/v2/operations/prepare", post(prepare_operation))
        .route(
            "/api/v2/operations/{operation_id}/execute",
            post(execute_operation),
        )
        .route("/api/v2/operations/{operation_id}", get(get_operation))
        .merge(auth::routes())
        .merge(legacy::routes())
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::authenticate,
        ))
        .with_state(state)
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok", "protocolVersion": PROTOCOL_VERSION }))
}
async fn metrics(State(state): State<GatewayState>) -> impl IntoResponse {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        state.events.metrics.prometheus(),
    )
}

async fn catalog(State(state): State<GatewayState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(catalog_value(&state).await?))
}

async fn catalog_value(state: &GatewayState) -> Result<Value, ApiError> {
    let (_, events) = state
        .events
        .snapshot(vec!["*".into()], Default::default())
        .await?;
    let mut accounts = std::collections::BTreeSet::new();
    let mut groups = std::collections::BTreeSet::new();
    let mut runs = std::collections::BTreeMap::<String, Value>::new();
    let mut instruments = std::collections::BTreeSet::new();
    for event in events {
        if !event.account_id.is_empty() {
            accounts.insert(event.account_id.clone());
        }
        if !event.strategy_group_id.is_empty() {
            groups.insert(event.strategy_group_id.clone());
        }
        if !event.instrument_id.is_empty() {
            instruments.insert(event.instrument_id.clone());
        }
        if !event.run_id.is_empty() {
            let run=runs.entry(event.run_id.clone()).or_insert_with(||json!({"id":event.run_id,"account_id":event.account_id,"strategy_group_id":event.strategy_group_id,"status":null}));
            if let Some(v2::event_envelope::Payload::RunStateChanged(status)) = event.payload {
                run["status"] = json!(
                    v2::RunState::try_from(status.current)
                        .ok()
                        .map(|state| state.as_str_name())
                );
            }
        }
    }
    let objects = |ids: std::collections::BTreeSet<String>| {
        ids.into_iter()
            .map(|id| json!({"id":id}))
            .collect::<Vec<_>>()
    };
    Ok(
        json!({"accounts":objects(accounts),"strategy_groups":objects(groups),"runs":runs.into_values().collect::<Vec<_>>(),"instruments":objects(instruments),
        "model_releases":[],"datasets":[{"id":"events","label":"运行事件"},{"id":"operation_receipts","label":"操作回执"}]}),
    )
}

async fn fields(State(state): State<GatewayState>) -> Json<Value> {
    Json(state.fields.as_ref().clone())
}

async fn workspaces(
    State(state): State<GatewayState>,
    Extension(principal): Extension<auth::Principal>,
) -> Result<Json<Value>, ApiError> {
    let store = state.operations.clone();
    let defaults = state.workspaces.as_ref().clone();
    let result = tokio::task::spawn_blocking(move || -> Result<Vec<Value>> {
        let db = store
            .db
            .lock()
            .map_err(|_| anyhow::anyhow!("workspace store unavailable"))?;
        let mut query = db.prepare("SELECT workspace_json FROM workspaces WHERE owner=?1")?;
        let saved = query
            .query_map([principal.id], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut workspaces = defaults
            .into_iter()
            .map(|value| (value["id"].as_str().unwrap_or_default().to_string(), value))
            .collect::<std::collections::BTreeMap<_, _>>();
        for text in saved {
            let value: Value = serde_json::from_str(&text)?;
            workspaces.insert(value["id"].as_str().unwrap_or_default().to_string(), value);
        }
        Ok(workspaces.into_values().collect())
    })
    .await
    .map_err(|e| ApiError::Internal(e.into()))?
    .map_err(ApiError::Internal)?;
    Ok(Json(json!(result)))
}

async fn save_workspace(
    State(state): State<GatewayState>,
    Extension(principal): Extension<auth::Principal>,
    AxumPath(workspace_id): AxumPath<String>,
    Json(workspace): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    if workspace.get("id").and_then(Value::as_str) != Some(workspace_id.as_str()) {
        return Err(ApiError::BadRequest("路径与工作区 id 不一致".into()));
    }
    if workspace.get("schemaVersion").and_then(Value::as_u64) != Some(2) {
        return Err(ApiError::BadRequest("仅接受 schemaVersion 2".into()));
    }
    if workspace_id.len() > 96
        || !workspace_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
    {
        return Err(ApiError::BadRequest("工作区 id 格式不正确".into()));
    }
    let panels = workspace
        .get("panels")
        .and_then(Value::as_array)
        .filter(|p| !p.is_empty() && p.len() <= 64)
        .ok_or_else(|| ApiError::BadRequest("请选择工作区面板".into()))?;
    let known = state
        .workspaces
        .iter()
        .flat_map(|w| w["panels"].as_array().into_iter().flatten())
        .filter_map(|p| p["id"].as_str())
        .collect::<std::collections::HashSet<_>>();
    if panels
        .iter()
        .any(|panel| panel["id"].as_str().is_none_or(|id| !known.contains(id)))
    {
        return Err(ApiError::BadRequest("工作区包含未知面板".into()));
    }
    let store = state.operations.clone();
    let serialized = serde_json::to_string(&workspace).map_err(|e| ApiError::Internal(e.into()))?;
    tokio::task::spawn_blocking(move||->Result<()>{
        store.db.lock().map_err(|_|anyhow::anyhow!("workspace store unavailable"))?.execute(
            "INSERT INTO workspaces(owner,id,workspace_json) VALUES(?1,?2,?3) ON CONFLICT(owner,id) DO UPDATE SET workspace_json=excluded.workspace_json",
            params![principal.id,workspace_id,serialized])?;Ok(())
    }).await.map_err(|e|ApiError::Internal(e.into()))?.map_err(ApiError::Internal)?;
    Ok(Json(workspace))
}

async fn bootstrap(
    State(state): State<GatewayState>,
    Extension(principal): Extension<auth::Principal>,
) -> Result<Json<Value>, ApiError> {
    let watermarks = state.events.watermarks().await?;
    let catalog = catalog_value(&state).await?;
    Ok(Json(json!({
        "protocol_version": PROTOCOL_VERSION,
        "principal": principal,
        "catalog": catalog,
        "workspaces": state.workspaces.as_ref(),
        "stream_url": "/api/v2/stream",
        "watermarks": watermarks
    })))
}

#[derive(Deserialize)]
struct EventQuery {
    stream: String,
    #[serde(default)]
    after_seq: u64,
    #[serde(default = "default_event_limit")]
    limit: usize,
}

fn default_event_limit() -> usize {
    500
}

async fn events(
    State(state): State<GatewayState>,
    Query(request): Query<EventQuery>,
) -> Result<Json<Value>, ApiError> {
    if request.stream.len() > 160 || request.limit == 0 || request.limit > 2_048 {
        return Err(ApiError::BadRequest("事件查询范围不正确".into()));
    }
    let rows = state
        .events
        .events_after(&request.stream, request.after_seq, request.limit)
        .await?;
    Ok(Json(
        serde_json::to_value(rows).map_err(|error| ApiError::Internal(error.into()))?,
    ))
}

async fn run_snapshot(
    State(state): State<GatewayState>,
    AxumPath(run_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    let stream = format!("run.{run_id}");
    let (watermarks, rows) = state
        .events
        .snapshot(
            vec![stream.clone()],
            v2::TerminalContext {
                run_id: run_id.clone(),
                ..Default::default()
            },
        )
        .await?;
    let source_event_seq = watermarks
        .get(&stream)
        .copied()
        .ok_or_else(|| ApiError::NotFound("该运行尚未接入事件日志".into()))?;
    Ok(Json(json!({
        "snapshot_seq": source_event_seq,
        "source_event_seq": source_event_seq,
        "context": {"run_id": run_id},
        "data": {"events": rows}
    })))
}

#[derive(Debug, Deserialize)]
struct StructuredQuery {
    dataset: String,
    fields: Vec<String>,
    #[serde(default)]
    filters: Vec<QueryFilter>,
    #[serde(default = "default_query_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
struct QueryFilter {
    field: String,
    op: String,
    value: Value,
}

fn default_query_limit() -> usize {
    1_000
}

async fn query(
    State(state): State<GatewayState>,
    Json(request): Json<StructuredQuery>,
) -> Result<Json<Value>, ApiError> {
    if request.dataset != "events" {
        return Err(ApiError::BadRequest(
            "该数据集由 projector 查询服务提供".into(),
        ));
    }
    if request.fields.is_empty()
        || request.fields.len() > 128
        || request.limit == 0
        || request.limit > 100_000
    {
        return Err(ApiError::BadRequest("字段或行数范围不正确".into()));
    }
    let stream = request
        .filters
        .iter()
        .find(|filter| filter.field == "stream" && filter.op == "eq")
        .and_then(|filter| filter.value.as_str())
        .ok_or_else(|| ApiError::BadRequest("events 查询必须限定 stream".into()))?;
    let rows = state.events.events_after(stream, 0, request.limit).await?;
    let watermark = rows.last().map(|event| event.stream_seq).unwrap_or(0);
    let values = rows
        .into_iter()
        .map(|event| {
            let value = serde_json::to_value(event).unwrap_or(Value::Null);
            let mut selected = serde_json::Map::new();
            for field in &request.fields {
                selected.insert(
                    field.clone(),
                    value.get(field).cloned().unwrap_or(Value::Null),
                );
            }
            Value::Object(selected)
        })
        .collect::<Vec<_>>();
    Ok(Json(
        json!({"schema": request.fields, "rows": values, "source_event_seq": watermark}),
    ))
}

async fn stream(
    ws: WebSocketUpgrade,
    State(state): State<GatewayState>,
    Extension(principal): Extension<auth::Principal>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    ws.protocols(["montlok.protobuf.v2"])
        .on_upgrade(move |socket| stream_socket(socket, state, principal, headers))
}

async fn send_server(socket: &mut WebSocket, message: v2::ServerMessage) -> Result<()> {
    let mut encoded = Vec::with_capacity(message.encoded_len());
    message.encode(&mut encoded)?;
    time::timeout(
        Duration::from_secs(5),
        socket.send(Message::Binary(encoded.into())),
    )
    .await??;
    Ok(())
}

async fn stream_socket(
    mut socket: WebSocket,
    state: GatewayState,
    principal: auth::Principal,
    headers: axum::http::HeaderMap,
) {
    let first = time::timeout(Duration::from_secs(10), socket.next()).await;
    let Ok(Some(Ok(Message::Binary(first)))) = first else {
        return;
    };
    let Ok(v2::ClientMessage {
        message: Some(v2::client_message::Message::Hello(hello)),
    }) = v2::ClientMessage::decode(first)
    else {
        return;
    };
    if hello.protocol_version != PROTOCOL_VERSION {
        let _ = send_server(
            &mut socket,
            server_error("PROTOCOL_VERSION", false, "客户端协议版本不兼容"),
        )
        .await;
        return;
    }
    let resume: HashMap<_, _> = hello
        .resume_positions
        .into_iter()
        .map(|p| (p.stream, p.last_stream_seq))
        .collect();
    let mut receiver = state.events.subscribe();
    let mut subscription: Option<v2::Subscribe> = None;
    let mut positions = HashMap::<String, u64>::new();
    let mut heartbeat = time::interval(Duration::from_secs(5));
    heartbeat.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            incoming=socket.next()=> {
                match incoming {
                    Some(Ok(Message::Binary(bytes)))=>{
                        let request=match v2::ClientMessage::decode(bytes){Ok(r)=>r,Err(_)=>break};
                        match request.message {
                            Some(v2::client_message::Message::Subscribe(mut next))=>{
                                if next.topics.is_empty() || next.topics.len()>32 || next.topics.iter().any(|topic|topic.len()>160) {break;}
                                if next.request_id.is_empty(){next.request_id=Uuid::now_v7().to_string();}
                                match deliver_initial(&mut socket,&state.events,&next,&resume).await {
                                    Ok(watermarks)=>{positions=watermarks;subscription=Some(next);}
                                    Err(error)=>{tracing::warn!(?error,"snapshot delivery failed");break;}
                                }
                            }
                            Some(v2::client_message::Message::Unsubscribe(request))=>{
                                if subscription.as_ref().is_some_and(|s|s.request_id==request.subscription_id) {subscription=None;positions.clear();}
                            }
                            Some(v2::client_message::Message::RequestSnapshot(request))=>{
                                if let Some(current)=subscription.as_ref().filter(|s|s.request_id==request.subscription_id) {
                                    match deliver_initial(&mut socket,&state.events,current,&HashMap::new()).await {
                                        Ok(watermarks)=>positions=watermarks,Err(_)=>break,
                                    }
                                }
                            }
                            _=>{}
                        }
                    }
                    Some(Ok(Message::Ping(bytes)))=>{if socket.send(Message::Pong(bytes)).await.is_err(){break;}}
                    Some(Ok(Message::Close(_)))|Some(Err(_))|None=>break,
                    _=>{}
                }
            }
            event=receiver.recv()=>match event {
                Ok(event)=>{
                    let Some(current)=&subscription else{continue;};
                    if !current.topics.iter().any(|t|t=="*"||t==&event.stream){continue;}
                    let previous=positions.get(&event.stream).copied().unwrap_or(0);
                    if event.stream_seq<=previous {continue;}
                    if event.stream_seq!=previous+1 {
                        // Catch up from the durable journal before releasing this new event.
                        let mut cursor=previous;
                        while cursor+1<event.stream_seq {
                            let rows=match state.events.replay(&event.stream,cursor,event.stream_seq-1,2048).await {Ok(rows)=>rows,Err(_)=>return};
                            if rows.is_empty(){return;}
                            for row in rows {
                                if row.stream_seq!=cursor+1{return;}
                                cursor=row.stream_seq;
                                if send_server(&mut socket,v2::ServerMessage{message:Some(v2::server_message::Message::Event(Box::new(row)))}).await.is_err(){return;}
                            }
                        }
                    }
                    positions.insert(event.stream.clone(),event.stream_seq);
                    if send_server(&mut socket,v2::ServerMessage{message:Some(v2::server_message::Message::Event(Box::new(event)))}).await.is_err(){break;}
                }
                Err(broadcast::error::RecvError::Lagged(_))=>{
                    state.events.metrics.slow_clients.fetch_add(1,std::sync::atomic::Ordering::Relaxed);
                    // A slow subscriber reconnects using its last contiguous sequence.
                    let _=send_server(&mut socket,server_error("REPLAY_REQUIRED",true,"正在恢复事件流")).await;
                    break;
                }
                Err(broadcast::error::RecvError::Closed)=>break,
            },
            _=heartbeat.tick()=>{
                if !state.auth.validate_socket(&principal,&headers).await{break;}
                let message=v2::Heartbeat{server_time_ns:now_ns(),last_stream_seq:positions.clone()};
                if send_server(&mut socket,v2::ServerMessage{message:Some(v2::server_message::Message::Heartbeat(message))}).await.is_err(){break;}
            }
        }
    }
    let _ = socket.close().await;
}

async fn deliver_initial(
    socket: &mut WebSocket,
    hub: &EventHub,
    request: &v2::Subscribe,
    resume: &HashMap<String, u64>,
) -> Result<HashMap<String, u64>> {
    // The receiver is subscribed before this consistent journal transaction.
    // All per-stream watermarks are retained; unrelated stream counters are never combined.
    let context = request.context.clone().unwrap_or_default();
    let (watermarks, events) = hub
        .snapshot(request.topics.clone(), context)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    let positions = watermarks
        .iter()
        .map(|(stream, last)| v2::ResumePosition {
            stream: stream.clone(),
            last_stream_seq: *last,
        })
        .collect::<Vec<_>>();
    let replayable = !watermarks.is_empty()
        && watermarks
            .iter()
            .all(|(stream, last)| resume.get(stream).is_some_and(|n| n <= last));
    if replayable {
        for (stream, last) in &watermarks {
            let mut after = resume[stream];
            while after < *last {
                let rows = hub
                    .replay(stream, after, *last, 2048)
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
                if rows.is_empty() {
                    anyhow::bail!("retained replay segment missing");
                }
                for event in rows {
                    hub.metrics
                        .replayed
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if event.stream_seq != after + 1 {
                        anyhow::bail!("retained replay segment has a gap");
                    }
                    after = event.stream_seq;
                    send_server(
                        socket,
                        v2::ServerMessage {
                            message: Some(v2::server_message::Message::Event(Box::new(event))),
                        },
                    )
                    .await?;
                }
            }
        }
    } else {
        hub.metrics
            .snapshots
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let batches = tokio::task::spawn_blocking(move || {
            montlok_contracts::columnar::snapshot_batches(&events)
        })
        .await??;
        let schema = montlok_contracts::columnar::encode_ipc(&[])?;
        send_server(
            socket,
            v2::ServerMessage {
                message: Some(v2::server_message::Message::SnapshotBegin(
                    v2::SnapshotBegin {
                        subscription_id: request.request_id.clone(),
                        snapshot_seq: 0,
                        arrow_schema: schema,
                        watermarks: positions.clone(),
                    },
                )),
            },
        )
        .await?;
        for (rows, bytes) in batches {
            send_server(
                socket,
                v2::ServerMessage {
                    message: Some(v2::server_message::Message::SnapshotBatch(
                        v2::SnapshotBatch {
                            subscription_id: request.request_id.clone(),
                            arrow_ipc: bytes,
                            row_count: rows as u32,
                        },
                    )),
                },
            )
            .await?;
        }
    }
    send_server(
        socket,
        v2::ServerMessage {
            message: Some(v2::server_message::Message::SnapshotEnd(v2::SnapshotEnd {
                subscription_id: request.request_id.clone(),
                snapshot_seq: 0,
                last_stream_seq: 0,
                watermarks: positions,
            })),
        },
    )
    .await?;
    Ok(watermarks)
}

fn server_error(code: &str, retryable: bool, detail: &str) -> v2::ServerMessage {
    v2::ServerMessage {
        message: Some(v2::server_message::Message::Error(v2::StreamError {
            code: code.into(),
            retryable,
            detail: detail.into(),
        })),
    }
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationContext {
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub strategy_group_id: String,
    #[serde(default)]
    pub run_id: String,
    #[serde(default)]
    pub instrument_id: String,
    #[serde(default)]
    pub model_release_id: String,
    #[serde(default)]
    pub signal_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PrepareOperationRequest {
    action: String,
    context: OperationContext,
    #[serde(default)]
    parameters: Value,
    #[serde(default)]
    client_request_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PreparedOperation {
    owner: String,
    operation_id: Uuid,
    action: String,
    context: OperationContext,
    parameters: Value,
    summary: String,
    expires_at: String,
    confirmation_hash: String,
    backend_request: Value,
}

#[derive(Debug, Deserialize)]
struct ExecuteOperationRequest {
    confirmation_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OperationReceipt {
    operation_id: Uuid,
    action: String,
    status: String,
    context: OperationContext,
    updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

struct OperationStore {
    db: Mutex<Connection>,
}

impl OperationStore {
    fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Connection::open(path)?;
        Self::initialize(db)
    }

    fn memory() -> Result<Self> {
        Self::initialize(Connection::open_in_memory()?)
    }

    fn initialize(db: Connection) -> Result<Self> {
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.pragma_update(None, "synchronous", "FULL")?;
        db.execute_batch("CREATE TABLE IF NOT EXISTS operations (
            id TEXT PRIMARY KEY,
            prepared_json TEXT NOT NULL,
            confirmation_hash TEXT NOT NULL,
            expires_at_ns INTEGER NOT NULL,
            status TEXT NOT NULL,
            result_json TEXT,
            updated_at_ns INTEGER NOT NULL
        );
        UPDATE operations SET status='UNKNOWN_BUT_QUERYABLE' WHERE status='PROCESSING';
        CREATE TABLE IF NOT EXISTS workspaces(owner TEXT NOT NULL,id TEXT NOT NULL,workspace_json TEXT NOT NULL,PRIMARY KEY(owner,id));")?;
        Ok(Self { db: Mutex::new(db) })
    }

    fn insert(&self, prepared: &PreparedOperation, expires_at_ns: i64) -> Result<(), ApiError> {
        let serialized =
            serde_json::to_string(prepared).map_err(|error| ApiError::Internal(error.into()))?;
        self.db.lock().expect("operation database mutex").execute(
            "INSERT INTO operations(id, prepared_json, confirmation_hash, expires_at_ns, status, updated_at_ns) VALUES(?1, ?2, ?3, ?4, 'PREPARED', ?5)",
            params![prepared.operation_id.to_string(), serialized, prepared.confirmation_hash, expires_at_ns, now_ns()]
        ).map_err(|error| ApiError::Conflict(error.to_string()))?;
        Ok(())
    }

    fn load(&self, id: Uuid) -> Result<(PreparedOperation, OperationReceipt, i64), ApiError> {
        self.db.lock().expect("operation database mutex").query_row(
            "SELECT prepared_json, status, result_json, expires_at_ns, updated_at_ns FROM operations WHERE id=?1",
            [id.to_string()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, Option<String>>(2)?, row.get::<_, i64>(3)?, row.get::<_, i64>(4)?))
        ).optional().map_err(|error| ApiError::Internal(error.into()))?
            .ok_or_else(|| ApiError::NotFound("操作回执不存在".into()))
            .and_then(|(prepared_json, status, result_json, expires_at, updated_at)| {
                let prepared: PreparedOperation = serde_json::from_str(&prepared_json).map_err(|error| ApiError::Internal(error.into()))?;
                let result = result_json.as_deref().map(serde_json::from_str).transpose().map_err(|error| ApiError::Internal(error.into()))?;
                let receipt = OperationReceipt {
                    operation_id: id, action: prepared.action.clone(), status,
                    context: prepared.context.clone(), updated_at: ns_to_rfc3339(updated_at),
                    result, reason: None,
                };
                Ok((prepared, receipt, expires_at))
            })
    }

    fn begin(
        &self,
        id: Uuid,
        confirmation_hash: &str,
    ) -> Result<(PreparedOperation, Option<OperationReceipt>), ApiError> {
        let (prepared, receipt, expires_at) = self.load(id)?;
        if prepared.confirmation_hash != confirmation_hash {
            return Err(ApiError::Conflict("确认内容已变化，请重新检查".into()));
        }
        if !matches!(receipt.status.as_str(), "PREPARED" | "PROCESSING") {
            return Ok((prepared, Some(receipt)));
        }
        if receipt.status == "PROCESSING" {
            return Ok((prepared, Some(receipt)));
        }
        if now_ns() > expires_at {
            self.finish(id, "EXPIRED", None)?;
            return Err(ApiError::Conflict("确认已过期，请重新检查".into()));
        }
        let changed = self.db.lock().expect("operation database mutex").execute(
            "UPDATE operations SET status='PROCESSING', updated_at_ns=?2 WHERE id=?1 AND status='PREPARED'",
            params![id.to_string(), now_ns()]
        ).map_err(|error| ApiError::Internal(error.into()))?;
        if changed != 1 {
            return Ok((prepared, Some(self.load(id)?.1)));
        }
        Ok((prepared, None))
    }

    fn finish(&self, id: Uuid, status: &str, result: Option<&Value>) -> Result<(), ApiError> {
        let result = result
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| ApiError::Internal(error.into()))?;
        self.db
            .lock()
            .expect("operation database mutex")
            .execute(
                "UPDATE operations SET status=?2, result_json=?3, updated_at_ns=?4 WHERE id=?1",
                params![id.to_string(), status, result, now_ns()],
            )
            .map_err(|error| ApiError::Internal(error.into()))?;
        Ok(())
    }
}

#[derive(Clone)]
struct ControlBackend {
    socket: Option<PathBuf>,
}

impl ControlBackend {
    async fn prepare(
        &self,
        request: &PrepareOperationRequest,
    ) -> Result<(Value, String), ApiError> {
        if request.context.account_id.is_empty() || request.context.strategy_group_id.is_empty() {
            return Err(ApiError::BadRequest("请指定执行账户与策略组".into()));
        }
        let action = legacy_action(&request.action)?;
        let mut legacy = request
            .parameters
            .clone()
            .as_object()
            .cloned()
            .unwrap_or_default();
        legacy.insert("action".into(), Value::String(action.into()));
        if !request.context.strategy_group_id.is_empty() {
            legacy.insert(
                "groupId".into(),
                Value::String(request.context.strategy_group_id.clone()),
            );
        }
        if !request.context.run_id.is_empty() {
            legacy.insert(
                "runId".into(),
                Value::String(request.context.run_id.clone()),
            );
        }
        let mut backend_request = Value::Object(legacy);
        let summary = if let Some(socket) = &self.socket {
            let status = socket_request(
                socket,
                json!({"command":"status","groupId":request.context.strategy_group_id}),
            )
            .await?;
            let groups = status["result"]["groups"]
                .as_array()
                .ok_or_else(|| ApiError::Unavailable("运行目录响应缺少策略组".into()))?;
            let group = groups
                .iter()
                .find(|group| {
                    group["groupId"].as_str() == Some(request.context.strategy_group_id.as_str())
                })
                .ok_or_else(|| ApiError::NotFound("策略组不存在".into()))?;
            if group["profileId"].as_str() != Some(request.context.account_id.as_str()) {
                return Err(ApiError::Conflict(
                    "策略组执行账户已变化，请重新选择".into(),
                ));
            }
            let response = socket_request(
                socket,
                json!({"command": "prepare", "request": backend_request}),
            )
            .await?;
            backend_request = response["result"]["request"].clone();
            if !backend_request.is_object() {
                return Err(ApiError::Unavailable("运行服务未提供确认请求".into()));
            }
            response
                .get("result")
                .and_then(|result| result.get("effect"))
                .and_then(Value::as_str)
                .unwrap_or("已由运行服务检查操作范围")
                .to_string()
        } else {
            return Err(ApiError::Unavailable("运行控制服务尚未连接".into()));
        };
        Ok((backend_request, summary))
    }

    async fn execute(&self, operation_id: Uuid, request: Value) -> Result<Value, ApiError> {
        let socket = self.socket.as_ref().ok_or_else(|| {
            ApiError::Unavailable("运行控制 socket 未配置；现有 Web 控制保持可用".into())
        })?;
        socket_request(socket, json!({"command": "execute", "operationId": operation_id.simple().to_string(), "request": request})).await
    }
    async fn receipt(&self, operation_id: Uuid) -> Result<Value, ApiError> {
        let socket = self
            .socket
            .as_ref()
            .ok_or_else(|| ApiError::Unavailable("运行控制服务尚未连接".into()))?;
        socket_request(
            socket,
            json!({"command":"receipt","operationId":operation_id.simple().to_string()}),
        )
        .await
    }
}

fn legacy_action(action: &str) -> Result<&'static str, ApiError> {
    match action {
        "start_strategy" => Ok("start"),
        "pause_opening" => Ok("halt"),
        "reduce_only" => Ok("reduce"),
        "resume" => Ok("resume"),
        "stop" => Ok("stop"),
        "cancel_and_pause" => Ok("cancel"),
        "flatten" => Ok("flatten"),
        _ => Err(ApiError::BadRequest("该操作尚未注册到 v2 控制协议".into())),
    }
}

async fn socket_request(path: &Path, request: Value) -> Result<Value, ApiError> {
    let stream = time::timeout(Duration::from_secs(5), UnixStream::connect(path))
        .await
        .map_err(|_| ApiError::Unavailable("运行服务连接超时".into()))?
        .map_err(|_| ApiError::Unavailable("运行服务未连接".into()))?;
    let (reader, mut writer) = stream.into_split();
    let encoded = serde_json::to_vec(&request).map_err(|error| ApiError::Internal(error.into()))?;
    if encoded.len() > 1_048_576 {
        return Err(ApiError::BadRequest("操作请求过大".into()));
    }
    writer
        .write_all(&encoded)
        .await
        .map_err(|error| ApiError::Internal(error.into()))?;
    writer
        .write_all(b"\n")
        .await
        .map_err(|error| ApiError::Internal(error.into()))?;
    writer
        .flush()
        .await
        .map_err(|error| ApiError::Internal(error.into()))?;
    let mut line = Vec::new();
    time::timeout(
        Duration::from_secs(60),
        BufReader::new(reader).read_until(b'\n', &mut line),
    )
    .await
    .map_err(|_| ApiError::Unavailable("操作结果待查询；不要重复提交".into()))?
    .map_err(|error| ApiError::Internal(error.into()))?;
    if line.len() > 1_048_576 || !line.ends_with(b"\n") {
        return Err(ApiError::Unavailable("运行服务响应不完整".into()));
    }
    let response: Value =
        serde_json::from_slice(&line).map_err(|error| ApiError::Internal(error.into()))?;
    if response.get("ok") != Some(&Value::Bool(true)) {
        return Err(ApiError::Conflict(
            response
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("运行服务拒绝操作")
                .into(),
        ));
    }
    Ok(response)
}

async fn prepare_operation(
    State(state): State<GatewayState>,
    Extension(principal): Extension<auth::Principal>,
    Json(request): Json<PrepareOperationRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let (backend_request, summary) = state.control.prepare(&request).await?;
    let operation_id = request.client_request_id.unwrap_or_else(Uuid::now_v7);
    let expires_at_ns = now_ns() + OPERATION_TTL_SECONDS * 1_000_000_000;
    let material = json!({
        "operation_id": operation_id, "action": request.action, "context": request.context,
        "parameters": request.parameters, "backend_request": backend_request, "expires_at_ns": expires_at_ns
    });
    let confirmation_hash = hex::encode(Sha256::digest(
        serde_json::to_vec(&material).map_err(|error| ApiError::Internal(error.into()))?,
    ));
    let prepared = PreparedOperation {
        owner: principal.id,
        operation_id,
        action: material["action"].as_str().unwrap_or_default().into(),
        context: serde_json::from_value(material["context"].clone())
            .map_err(|error| ApiError::Internal(error.into()))?,
        parameters: material["parameters"].clone(),
        summary,
        expires_at: ns_to_rfc3339(expires_at_ns),
        confirmation_hash,
        backend_request: material["backend_request"].clone(),
    };
    state.operations.insert(&prepared, expires_at_ns)?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::to_value(prepared).map_err(|error| ApiError::Internal(error.into()))?),
    ))
}

async fn execute_operation(
    State(state): State<GatewayState>,
    Extension(principal): Extension<auth::Principal>,
    AxumPath(operation_id): AxumPath<Uuid>,
    Json(request): Json<ExecuteOperationRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    if state.operations.load(operation_id)?.0.owner != principal.id {
        return Err(ApiError::NotFound("操作回执不存在".into()));
    }
    let (prepared, existing) = state
        .operations
        .begin(operation_id, &request.confirmation_hash)?;
    if let Some(receipt) = existing {
        let status = if receipt.status == "PROCESSING" {
            StatusCode::ACCEPTED
        } else {
            StatusCode::OK
        };
        return Ok((
            status,
            Json(serde_json::to_value(receipt).map_err(|error| ApiError::Internal(error.into()))?),
        ));
    }
    let outcome = state
        .control
        .execute(operation_id, prepared.backend_request.clone())
        .await;
    let (status, result, reason): (&str, Option<Value>, Option<String>) = match outcome {
        Ok(response) => {
            let result = response.get("result").cloned().unwrap_or(response);
            let receipt_status = match result.get("receiptStatus").and_then(Value::as_str) {
                Some("unknown" | "processing") => "UNKNOWN_BUT_QUERYABLE",
                Some("failed") => "FAILED",
                _ if result.get("status").and_then(Value::as_str) == Some("failed") => "FAILED",
                _ => "SUCCEEDED",
            };
            (receipt_status, Some(result), None)
        }
        Err(ApiError::Unavailable(detail)) => ("UNKNOWN_BUT_QUERYABLE", None, Some(detail)),
        Err(error) => ("FAILED", None, Some(error.to_string())),
    };
    state
        .operations
        .finish(operation_id, status, result.as_ref())?;
    let mut receipt = state.operations.load(operation_id)?.1;
    receipt.reason = reason;
    let http = if status == "UNKNOWN_BUT_QUERYABLE" {
        StatusCode::ACCEPTED
    } else {
        StatusCode::OK
    };
    Ok((
        http,
        Json(serde_json::to_value(receipt).map_err(|error| ApiError::Internal(error.into()))?),
    ))
}

async fn get_operation(
    State(state): State<GatewayState>,
    Extension(principal): Extension<auth::Principal>,
    AxumPath(operation_id): AxumPath<Uuid>,
) -> Result<Json<Value>, ApiError> {
    let (prepared, receipt, _) = state.operations.load(operation_id)?;
    if prepared.owner != principal.id {
        return Err(ApiError::NotFound("操作回执不存在".into()));
    }
    if matches!(
        receipt.status.as_str(),
        "PROCESSING" | "UNKNOWN_BUT_QUERYABLE"
    ) && let Ok(response) = state.control.receipt(operation_id).await
    {
        let result = response.get("result").cloned().unwrap_or(Value::Null);
        match result["receiptStatus"].as_str() {
            Some("completed") => {
                state
                    .operations
                    .finish(operation_id, "SUCCEEDED", Some(&result))?
            }
            Some("failed") => state
                .operations
                .finish(operation_id, "FAILED", Some(&result))?,
            _ => {}
        }
    }
    Ok(Json(
        serde_json::to_value(state.operations.load(operation_id)?.1)
            .map_err(|error| ApiError::Internal(error.into()))?,
    ))
}

fn ns_to_rfc3339(ns: i64) -> String {
    let seconds = ns.div_euclid(1_000_000_000);
    let nanos = ns.rem_euclid(1_000_000_000) as u32;
    chrono::DateTime::from_timestamp(seconds, nanos)
        .unwrap_or_default()
        .to_rfc3339()
}

fn built_in_workspaces() -> Result<Vec<Value>> {
    [
        include_str!("../../../packages/workspaces/live-operations.json"),
        include_str!("../../../packages/workspaces/execution-investigation.json"),
        include_str!("../../../packages/workspaces/research-release.json"),
        include_str!("../../../packages/workspaces/portfolio-risk.json"),
        include_str!("../../../packages/workspaces/market-data.json"),
        include_str!("../../../packages/workspaces/operations-security.json"),
    ]
    .into_iter()
    .map(|source| serde_json::from_str(source).context("parse built-in workspace"))
    .collect()
}

fn built_in_fields() -> Result<Value> {
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(include_str!("../../../packages/contracts/fields/core.yaml"))?;
    let mut fields = serde_json::to_value(yaml)?
        .get("fields")
        .cloned()
        .unwrap_or_else(|| Value::Array(Vec::new()));
    let extra: Value = serde_json::from_str(include_str!(
        "../../../packages/contracts/fields/terminal.json"
    ))?;
    fields.as_array_mut().expect("field array").extend(
        extra["fields"]
            .as_array()
            .context("terminal field array")?
            .iter()
            .cloned(),
    );
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(stream: &str, sequence: u64, id: &str) -> v2::EventEnvelope {
        v2::EventEnvelope {
            schema_version: 2,
            stream: stream.into(),
            stream_seq: sequence,
            snapshot_seq: 0,
            event_id: id.into(),
            event_type: v2::EventType::MarketQuote as i32,
            occurred_at_ns: now_ns(),
            received_at_ns: now_ns(),
            account_id: String::new(),
            strategy_group_id: String::new(),
            run_id: String::new(),
            instrument_id: "BTC-USDT".into(),
            correlation_id: String::new(),
            causation_id: String::new(),
            source: "test".into(),
            payload: None,
            source_payload_json: Vec::new(),
        }
    }

    #[tokio::test]
    async fn stream_sequence_is_contiguous_and_duplicates_are_idempotent() {
        let hub = EventHub::new();
        let original = event("quote", 1, "a");
        assert_eq!(hub.publish(original.clone()).await.unwrap(), 1);
        assert_eq!(hub.publish(original).await.unwrap(), 1);
        assert!(matches!(
            hub.publish(event("quote", 3, "b")).await,
            Err(ApiError::Conflict(_))
        ));
        assert_eq!(hub.publish(event("quote", 2, "b")).await.unwrap(), 2);
        assert_eq!(hub.events_after("quote", 1, 100).await.unwrap().len(), 1);
    }

    #[test]
    fn operation_store_claims_execute_once() {
        let store = OperationStore::memory().unwrap();
        let id = Uuid::now_v7();
        let prepared = PreparedOperation {
            owner: "operator".into(),
            operation_id: id,
            action: "pause_opening".into(),
            context: OperationContext {
                account_id: "a".into(),
                strategy_group_id: "g".into(),
                run_id: "r".into(),
                instrument_id: String::new(),
                model_release_id: String::new(),
                signal_version: String::new(),
            },
            parameters: json!({}),
            summary: "pause".into(),
            expires_at: ns_to_rfc3339(now_ns() + 1_000_000_000),
            confirmation_hash: "hash".into(),
            backend_request: json!({"action":"halt"}),
        };
        store.insert(&prepared, now_ns() + 1_000_000_000).unwrap();
        assert!(store.begin(id, "hash").unwrap().1.is_none());
        assert_eq!(
            store.begin(id, "hash").unwrap().1.unwrap().status,
            "PROCESSING"
        );
    }

    #[test]
    fn all_built_in_workspaces_parse() {
        let workspaces = built_in_workspaces().unwrap();
        assert_eq!(workspaces.len(), 6);
        assert!(
            workspaces
                .iter()
                .all(|workspace| workspace["schemaVersion"] == 2)
        );
    }
}
