//! Observe supervisor status and manage only our own read-side adapter children.
use anyhow::{Context, Result, bail};
use clap::Parser;
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    process::{Child, Command},
    time,
};

#[derive(Parser)]
struct Args {
    #[arg(long, env = "MONTLOK_CONTROL_SOCKET")]
    socket: PathBuf,
    #[arg(long, env = "MONTLOK_RUN_ROOT")]
    run_root: PathBuf,
    #[arg(long, env = "MONTLOK_ADAPTER_ROOT")]
    state_root: PathBuf,
    #[arg(long, env = "MONTLOK_ADAPTER_BINARY")]
    adapter: PathBuf,
    #[arg(
        long,
        env = "MONTLOK_NATS_URL",
        default_value = "nats://127.0.0.1:4222"
    )]
    nats_url: String,
}
#[derive(Clone)]
struct Source {
    run: String,
    group: String,
    account: String,
    path: PathBuf,
    active: bool,
}
fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|v| v.is_ascii_alphanumeric() || matches!(v, b'-' | b'_'))
}
fn sources(value: &Value, root: &Path) -> Vec<Source> {
    let mut found = Vec::new();
    for group in value["result"]["groups"].as_array().into_iter().flatten() {
        if group["mode"] != "live" {
            continue;
        }
        let Some(group_id) = group["groupId"].as_str().filter(|v| identifier(v)) else {
            continue;
        };
        for run in group["runs"].as_array().into_iter().flatten() {
            let Some(id) = run["runId"].as_str().filter(|v| identifier(v)) else {
                continue;
            };
            let expected = root.join(id);
            if run["runDir"].as_str() != expected.to_str() {
                continue;
            }
            let path = expected.join("execution-events.jsonl");
            if path.canonicalize().ok().as_ref() != Some(&path) {
                continue;
            }
            let manifest_path = expected.join("manifest.json");
            if manifest_path.canonicalize().ok().as_ref() != Some(&manifest_path)
                || manifest_path
                    .metadata()
                    .map_or(true, |m| m.len() > 1_048_576)
            {
                continue;
            }
            let Some(manifest) = std::fs::read(&manifest_path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            else {
                continue;
            };
            if manifest["mode"] != "live"
                || manifest["run_id"] != id
                || manifest["group_id"] != group_id
            {
                continue;
            }
            let Some(account) = manifest["account_id"]
                .as_str()
                .filter(|v| !v.is_empty() && v.len() <= 128)
            else {
                continue;
            };
            let active = matches!(
                run["status"].as_str(),
                Some("starting" | "running" | "halted" | "reducing" | "recovering" | "stopping")
            );
            found.push(Source {
                run: id.into(),
                group: group_id.into(),
                account: account.into(),
                path,
                active,
            });
        }
    }
    found.sort_by_key(|source| !source.active);
    found
}
async fn status(socket: &Path) -> Result<Value> {
    let connection = time::timeout(Duration::from_secs(5), UnixStream::connect(socket)).await??;
    let (read, mut write) = connection.into_split();
    write.write_all(b"{\"command\":\"status\"}\n").await?;
    let mut bytes = Vec::new();
    time::timeout(
        Duration::from_secs(5),
        BufReader::new(read).read_until(b'\n', &mut bytes),
    )
    .await??;
    if bytes.len() > 8 * 1024 * 1024 {
        bail!("supervisor status exceeds observer budget");
    }
    let value: Value = serde_json::from_slice(&bytes)?;
    if value["ok"] != true {
        bail!("supervisor status is unavailable");
    }
    Ok(value)
}
fn caught_up(database: &Path, source: &Path) -> bool {
    let Ok(size) = source.metadata().map(|m| m.len()) else {
        return false;
    };
    let Ok(db) = Connection::open_with_flags(
        database,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) else {
        return false;
    };
    let _ = db.busy_timeout(Duration::from_millis(100));
    let offset = db
        .query_row(
            "SELECT offset FROM source_offsets WHERE source=?1",
            [source.to_string_lossy().as_ref()],
            |r| r.get::<_, u64>(0),
        )
        .optional();
    let pending = db.query_row(
        "SELECT COUNT(*) FROM outbox WHERE source=?1",
        params![source.to_string_lossy().as_ref()],
        |r| r.get::<_, u64>(0),
    );
    matches!((offset,pending),(Ok(Some(offset)),Ok(0)) if offset==size)
}
fn report(event: &str, run: &str) {
    println!(
        "{}",
        json!({"component":"run_watcher","event":event,"run_id":run})
    );
}
#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::parse();
    args.run_root = args.run_root.canonicalize()?;
    args.adapter = args.adapter.canonicalize()?;
    std::fs::create_dir_all(&args.state_root)?;
    args.state_root = args.state_root.canonicalize()?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(args.state_root.join("watcher.lock"))?;
    lock.try_lock()
        .context("one observer must own the adapter fleet")?;
    let mut children = HashMap::<String, Child>::new();
    let mut restart_after = HashMap::<String, time::Instant>::new();
    let mut tick = time::interval(Duration::from_secs(2));
    tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        tokio::select! {_=tick.tick()=>{},_=tokio::signal::ctrl_c()=>break,_=terminate.recv()=>break}
        let listing = match status(&args.socket).await {
            Ok(value) => value,
            Err(_) => {
                report("supervisor_temporarily_unavailable", "");
                continue;
            }
        };
        let sources = sources(&listing, &args.run_root);
        let known = sources
            .iter()
            .map(|source| source.run.clone())
            .collect::<HashSet<_>>();
        for source in &sources {
            let db = args.state_root.join(format!("{}.sqlite", source.run));
            if let Some(child) = children.get_mut(&source.run) {
                if child.try_wait()?.is_some() {
                    children.remove(&source.run);
                    restart_after.insert(
                        source.run.clone(),
                        time::Instant::now() + Duration::from_secs(10),
                    );
                } else if !source.active && caught_up(&db, &source.path) {
                    let mut child = children.remove(&source.run).unwrap();
                    child.kill().await?;
                    report("completed_run_indexed", &source.run);
                }
                continue;
            }
            if (!source.active && caught_up(&db, &source.path))
                || children.len() >= 32
                || restart_after
                    .get(&source.run)
                    .is_some_and(|at| *at > time::Instant::now())
            {
                continue;
            }
            let child = Command::new(&args.adapter)
                .args(["--event-log"])
                .arg(&source.path)
                .arg("--database")
                .arg(&db)
                .args([
                    "--nats-url",
                    &args.nats_url,
                    "--account-id",
                    &source.account,
                    "--group-id",
                    &source.group,
                    "--run-id",
                    &source.run,
                ])
                .stdin(Stdio::null())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .kill_on_drop(true)
                .spawn()?;
            children.insert(source.run.clone(), child);
            report("following_live_run", &source.run);
        }
        // A directory disappearing must not make us manipulate any trading PID.
        let removed = children
            .keys()
            .filter(|run| !known.contains(*run))
            .cloned()
            .collect::<Vec<_>>();
        for run in removed {
            if let Some(mut child) = children.remove(&run) {
                let _ = child.kill().await;
                report("run_directory_unavailable", &run);
            }
        }
    }
    for (_, mut child) in children {
        let _ = child.kill().await;
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_explicit_live_sources_inside_the_run_root_are_selected() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let run = root.join("run-1");
        std::fs::create_dir(&run).unwrap();
        std::fs::write(run.join("execution-events.jsonl"), b"").unwrap();
        std::fs::write(
            run.join("manifest.json"),
            serde_json::to_vec(
                &json!({"mode":"live","run_id":"run-1","group_id":"g","account_id":"a"}),
            )
            .unwrap(),
        )
        .unwrap();
        let group = json!({"mode":"live","groupId":"g","profileId":"a","runs":[{"runId":"run-1","status":"running","runDir":run.to_str().unwrap()}]});
        let data = json!({"result":{"groups":[group.clone()]}});
        assert_eq!(sources(&data, &root).len(), 1);
        let mut other = group.clone();
        other["mode"] = json!("shadow");
        assert!(sources(&json!({"result":{"groups":[other]}}), &root).is_empty());
        std::fs::write(
            run.join("manifest.json"),
            serde_json::to_vec(
                &json!({"mode":"shadow","run_id":"run-1","group_id":"g","account_id":"a"}),
            )
            .unwrap(),
        )
        .unwrap();
        assert!(sources(&json!({"result":{"groups":[group.clone()]}}), &root).is_empty());
        let mut other = group;
        other["runs"][0]["runDir"] = json!("/outside/run-1");
        assert!(sources(&json!({"result":{"groups":[other]}}), &root).is_empty());
    }
}
