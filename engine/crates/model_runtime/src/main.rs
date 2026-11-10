use nautilus_model_runtime::{MAX_FRAME_BYTES, ModelGuard};
use serde_json::{Value, json};
use std::io::{self, BufRead, Write};
use std::time::{SystemTime, UNIX_EPOCH};

fn now() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    )
    .unwrap_or(u64::MAX)
}

fn main() -> io::Result<()> {
    let stdin = io::stdin();
    let mut reader = stdin.lock();
    let stdout = io::stdout();
    let mut out = stdout.lock();
    let mut guard: Option<ModelGuard> = None;
    loop {
        let mut line = Vec::new();
        let mut oversize = false;
        loop {
            let buf = reader.fill_buf()?;
            if buf.is_empty() {
                break;
            }
            let n = buf
                .iter()
                .position(|c| *c == b'\n')
                .map_or(buf.len(), |n| n + 1);
            let ended = buf[n - 1] == b'\n';
            if !oversize && line.len() + n <= MAX_FRAME_BYTES {
                line.extend_from_slice(&buf[..n]);
            } else {
                oversize = true;
            }
            reader.consume(n);
            if ended {
                break;
            }
        }
        if line.is_empty() && !oversize {
            break;
        }
        let result = (|| -> Result<Value, String> {
            if oversize {
                return Err("frame_too_large".into());
            }
            let v: Value = serde_json::from_slice(&line).map_err(|_| "invalid_json")?;
            match v["op"].as_str() {
                Some("init") => {
                    if guard.is_some() {
                        return Err("release_already_initialized".into());
                    }
                    guard = Some(ModelGuard::new(v["release"].clone())?);
                    Ok(json!({"ok":true,"runtime":"nautilus-model-guard","schemaVersion":1}))
                }
                Some("prepare") => guard
                    .as_mut()
                    .ok_or("release_not_initialized")?
                    .prepare(v, now()),
                Some("validate_prediction") => guard
                    .as_mut()
                    .ok_or("release_not_initialized")?
                    .validate_prediction(&v["prediction"], &v["request"], now()),
                Some("metrics") => Ok(
                    json!({"ok":true,"metrics":guard.as_ref().ok_or("release_not_initialized")?.metrics()}),
                ),
                _ => Err("unknown_operation".into()),
            }
        })();
        serde_json::to_writer(
            &mut out,
            &result.unwrap_or_else(|e| json!({"ok":false,"error":e})),
        )?;
        out.write_all(b"\n")?;
        out.flush()?;
    }
    Ok(())
}
