// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Supervised task spawning for long-lived live components.
//!
//! Background tasks such as venue WebSocket consumers are the spine of a live node: when one
//! dies silently the node keeps running with stale state, which is the most expensive failure
//! mode in production. [`spawn_supervised`] wraps a task so that *every* way it can stop
//! (normal completion, a panic in unwind builds, or abort) is reported through a single exit
//! callback, letting the owner decide whether to halt, restart, or ignore.
//!
//! Release builds compile with `panic = "abort"`, where a panic terminates the process before
//! the wrapper can observe it; the logging panic hook covers post-mortem diagnostics there.

use std::{future::Future, panic::AssertUnwindSafe};

use futures::FutureExt;

use super::dst::task::{JoinHandle, spawn};

/// Describes how a supervised task stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskExit {
    /// The task future ran to completion.
    Completed,
    /// The task panicked; the payload is rendered as a message where possible.
    Panicked(String),
}

impl TaskExit {
    /// Returns whether the task stopped because of a panic.
    #[must_use]
    pub fn is_panic(&self) -> bool {
        matches!(self, Self::Panicked(_))
    }
}

impl std::fmt::Display for TaskExit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Completed => write!(f, "completed"),
            Self::Panicked(message) => write!(f, "panicked: {message}"),
        }
    }
}

/// Spawns `future` on the live runtime and invokes `on_exit` when it stops for any reason
/// other than abort.
///
/// Panics raised inside the task are caught (in unwind builds) so the exit callback always
/// runs; the callback is the single place to translate a dead task into a venue halt or a
/// restart. Aborting the returned handle drops the future without invoking the callback,
/// which keeps ordinary shutdown quiet.
pub fn spawn_supervised<F, H>(name: &str, future: F, on_exit: H) -> JoinHandle<()>
where
    F: Future<Output = ()> + Send + 'static,
    H: FnOnce(TaskExit) + Send + 'static,
{
    let name = name.to_string();
    spawn(async move {
        let exit = match AssertUnwindSafe(future).catch_unwind().await {
            Ok(()) => TaskExit::Completed,
            Err(payload) => {
                let message = panic_payload_message(payload.as_ref());
                log::error!("Supervised task '{name}' panicked: {message}");
                TaskExit::Panicked(message)
            }
        };
        on_exit(exit);
    })
}

fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use rstest::rstest;

    use super::*;
    use crate::live::dst::time;

    #[rstest]
    #[cfg_attr(not(all(feature = "simulation", madsim)), tokio::test)]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_completed_task_reports_completion() {
        let (tx, rx) = tokio::sync::oneshot::channel();

        spawn_supervised("completes", async {}, move |exit| {
            let _ = tx.send(exit);
        });

        let exit = time::timeout(Duration::from_secs(1), rx)
            .await
            .expect("exit callback should run")
            .expect("exit should be sent");
        assert_eq!(exit, TaskExit::Completed);
        assert!(!exit.is_panic());
    }

    #[rstest]
    #[cfg_attr(not(all(feature = "simulation", madsim)), tokio::test)]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_panicking_task_reports_panic_message() {
        let (tx, rx) = tokio::sync::oneshot::channel();

        spawn_supervised(
            "panics",
            async {
                panic!("boom {}", 7);
            },
            move |exit| {
                let _ = tx.send(exit);
            },
        );

        let exit = time::timeout(Duration::from_secs(1), rx)
            .await
            .expect("exit callback should run")
            .expect("exit should be sent");
        assert_eq!(exit, TaskExit::Panicked("boom 7".to_string()));
        assert!(exit.is_panic());
        assert_eq!(exit.to_string(), "panicked: boom 7");
    }

    #[rstest]
    #[cfg_attr(not(all(feature = "simulation", madsim)), tokio::test)]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_aborted_task_does_not_invoke_callback() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_clone = calls.clone();

        let handle = spawn_supervised("pending", std::future::pending(), move |_| {
            calls_clone.fetch_add(1, Ordering::SeqCst);
        });
        handle.abort();

        time::sleep(Duration::from_millis(20)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
