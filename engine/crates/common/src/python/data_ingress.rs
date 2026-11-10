//! Explicit, data-only native callback ingress owned by one Python event-loop node.
//!
//! This does not install TLS senders or impersonate a Rust LiveNode. The actual
//! Python callback queue is bounded, with one outstanding loop wake-up. A fatal
//! callback, adapter, queue or loop error latches this owner and rejects new data.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, OnceLock, Weak},
    thread::{self, ThreadId},
};

use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
    types::PyDict,
};

type Callback = (Py<PyAny>, Py<PyAny>);

#[derive(Default, Debug)]
struct State {
    queue: VecDeque<Callback>,
    scheduled: bool,
    closed: bool,
    failure: Option<String>,
    notified: bool,
    high_water: usize,
    delivered: usize,
    rejected: usize,
}

#[derive(Debug)]
struct Inner {
    owner: String,
    thread: ThreadId,
    loop_: Py<PyAny>,
    on_failure: Py<PyAny>,
    owner_context: Mutex<Option<Py<PyAny>>>,
    capacity: usize,
    batch: usize,
    state: Mutex<State>,
}

fn owners() -> &'static Mutex<HashMap<String, Weak<Inner>>> {
    static OWNERS: OnceLock<Mutex<HashMap<String, Weak<Inner>>>> = OnceLock::new();
    OWNERS.get_or_init(|| Mutex::new(HashMap::new()))
}

impl Inner {
    fn failure(&self) -> Option<String> {
        self.state.lock().expect("ingress lock").failure.clone()
    }

    fn closed(&self) -> bool {
        self.state.lock().expect("ingress lock").closed
    }

    fn fail(self: &Arc<Self>, reason: String) {
        let (discarded, schedule) = {
            let mut state = self.state.lock().expect("ingress lock");
            if state.closed || state.failure.is_some() {
                return;
            }
            state.failure = Some(reason);
            let discarded = std::mem::take(&mut state.queue);
            let schedule = !state.scheduled;
            state.scheduled = true;
            (discarded, schedule)
        };
        drop(discarded);
        if schedule {
            Python::attach(|py| self.schedule(py));
        }
    }

    fn schedule(self: &Arc<Self>, py: Python<'_>) {
        let result = Py::new(
            py,
            Wake {
                inner: self.clone(),
            },
        )
        .and_then(|wake| {
            self.loop_
                .call_method1(py, "call_soon_threadsafe", (wake,))
                .map(|_| ())
        });
        if let Err(error) = result {
            // A closed loop cannot run a failure callback. Keep the failure
            // inspectable, clear retained data and never schedule another wake.
            let discarded = {
                let mut state = self.state.lock().expect("ingress lock");
                state
                    .failure
                    .get_or_insert_with(|| format!("event_loop_unavailable: {error}"));
                state.scheduled = false;
                std::mem::take(&mut state.queue)
            };
            drop(discarded);
        }
    }

    fn enqueue(
        self: &Arc<Self>,
        py: Python<'_>,
        callback: Py<PyAny>,
        value: Py<PyAny>,
    ) -> PyResult<()> {
        let schedule = {
            let mut state = self.state.lock().expect("ingress lock");
            if state.closed || state.failure.is_some() {
                state.rejected += 1;
                return Err(PyRuntimeError::new_err(
                    "Python data ingress is closed or failed",
                ));
            }
            if state.queue.len() >= self.capacity {
                state.rejected += 1;
                drop(state);
                self.fail("python_callback_queue_full".to_string());
                return Err(PyRuntimeError::new_err(
                    "Python data ingress callback queue full",
                ));
            }
            state.queue.push_back((callback, value));
            state.high_water = state.high_water.max(state.queue.len());
            let schedule = !state.scheduled;
            state.scheduled = true;
            schedule
        };
        if schedule {
            self.schedule(py);
        }
        match self.failure() {
            Some(reason) => Err(PyRuntimeError::new_err(reason)),
            None => Ok(()),
        }
    }

    fn drain(self: &Arc<Self>, py: Python<'_>) {
        if self.thread != thread::current().id() {
            self.state.lock().expect("ingress lock").scheduled = false;
            self.fail("callback_on_wrong_thread".to_string());
            return;
        }
        for _ in 0..self.batch {
            let (next, failure) = {
                let mut state = self.state.lock().expect("ingress lock");
                if state.closed {
                    state.scheduled = false;
                    return;
                }
                if let Some(reason) = state.failure.clone() {
                    let notify = !state.notified;
                    state.notified = true;
                    state.scheduled = false;
                    (None, notify.then_some(reason))
                } else {
                    let next = state.queue.pop_front();
                    if next.is_none() {
                        state.scheduled = false;
                    }
                    (next, None)
                }
            };
            if let Some(reason) = failure {
                if let Err(error) = self.on_failure.call1(py, (reason,)) {
                    log::error!(
                        "Python ingress owner {} failure callback raised: {error}",
                        self.owner
                    );
                }
                return;
            }
            let Some((callback, value)) = next else {
                return;
            };
            if let Err(error) = callback.call1(py, (value,)) {
                self.fail(format!("python_data_callback_failed: {error}"));
            } else {
                self.state.lock().expect("ingress lock").delivered += 1;
            }
        }
        // Keep the scheduled latch while moving the one wake-up to the tail of
        // the host loop. Python's data consumer gets a turn between batches.
        self.schedule(py);
    }

    fn close(&self) {
        let discarded = {
            let mut state = self.state.lock().expect("ingress lock");
            state.closed = true;
            state.scheduled = false;
            std::mem::take(&mut state.queue)
        };
        drop(discarded);
        let context = self
            .owner_context
            .lock()
            .expect("ingress context lock")
            .take();
        drop(context);
    }
}

#[pyclass(
    name = "PythonDataIngress",
    module = "nautilus_trader.core.nautilus_pyo3.common"
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.common")]
#[derive(Debug)]
pub struct PyDataIngress {
    inner: Arc<Inner>,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyDataIngress {
    #[new]
    #[pyo3(signature = (loop_, owner_id, on_failure, capacity=1024, max_batch=64, *, owner_context=None))]
    fn new(
        py: Python<'_>,
        loop_: Py<PyAny>,
        owner_id: String,
        on_failure: Py<PyAny>,
        capacity: usize,
        max_batch: usize,
        owner_context: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        if owner_id.is_empty()
            || owner_id.len() > 128
            || capacity == 0
            || capacity > 65_536
            || max_batch == 0
            || max_batch > capacity
        {
            return Err(PyValueError::new_err(
                "Invalid Python data ingress identity or bounds",
            ));
        }
        if loop_.call_method0(py, "is_closed")?.extract::<bool>(py)?
            || !on_failure.bind(py).is_callable()
        {
            return Err(PyValueError::new_err(
                "Ingress requires an open event loop and failure callback",
            ));
        }
        let mut registry = owners().lock().expect("ingress owner registry");
        registry.retain(|_, weak| weak.strong_count() != 0);
        if registry
            .get(&owner_id)
            .and_then(Weak::upgrade)
            .is_some_and(|owner| !owner.closed())
        {
            return Err(PyValueError::new_err(
                "Python data ingress owner is already bound",
            ));
        }
        let inner = Arc::new(Inner {
            owner: owner_id.clone(),
            thread: thread::current().id(),
            loop_,
            on_failure,
            owner_context: Mutex::new(owner_context),
            capacity,
            batch: max_batch,
            state: Mutex::new(State::default()),
        });
        registry.insert(owner_id, Arc::downgrade(&inner));
        Ok(Self { inner })
    }

    #[getter]
    fn owner_id(&self) -> String {
        self.inner.owner.clone()
    }

    #[getter]
    fn failure(&self) -> Option<String> {
        self.inner.failure()
    }

    #[getter]
    fn closed(&self) -> bool {
        self.inner.closed()
    }

    fn fail(&self, reason: String) {
        self.inner.fail(reason);
    }

    fn close(&self) {
        self.inner.close();
    }

    fn validate_context(&self, context: &Bound<'_, PyAny>) -> PyResult<()> {
        if self.inner.closed() || self.inner.failure().is_some() {
            return Err(PyRuntimeError::new_err(
                "Python data ingress is closed or failed",
            ));
        }
        let owner = self
            .inner
            .owner_context
            .lock()
            .expect("ingress context lock");
        if owner
            .as_ref()
            .is_none_or(|owner| owner.as_ptr() != context.as_ptr())
        {
            return Err(PyValueError::new_err(
                "Python data ingress belongs to another kernel cache",
            ));
        }
        Ok(())
    }

    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let state = self.inner.state.lock().expect("ingress lock");
        let result = PyDict::new(py);
        result.set_item("ownerId", &self.inner.owner)?;
        result.set_item("capacity", self.inner.capacity)?;
        result.set_item("pending", state.queue.len())?;
        result.set_item("highWater", state.high_water)?;
        result.set_item("delivered", state.delivered)?;
        result.set_item("rejected", state.rejected)?;
        result.set_item("wakePending", state.scheduled)?;
        result.set_item("closed", state.closed)?;
        result.set_item("failure", &state.failure)?;
        Ok(result)
    }
}

impl PyDataIngress {
    /// Validate explicit capability before any network connection takes place.
    pub fn scheduler(&self, py: Python<'_>, loop_: &Py<PyAny>, lane: &str) -> PyResult<Py<PyAny>> {
        if lane != "data" {
            return Err(PyValueError::new_err(
                "PythonDataIngress is data-only; execution lane is forbidden",
            ));
        }
        if self.inner.thread != thread::current().id()
            || self.inner.loop_.as_ptr() != loop_.as_ptr()
        {
            return Err(PyValueError::new_err(
                "Python data ingress belongs to another thread or event loop",
            ));
        }
        if self.inner.closed() || self.inner.failure().is_some() {
            return Err(PyRuntimeError::new_err(
                "Python data ingress is closed or failed",
            ));
        }
        if loop_.call_method0(py, "is_closed")?.extract::<bool>(py)? {
            self.inner
                .fail("event_loop_closed_before_connect".to_string());
            return Err(PyRuntimeError::new_err(
                "Python data ingress event loop is closed",
            ));
        }
        Py::new(
            py,
            Scheduler {
                inner: self.inner.clone(),
            },
        )
        .map(Py::into_any)
    }

    /// First adapter failure latches the same owner as the actual data queue.
    pub fn failure_callback(&self) -> Arc<dyn Fn(String) + Send + Sync> {
        let inner = self.inner.clone();
        Arc::new(move |reason| inner.fail(reason))
    }
}

#[pyclass]
struct Scheduler {
    inner: Arc<Inner>,
}

#[pymethods]
impl Scheduler {
    fn __call__(&self, py: Python<'_>, callback: Py<PyAny>, value: Py<PyAny>) -> PyResult<()> {
        self.inner.enqueue(py, callback, value)
    }
}

#[pyclass]
struct Wake {
    inner: Arc<Inner>,
}

#[pymethods]
impl Wake {
    fn __call__(&self, py: Python<'_>) {
        self.inner.drain(py);
    }
}

/// Conversion happens before queueing; bridge conversion failures must not be log-only.
pub fn fail_python_data_ingress(py: Python<'_>, scheduler: &Py<PyAny>, reason: String) {
    if let Ok(scheduler) = scheduler.bind(py).extract::<PyRef<'_, Scheduler>>() {
        scheduler.inner.fail(reason);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::types::{PyList, PyModule};

    fn fixture(
        py: Python<'_>,
        name: &str,
        capacity: usize,
        batch: usize,
    ) -> (PyDataIngress, Py<PyAny>, Py<PyAny>, Py<PyList>, Py<PyList>) {
        let module = PyModule::from_code(py, c"class Loop:\n def __init__(self):\n  self.closed=False\n  self.wakes=[]\n def is_closed(self): return self.closed\n def call_soon_threadsafe(self, fn):\n  if self.closed: raise RuntimeError('Event loop is closed')\n  self.wakes.append(fn)\n def step(self): self.wakes.pop(0)()\ndef explode(value): raise ValueError('conversion failed')\n", c"data_ingress_test.py", c"data_ingress_test").unwrap();
        let loop_ = module.getattr("Loop").unwrap().call0().unwrap().unbind();
        let values = PyList::empty(py).unbind();
        let failures = PyList::empty(py).unbind();
        let callback = values.getattr(py, "append").unwrap();
        let failed = failures.getattr(py, "append").unwrap();
        let bridge = PyDataIngress::new(
            py,
            loop_.clone_ref(py),
            name.to_string(),
            failed,
            capacity,
            batch,
            None,
        )
        .unwrap();
        (bridge, loop_, callback, values, failures)
    }

    fn pending_wakes(py: Python<'_>, loop_: &Py<PyAny>) -> usize {
        loop_.getattr(py, "wakes").unwrap().bind(py).len().unwrap()
    }

    #[test]
    fn blocked_consumer_is_bounded_and_overflow_notifies_once() {
        Python::initialize();
        Python::attach(|py| {
            let (bridge, loop_, callback, values, failures) = fixture(py, "blocked-consumer", 2, 1);
            let scheduler = bridge.scheduler(py, &loop_, "data").unwrap();
            for number in 0..2 {
                scheduler
                    .call1(py, (callback.clone_ref(py), number))
                    .unwrap();
            }
            assert_eq!(bridge.inner.state.lock().unwrap().queue.len(), 2);
            assert_eq!(pending_wakes(py, &loop_), 1);
            for _ in 0..100 {
                assert!(scheduler.call1(py, (callback.clone_ref(py), 3)).is_err());
            }
            assert_eq!(bridge.inner.state.lock().unwrap().queue.len(), 0);
            assert_eq!(bridge.inner.state.lock().unwrap().high_water, 2);
            assert_eq!(pending_wakes(py, &loop_), 1);
            loop_.call_method0(py, "step").unwrap();
            assert_eq!(failures.bind(py).len(), 1);
            assert_eq!(values.bind(py).len(), 0);
            assert_eq!(
                bridge.failure().as_deref(),
                Some("python_callback_queue_full")
            );
            bridge.close();
        });
    }

    #[test]
    fn batched_delivery_keeps_one_wake_and_preserves_order() {
        Python::initialize();
        Python::attach(|py| {
            let (bridge, loop_, callback, values, failures) = fixture(py, "batch-order", 8, 1);
            let scheduler = bridge.scheduler(py, &loop_, "data").unwrap();
            for number in 0..3 {
                scheduler
                    .call1(py, (callback.clone_ref(py), number))
                    .unwrap();
            }
            while pending_wakes(py, &loop_) != 0 {
                assert_eq!(pending_wakes(py, &loop_), 1);
                loop_.call_method0(py, "step").unwrap();
            }
            assert_eq!(
                values.bind(py).extract::<Vec<i32>>().unwrap(),
                vec![0, 1, 2]
            );
            assert_eq!(failures.bind(py).len(), 0);
            assert_eq!(
                bridge
                    .stats(py)
                    .unwrap()
                    .get_item("pending")
                    .unwrap()
                    .unwrap()
                    .extract::<usize>()
                    .unwrap(),
                0
            );
            bridge.close();
        });
    }

    #[test]
    fn closed_loop_latches_inspectable_failure_and_clears_pending() {
        Python::initialize();
        Python::attach(|py| {
            let (bridge, loop_, callback, _, failures) = fixture(py, "loop-closed", 2, 1);
            let scheduler = bridge.scheduler(py, &loop_, "data").unwrap();
            loop_.setattr(py, "closed", true).unwrap();
            assert!(scheduler.call1(py, (callback, 1)).is_err());
            assert!(bridge.failure().unwrap().contains("event_loop_unavailable"));
            assert_eq!(bridge.inner.state.lock().unwrap().queue.len(), 0);
            assert_eq!(pending_wakes(py, &loop_), 0);
            assert_eq!(failures.bind(py).len(), 0); // Cannot invoke callbacks on a closed loop.
            assert!(bridge.scheduler(py, &loop_, "data").is_err());
            bridge.close();
        });
    }

    #[test]
    fn wrong_lane_loop_thread_and_duplicate_owner_rejected() {
        Python::initialize();
        Python::attach(|py| {
            let (bridge, loop_, callback, _, _) = fixture(py, "owner-scope", 2, 1);
            assert!(bridge.scheduler(py, &loop_, "execution").is_err());
            let (_, other_loop, _, _, _) = fixture(py, "other-loop", 2, 1);
            assert!(bridge.scheduler(py, &other_loop, "data").is_err());
            assert!(
                PyDataIngress::new(
                    py,
                    loop_.clone_ref(py),
                    "owner-scope".to_string(),
                    callback,
                    2,
                    1,
                    None
                )
                .is_err()
            );
            let inner = bridge.inner.clone();
            let thread_result = py.detach(move || {
                std::thread::spawn(move || {
                    Python::attach(|py| {
                        let cap = PyDataIngress { inner };
                        cap.scheduler(py, &cap.inner.loop_, "data").is_err()
                    })
                })
                .join()
                .unwrap()
            });
            assert!(thread_result);
            assert!(bridge.failure().is_none());
            bridge.close();
        });
    }

    #[test]
    fn normal_close_drops_callbacks_and_old_identity_cannot_poison_replacement() {
        Python::initialize();
        Python::attach(|py| {
            let (bridge, loop_, callback, values, failures) = fixture(py, "identity-reuse", 2, 1);
            let scheduler = bridge.scheduler(py, &loop_, "data").unwrap();
            let fail_old = bridge.failure_callback();
            scheduler.call1(py, (callback.clone_ref(py), 1)).unwrap();
            bridge.close();
            bridge.close();
            loop_.call_method0(py, "step").unwrap();
            assert!(scheduler.call1(py, (callback, 2)).is_err());
            assert_eq!(values.bind(py).len(), 0);
            assert_eq!(failures.bind(py).len(), 0);
            assert!(bridge.failure().is_none());
            let replacement = PyDataIngress::new(
                py,
                loop_,
                "identity-reuse".to_string(),
                failures.getattr(py, "append").unwrap(),
                2,
                1,
                None,
            )
            .unwrap();
            fail_old("late old adapter failure".to_string());
            assert!(replacement.failure().is_none());
            replacement.close();
        });
    }

    #[test]
    fn callback_and_conversion_errors_terminate_the_same_owner() {
        Python::initialize();
        Python::attach(|py| {
            let (bridge, loop_, _, _, failures) = fixture(py, "callback-error", 2, 1);
            let module = PyModule::from_code(
                py,
                c"def explode(value): raise ValueError('broken callback')\n",
                c"failure.py",
                c"failure",
            )
            .unwrap();
            let scheduler = bridge.scheduler(py, &loop_, "data").unwrap();
            scheduler
                .call1(py, (module.getattr("explode").unwrap(), 1))
                .unwrap();
            while pending_wakes(py, &loop_) != 0 {
                loop_.call_method0(py, "step").unwrap();
            }
            assert!(
                bridge
                    .failure()
                    .unwrap()
                    .contains("python_data_callback_failed")
            );
            assert_eq!(failures.bind(py).len(), 1);
            bridge.close();
            let (bridge, loop_, _, _, failures) = fixture(py, "conversion-error", 2, 1);
            let scheduler = bridge.scheduler(py, &loop_, "data").unwrap();
            fail_python_data_ingress(py, &scheduler, "test_conversion_failed".to_string());
            loop_.call_method0(py, "step").unwrap();
            assert_eq!(bridge.failure().as_deref(), Some("test_conversion_failed"));
            assert_eq!(failures.bind(py).len(), 1);
            bridge.close();
        });
    }
}
