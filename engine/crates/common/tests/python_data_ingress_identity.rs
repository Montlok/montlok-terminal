//! Real PyO3 capability identity tests without changing production source hashes.

#[cfg(feature = "python")]
mod tests {
    use nautilus_common::python::data_ingress::PyDataIngress;
    use pyo3::{prelude::*, types::{PyDict, PyList, PyModule}};

    #[test]
    fn same_loop_foreign_cache_is_rejected_and_close_releases_context() {
        Python::initialize();
        Python::attach(|py| {
            let module = PyModule::from_code(py, c"class Loop:\n def is_closed(self): return False\n def call_soon_threadsafe(self, fn): pass\n", c"identity_test.py", c"identity_test").unwrap();
            let loop_ = module.getattr("Loop").unwrap().call0().unwrap();
            let failures = PyList::empty(py);
            let cache_a = PyDict::new(py);
            let cache_b = PyDict::new(py);
            let kwargs = PyDict::new(py);
            kwargs.set_item("owner_context", &cache_a).unwrap();
            let sys = py.import("sys").unwrap();
            let before: usize = sys.call_method1("getrefcount", (&cache_a,)).unwrap().extract().unwrap();
            let bridge = py.get_type::<PyDataIngress>().call(
                (loop_, "real-kernel-identity", failures.getattr("append").unwrap(), 8, 2), Some(&kwargs),
            ).unwrap();
            bridge.call_method1("validate_context", (&cache_a,)).unwrap();
            assert!(bridge.call_method1("validate_context", (&cache_b,)).is_err());
            let retained: usize = sys.call_method1("getrefcount", (&cache_a,)).unwrap().extract().unwrap();
            assert_eq!(retained, before + 1);
            bridge.call_method0("close").unwrap();
            let released: usize = sys.call_method1("getrefcount", (&cache_a,)).unwrap().extract().unwrap();
            assert_eq!(released, before);
            assert!(bridge.call_method1("validate_context", (&cache_a,)).is_err());
            assert!(bridge.getattr("failure").unwrap().is_none());
            assert_eq!(failures.len(), 0);
        });
    }
}
