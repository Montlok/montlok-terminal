"""The standalone PyO3 build must retain the Cython DataFFI ABI contract."""
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def test_standalone_extension_forwards_cython_compatibility():
    pyo3 = tomllib.loads((ROOT / "crates/pyo3/Cargo.toml").read_text())
    features = pyo3["features"]["cython-compat"]
    assert "nautilus-model/cython-compat" in features
    assert "ffi" in features


def test_model_cython_compatibility_requires_ffi_layout():
    model = tomllib.loads((ROOT / "crates/model/Cargo.toml").read_text())
    assert "ffi" in model["features"]["cython-compat"]
