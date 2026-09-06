"""Original RDT source feature parity; no GPU, training, network or orders."""
import copy
import json
from pathlib import Path
import unittest

from model_market_actor import MARKETS, rdt_feature_module, rdt_window_from_markets
from model_release import ModelAdapter, rdt_source_files, sha

ROOT = Path(__file__).resolve().parents[3] / "model_training"


def fixture_manifest():
    crypto = json.loads((ROOT / "runs/rdt4quantv1_20260905/prepared/manifest.json").read_text())
    multi = json.loads((ROOT / "runs/rdt4quantv1_multimarket_20260905/prepared/manifest.json").read_text())
    token = next(a for a in multi["domains"]["token_hour"]["assets"] if a["asset"] == "XNVDA")
    return {"runnerId": "rdt4quant_v1", "family": "rdt4quant_multiasset", "sources": [
        {"path": "source/" + n, "sha256": sha(ROOT / "pipelines" / n)} for n in
        ("rdt4quant/prepare.py", "rdt4quant/download.py", "rdt4quant_fullpass/prepare.py")],
        "domainContracts": {
            "crypto:BTC-USDT": {"names": crypto["features"], "mean": crypto["mean"], "scale": crypto["std"],
                "sequenceBars": 480, "barSeconds": 60, "domain": "crypto", "instrument": "BTC-USDT", "assetId": 0},
            "token_hour:XNVDA": {"names": token["features"], "mean": token["feature_mean"], "scale": token["feature_scale"],
                "sequenceBars": 64, "barSeconds": 3600, "domain": "token_hour", "instrument": "XNVDA", "assetId": token["asset_id"]}}}


class RdtFeatureTests(unittest.TestCase):
    def test_reviewed_rdt_sources_fit_published_archive_capacity(self):
        files = rdt_source_files(ROOT / "pipelines")
        self.assertLess(len(files) + 2, 256)
        self.assertTrue(any(p.name == "__init__.py" for p in files))
        self.assertFalse(any("tests" in p.relative_to(ROOT / "pipelines").parts for p in files))

    @classmethod
    def setUpClass(cls):
        import pandas as pd
        cls.manifest = fixture_manifest()
        cls.crypto = {}
        for name in MARKETS:
            frame = pd.read_csv(ROOT / f"data/rdt4quantv1_20260905/{name}-1m.csv").iloc[-1000:].copy()
            frame["timestamp"] = pd.to_datetime(frame["ts_ms"], unit="ms", utc=True)
            cls.crypto[name] = frame.set_index("timestamp")
        token = pd.read_csv(ROOT / "data/stock_alpha/okx-XNVDA-USDT-1H.csv").iloc[-150:].copy()
        token["timestamp"] = pd.to_datetime(token["timestamp"], utc=True)
        cls.token = {"XNVDA-USDT": token.set_index("timestamp")}

    def test_crypto_original_57_feature_480_sequence_exact(self):
        import numpy as np
        module = rdt_feature_module(self.manifest, ROOT / "pipelines", "crypto:BTC-USDT")
        window = rdt_window_from_markets(self.manifest, self.crypto, module, "crypto:BTC-USDT")
        expected = module.features({name: frame.reset_index() for name, frame in self.crypto.items()}).iloc[-480:].to_numpy()
        self.assertEqual(expected.dtype, np.float64)
        self.assertEqual(expected.shape, (480, 57))
        self.assertTrue(np.array_equal(np.asarray(window["inputs"]), expected))
        self.assertEqual(window["asOfNs"], int(self.crypto["BTC-USDT"].index[-1].value) + 60_000_000_000)

    def test_token_original_23_features_and_base_volume_exact(self):
        import numpy as np
        module = rdt_feature_module(self.manifest, ROOT / "pipelines", "token_hour:XNVDA")
        window = rdt_window_from_markets(self.manifest, self.token, module, "token_hour:XNVDA")
        expected = module.features(self.token["XNVDA-USDT"].reset_index(), 3600).iloc[-64:].to_numpy()
        self.assertEqual(expected.dtype, np.float64)
        self.assertEqual(expected.shape, (64, 23))
        self.assertTrue(np.array_equal(np.asarray(window["inputs"]), expected))
        changed = self.token["XNVDA-USDT"].copy()
        changed["volume"] = changed["volume_quote"]
        wrong = module.features(changed.reset_index(), 3600).iloc[-64:].to_numpy()
        self.assertFalse(np.array_equal(expected, wrong))

    def test_missing_bars_and_feature_reordering_rejected(self):
        module = rdt_feature_module(self.manifest, ROOT / "pipelines", "crypto:BTC-USDT")
        bad = dict(self.crypto)
        bad["BTC-USDT-SWAP"] = bad["BTC-USDT-SWAP"].drop(bad["BTC-USDT-SWAP"].index[-10])
        with self.assertRaises(ValueError):
            rdt_window_from_markets(self.manifest, bad, module, "crypto:BTC-USDT")
        manifest = copy.deepcopy(self.manifest)
        manifest["domainContracts"]["crypto:BTC-USDT"]["names"].reverse()
        with self.assertRaises(ValueError):
            rdt_window_from_markets(manifest, self.crypto, module, "crypto:BTC-USDT")

    def test_worker_only_accepts_explicit_selected_contract(self):
        adapter = ModelAdapter.__new__(ModelAdapter)
        adapter.manifest = self.manifest
        adapter.contract_key = "crypto:BTC-USDT"
        self.assertEqual(adapter.contract({"domain": "crypto", "instrument": "BTC-USDT"})["sequenceBars"], 480)
        with self.assertRaises(ValueError):
            adapter.contract({"domain": "token_hour", "instrument": "XNVDA"})


if __name__ == "__main__":
    unittest.main()
