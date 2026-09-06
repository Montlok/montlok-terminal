"""Offline ABI smoke for the actual PyO3-to-Cython market-data boundary."""
import faulthandler
import json

faulthandler.enable()
from nautilus_trader.core import nautilus_pyo3 as native
from nautilus_trader.model.data import capsule_to_data

quote = native.QuoteTick(
    native.InstrumentId.from_str("BTC-USDT.OKX"),
    native.Price.from_str("80000.1"), native.Price.from_str("80000.2"),
    native.Quantity.from_str("0.125"), native.Quantity.from_str("0.250"),
    1788680400000000000, 1788680400000000001,
)
print("native_quote_created", flush=True)
capsule = quote.as_pycapsule()
print("native_capsule_created", flush=True)
converted = capsule_to_data(capsule)
assert type(converted).__name__ == "QuoteTick", f"ABI type mismatch: QuoteTick decoded as {type(converted).__name__}"
assert str(converted.instrument_id) == "BTC-USDT.OKX"
assert str(converted.bid_price) == "80000.1"
assert str(converted.ask_price) == "80000.2"
assert str(converted.bid_size) == "0.125"
assert str(converted.ask_size) == "0.250"
assert converted.ts_event == 1788680400000000000
assert converted.ts_init == 1788680400000000001
print(json.dumps({"pass": True, "kind": "offline_quote_capsule_abi", "quote": str(converted)}), flush=True)
