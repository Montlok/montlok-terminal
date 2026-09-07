"""Reconstruct a cash-funded sandbox strategy from its complete owned fill ledger.

The sandbox adapter does not implement venue position reports. Older deployments
ran position reconciliation against its empty response and created EXTERNAL
offsets. These account-wide adjustments must not become strategy performance.
Raw files remain untouched; incomplete ledgers never produce estimated returns.
"""
from decimal import Decimal, InvalidOperation


def value(raw):
    result = Decimal(str(raw))
    if not result.is_finite():
        raise ValueError("non-finite accounting value")
    return result


def cash_ledger(manifest, view):
    if (manifest.get("execution") != "nautilus_sandbox"
            or manifest.get("account_type") != "CASH"
            or manifest.get("rebalance_policy") != "frozen_signals_initial_allocation"
            or not manifest.get("group_id") or not manifest.get("run_id")):
        return None
    fills = view.get("fills", [])
    if not isinstance(fills, list) or len(fills) != view.get("fillsTotal"):
        return {"valid": False, "reason": "成交记录不完整"}
    try:
        capital = value(manifest["capital_usdt"])
        if capital <= 0:
            raise ValueError("invalid capital")
        rows = sorted(fills, key=lambda row: row["time"])
        seen = set()
        cash, fees, inventory = capital, Decimal(0), {}
        states = [(cash, fees, dict(inventory))]
        for row in rows:
            instrument, trade_id = row["instrument"], row["id"]
            if (not trade_id or (instrument, trade_id) in seen
                    or str(row.get("strategy", "")).startswith("EXTERNAL")
                    or not instrument.endswith("-USDT.OKX")
                    or row.get("feeCurrency") != "USDT"
                    or row["side"] not in ("BUY", "SELL")):
                raise ValueError("invalid owned fill")
            seen.add((instrument, trade_id))
            quantity, price, fee = value(row["quantity"]), value(row["price"]), value(row["fee"])
            if quantity <= 0 or price <= 0:
                raise ValueError("invalid fill size")
            sign = 1 if row["side"] == "BUY" else -1
            cash -= sign * quantity * price + fee
            fees += fee
            inventory[instrument] = inventory.get(instrument, Decimal(0)) + sign * quantity
            if inventory[instrument] < 0 or cash < Decimal("-0.000001"):
                raise ValueError("cash-funded ledger overdrawn")
            states.append((cash, fees, dict(inventory)))
        return {"valid": True, "capital": capital, "states": states}
    except (KeyError, ValueError, TypeError, InvalidOperation):
        return {"valid": False, "reason": "成交与资金记录待核对"}


def cash_snapshot(ledger, snapshot):
    if not ledger or not ledger.get("valid"):
        return None
    try:
        count = snapshot["fills"]
        if type(count) is not int or not 0 <= count < len(ledger["states"]):
            return None
        cash, fees, expected = ledger["states"][count]
        positions = snapshot["positions"]
        if not isinstance(positions, list):
            return None
        actual, market_value = {}, Decimal(0)
        for row in positions:
            instrument = row["instrument"]
            quantity, mark = value(row["quantity"]), value(row["bid_mark"])
            if quantity < 0 or mark <= 0:
                return None
            actual[instrument] = actual.get(instrument, Decimal(0)) + quantity
            market_value += quantity * mark
        if any(abs(actual.get(key, Decimal(0)) - expected.get(key, Decimal(0))) > Decimal("0.000000001")
               for key in actual.keys() | expected.keys()):
            return None
        nav = cash + market_value
        return {"cash_usdt": float(cash), "nav_usdt": float(nav),
                "pnl_usdt": float(nav - ledger["capital"]), "fees_usdt": float(fees),
                "max_drawdown": None}
    except (KeyError, TypeError, ValueError, InvalidOperation):
        return None
