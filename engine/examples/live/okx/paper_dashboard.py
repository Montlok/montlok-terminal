#!/usr/bin/env python3
"""Serve the existing Operator UI with read-only data from one paper run."""

import argparse
import json
import re
import threading
from datetime import datetime
from datetime import timezone
from functools import partial
from http.server import SimpleHTTPRequestHandler
from http.server import ThreadingHTTPServer
from pathlib import Path
from zoneinfo import ZoneInfo

import psutil


SECTORS = {
    "semiconductor_ai_hardware": "半导体 / AI",
    "mega_tech_cloud_software": "大型科技 / 云",
    "crypto_fintech_high_beta": "加密金融",
    "industrial_mobility_space": "工业 / 航天",
}


class RunView:
    def __init__(self, path: Path):
        self.path = path
        self.offsets = {}
        self.points = {}
        self.orders = {}
        self.fills = {}
        self.event_count = 0
        self.lock = threading.Lock()

    def read_new(self, filename):
        path = self.path / filename
        if not path.exists():
            return []
        output = []
        with path.open() as stream:
            stream.seek(self.offsets.get(filename, 0))
            while True:
                before = stream.tell()
                line = stream.readline()
                if not line or not line.endswith("\n"):
                    self.offsets[filename] = before
                    break
                output.append(json.loads(line))
        return output

    def snapshot(self):
        with self.lock:
            return self._snapshot()

    def _snapshot(self):
        final_path = self.path / "final.json"
        status = json.loads((final_path if final_path.exists() else self.path / "status.json").read_text())
        manifest = json.loads((self.path / "manifest.json").read_text())
        now = datetime.now(timezone.utc)
        observed = datetime.fromisoformat(status["observed_at"])
        age = max(0, (now - observed).total_seconds())
        try:
            process = psutil.Process(manifest["pid"])
            alive = abs(process.create_time() - manifest["process_create_time"]) < 0.01 and process.is_running()
        except psutil.Error:
            alive = False
        for row in self.read_new("equity.jsonl"):
            timestamp = int(datetime.fromisoformat(row["observed_at"]).timestamp())
            self.points[timestamp] = row["nav_usdt"]
        for row in self.read_new("events.jsonl"):
            self.event_count += 1
            text = row["event"]
            def field(name, default=""):
                found = re.search(r"(?:\(|, )" + re.escape(name) + r"=([^,)]+)", text)
                return found.group(1) if found else default
            order_id = field("client_order_id")
            if not order_id:
                continue
            kind = row["type"]
            if kind == "OrderInitialized":
                price = re.search(r"'price': '([^']+)'", text)
                self.orders[order_id] = {
                    "id": order_id, "strategy": "SECTOR-60/40", "instrument": field("instrument_id"),
                    "side": "Buy" if field("side") == "BUY" else "Sell", "type": "Limit",
                    "quantity": float(field("quantity", "0")), "price": float(price.group(1)) if price else None,
                    "status": "PENDING",
                }
            elif order_id in self.orders:
                states = {"OrderAccepted": "OPEN", "OrderFilled": "FILLED", "OrderCanceled": "CANCELED", "OrderRejected": "REJECTED", "OrderDenied": "REJECTED"}
                if kind in states:
                    self.orders[order_id]["status"] = states[kind]
            if kind == "OrderFilled":
                trade_id = field("trade_id")
                self.fills[trade_id] = {
                    "id": trade_id, "time": row["observed_at"], "instrument": field("instrument_id"),
                    "side": "Buy" if field("order_side") == "BUY" else "Sell",
                    "quantity": float(field("last_qty", "0")), "price": float(field("last_px", "0").split()[0]),
                    "fee": float(field("commission", "0").split()[0]),
                }
        positions = []
        for row in status["positions"]:
            qty = row["quantity"]
            positions.append({
                "strategy": "SECTOR-60/40", "instrument": row["instrument"], "side": "Long",
                "quantity": qty, "averagePrice": row["bid_mark"] - row["unrealized_pnl_usdt"] / qty,
                "markPrice": row["bid_mark"], "unrealizedPnl": row["unrealized_pnl_usdt"],
                "notional": qty * row["bid_mark"], "margin": 0, "state": "ACTIVE",
            })
        worst_quote_age = max(status["quote_age_seconds"].values(), default=0) + age
        # The engine suppresses identical BBO updates. A quiet instrument's quote
        # timestamp therefore measures time since its last BBO change, not HTTP
        # transport liveness. Global liveness needs some recent market updates.
        newest_quote_age = min(status["quote_age_seconds"].values(), default=999) + age
        fresh = age < 30 and newest_quote_age < 60 and status["instruments_seen"] == 25
        exceptions = []
        if alive and not fresh:
            exceptions.append({"id": "data-age", "severity": "CRITICAL", "title": "数据更新延迟", "detail": f"快照 {age:.0f} 秒前，最长行情年龄 {worst_quote_age:.0f} 秒", "firstSeen": observed.isoformat(), "age": f"{age:.0f}s", "status": "OPEN"})
        strategies = []
        for sector, regime in manifest["regimes"].items():
            members = {k for k, v in manifest["instruments"].items() if v["sector"] == sector}
            strategies.append({
                "id": SECTORS[sector], "artifact": manifest["runner_sha256"][:12],
                "configVersion": "60/40 · 逆波动", "account": "PAPER-OKX-001",
                "instrument": f"{len(members)} 标的 · {regime['proxy']} MA20/100",
                "mode": "ACTIVE" if alive and regime["enabled"] else "PAUSED",
                "pnl": sum(p["unrealizedPnl"] for p in positions if p["instrument"] in members) - sum(f["fee"] for f in self.fills.values() if f["instrument"] in members),
                "volume": sum(f["quantity"] * f["price"] for f in self.fills.values() if f["instrument"] in members),
                "heartbeat": f"{age:.0f}s",
            })
        stop_at = datetime.fromtimestamp(manifest["stop_at_unix"], ZoneInfo("Asia/Shanghai")).strftime("%m-%d %H:%M:%S")
        config_rows = {
            "撮合环境": "Nautilus Sandbox / CASH / 1x", "行情来源": "OKX 公开 REST · 每侧 10 档",
            "信号日期": manifest["signal_as_of"], "初始模拟资金": "2000 USDT",
            "组合": "板块趋势 + 60% 核心 + 40% 动量前三", "仓位上限": "单标的 10% / 单板块 25%",
            "手续费假设": "买卖单边 10 bps", "预计结束（UTC+8）": stop_at,
            "ML 影子模型": "未加载；当前仅运行选定的规则 Alpha", "引擎版本": "46f5302ac3",
        }
        return {
            "schemaVersion": 1, "observedAtNs": int(observed.timestamp() * 1e9),
            "environment": "PAPER", "accountId": "PAPER-OKX-001", "runId": self.path.name,
            "tradingState": "ACTIVE" if alive else "HALTED", "readyToTrade": False,
            "systems": [
                {"id": "market", "label": "行情", "level": "HEALTHY" if fresh else "DEGRADED", "detail": f"{status['instruments_seen']}/25 · REST快照"},
                {"id": "execution", "label": "执行", "level": "HEALTHY" if alive else "DEGRADED", "detail": "本地模拟撮合" if alive else "已结束"},
                {"id": "event-store", "label": "记录", "level": "HEALTHY", "detail": f"JSONL · {self.event_count} 事件"},
            ],
            "market": {"instrumentId": "组合净值", "venue": "OKX", "last": status["nav_usdt"], "changePct": status["pnl_usdt"] / 2000 * 100, "mark": status["nav_usdt"], "high24h": max(self.points.values(), default=2000), "low24h": min(self.points.values(), default=2000), "volume24h": "—", "fundingRate": 0, "fundingCountdown": "—", "bid": status["nav_usdt"], "ask": status["nav_usdt"], "equity": [{"time": k, "value": v} for k, v in sorted(self.points.items())]},
            "risk": {"nav": status["nav_usdt"], "cash": status["cash_usdt"], "fees": status["fees_usdt"], "maxDrawdown": status["max_drawdown"], "fills": status["fills"], "stopAt": stop_at, "netExposure": sum(p["notional"] for p in positions), "delta": 0, "margin": 0, "marginRatioPct": 0, "dailyPnl": status["pnl_usdt"], "dailyLossLimit": 0, "dailyLossUsedPct": 0, "queuePressurePct": 0, "lastReconcileAt": observed.astimezone(ZoneInfo("Asia/Shanghai")).strftime("%H:%M:%S"), "reconcileAge": f"{age:.0f}s"},
            "positions": positions, "orders": list(self.orders.values()), "fills": list(reversed(self.fills.values())),
            "exceptions": exceptions, "strategies": strategies,
            "runtimeConfig": [{"key": k, "value": v, "previous": v, "changed": False} for k, v in config_rows.items()],
            "strategyArtifact": manifest["signals_sha256"][:12], "configVersion": "sector-60/40",
            "eventStore": "JSONL · 已记录", "grafanaUrl": "", "memoryPct": status["rss_bytes"] / psutil.virtual_memory().total * 100, "cpuPct": 0,
        }


class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        host = self.headers.get("Host", "").split(":")[0]
        if host not in {"127.0.0.1", "localhost"}:
            self.send_error(403)
            return
        if self.path.split("?")[0] == "/api/operator/v1/snapshot":
            try:
                body = json.dumps(self.server.view.snapshot(), allow_nan=False).encode()
            except (OSError, ValueError, KeyError) as error:
                self.send_error(503, str(error))
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            super().do_GET()

    def list_directory(self, path):
        self.send_error(404)
        return None

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--static-dir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=18080)
    args = parser.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), partial(Handler, directory=str(args.static_dir.resolve())))
    server.view = RunView(args.run_dir.resolve())
    server.serve_forever()
