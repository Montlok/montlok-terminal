"""Read-only, run-scoped strategy views; exchange account selection is unrelated.

Existing-run contract: ``sector_rotation_paper.py`` writes ``manifest.json``,
``status.json`` and newline-terminated full snapshots to ``equity.jsonl`` every
10 seconds; orderly shutdown adds ``final.json``. Each curve snapshot contains
timezone-qualified ``observed_at``, ``nav_usdt``, ``pnl_usdt``, ``max_drawdown``
(signed fraction) and ``fees_usdt``. Orders/fills/positions come from RunView.
No account balance or latest status is substituted for missing historical data.
The history endpoint reports file availability separately from the run's state.
"""
from __future__ import annotations

import copy
import json
import math
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from managed_run_view import bound_model
from strategy_accounting import cash_ledger, cash_snapshot


def numeric(value):
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (ValueError, TypeError):
        return None


def timestamp(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return None
        return parsed.timestamp()
    except (ValueError, TypeError, AttributeError, OverflowError):
        return None


def read_object(path):
    try:
        with path.open("rb") as stream:
            data = stream.read(2 * 1024 * 1024 + 1)
        if len(data) > 2 * 1024 * 1024:
            return {}
        value = json.loads(data)
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


class GroupViews:
    """Only the configured paper run is readable, never a caller-provided path."""

    MAX_POINTS = 1600
    # Stream a full day on the first request without retaining its JSON objects.
    # Points remain bounded separately, so the normal 24-hour file loads promptly.
    MAX_BYTES = 64 * 1024 * 1024
    MAX_LINE = 64 * 1024
    CACHE_SECONDS = 1.0
    GROUP_IDS = frozenset({"baseline", "enhanced"})

    def __init__(self, paper_run: Path, paper_view, *, group_id=None, group_name=None):
        self.path = paper_run
        self.group_id = group_id
        self.group_name = group_name
        self.paper = paper_view
        self.lock = threading.RLock()
        self.cached_at = None
        self.cached_detail = None
        self.curve_at = None
        self.file_identity = None
        self.offset = 0
        self.samples = []
        self.sample_count = 0
        self.invalid_lines = 0
        self.peak = 0.0
        self.partial = False
        self.source_available = False
        self.source_status = "missing"
        self.source_issue = "未读取到 equity.jsonl"
        self._discarding_line = False
        self.accounting_ledger = None

    def _validate(self, group_id):
        if group_id not in ({self.group_id} if self.group_id else self.GROUP_IDS):
            raise KeyError("Unknown strategy group")

    def _baseline(self):
        now = time.monotonic()
        if self.cached_at is not None and now - self.cached_at < self.CACHE_SECONDS:
            return self.cached_detail
        final = self.path / "final.json"
        status = read_object(final if final.exists() else self.path / "status.json")
        manifest = read_object(self.path / "manifest.json")
        try:
            view = self.paper.snapshot()
            if not isinstance(view, dict):
                view = {}
        except (OSError, ValueError, KeyError, TypeError):
            view = {}
        sources = view.get("sources") if isinstance(view.get("sources"), dict) else {}
        managed = bool(self.group_id) or bool(sources) or bool(manifest.get("run_id"))
        if sources.get("status") is False:
            status = {}
        if sources.get('accounting') is False:
            status = {**status, **{key: None for key in ('nav_usdt', 'pnl_usdt', 'max_drawdown', 'fees_usdt')}}
        if sources.get("manifest") is False:
            manifest = {}
        if managed and self.group_id and (manifest.get("run_id") != self.path.name or manifest.get("group_id") != self.group_id):
            manifest, status = {}, {}
        self.accounting_ledger = cash_ledger(manifest, view)
        if self.accounting_ledger is not None:
            reconstructed = cash_snapshot(self.accounting_ledger, status)
            sources = {**sources, "accounting": reconstructed is not None,
                       "accountingBasis": "strategy_fill_ledger"}
            if reconstructed is None:
                status = {**status, **{key: None for key in ("nav_usdt", "pnl_usdt", "max_drawdown", "fees_usdt")}}
                view = {**view, "exceptions": [*view.get("exceptions", []),
                    {"id": "cash-accounting", "severity": "WARNING", "title": "收益数据待核对",
                     "detail": "成交记录与持仓数量尚未对应", "status": "OPEN"}]}
            else:
                status = {**status, **reconstructed}
                self._read_curve()
                status["max_drawdown"] = min((point[2] / 100 for point in self.samples), default=None)
        observed = timestamp(status.get("observed_at"))
        age = max(0, time.time() - observed) if observed is not None else None
        state = "unavailable"
        if status:
            engine_state = {"ACTIVE": "running", "STOPPED": "stopped", "HALTED": "halted",
                            "REDUCING": "reducing", "RECOVERING": "recovering", "ERROR": "error",
                            "FAULTED": "error", "FAILED": "error"}.get(view.get("tradingState"), "unknown")
            if final.exists():
                if status.get("error") or engine_state == "error" or status.get("trading_state") in {"ERROR", "FAULTED", "FAILED"}:
                    state = "error"
                elif status.get("deadline_reached") is True:
                    state = "completed"
                elif managed or status.get("deadline_reached") is False or engine_state == "stopped":
                    state = "stopped"
                else:
                    state = "completed"  # Legacy final snapshots predate explicit stop reasons.
            else:
                state = "stale" if age is None or age > 120 else engine_state
        nav, pnl = numeric(status.get("nav_usdt")), numeric(status.get("pnl_usdt"))
        capital = numeric(manifest.get("capital_usdt"))
        if capital is None:
            capital = nav - pnl if nav is not None and pnl is not None else None
        drawdown = numeric(status.get("max_drawdown"))
        signal_date = manifest.get("signal_as_of") or status.get("signal_as_of")
        signal_version = manifest.get("signals_sha256")
        execution_version = manifest.get("runner_sha256")
        started_at = timestamp(manifest.get("run_started_at"))
        scheduled_stop_at = numeric(manifest.get("stop_at_unix"))
        completed_at = (timestamp(status.get("completed_at")) or observed) if final.exists() and status else None
        market_source = manifest.get("market_data") or status.get("market_data")
        market_label = {"okx_public_live_websocket_l2": "OKX 公共 WebSocket 订单簿",
                        "okx_public_live_rest_l2_snapshots": "OKX 公共 REST 订单簿"}.get(market_source, market_source)
        if not market_label and not managed:
            market_label = "OKX 公共 REST 订单簿"
        fees = manifest.get("fee_assumption") if isinstance(manifest.get("fee_assumption"), dict) else {}
        maker, taker = numeric(fees.get("maker_bps")), numeric(fees.get("taker_bps"))
        fee_label = f"Maker {maker:g} / Taker {taker:g} bps" if maker is not None and taker is not None else None
        if not fee_label and not managed:
            fee_label = "买卖单边 10 bps"
        orders_total = None if sources.get("orders") is False else numeric(view.get("ordersTotal"))
        fills_total = None if sources.get("fills") is False else numeric(view.get("fillsTotal", status.get("fills")))
        group_name = "四板块 60/40"
        description = "板块趋势 · 逆波动 · 20 日动量"
        alpha = [
            {"name": "板块趋势", "rule": "板块代理 MA20/100", "source": group_name, "asOf": signal_date},
            {"name": "逆波动权重", "rule": "核心仓位 60%", "source": group_name, "asOf": signal_date},
            {"name": "20 日动量", "rule": "动量前三 40%", "source": group_name, "asOf": signal_date},
            {"name": "仓位约束", "rule": "单标的 10% / 单板块 25%", "source": group_name, "asOf": signal_date},
        ]
        matching_label = "Nautilus Sandbox · CASH · 1x"
        mode, mode_label = "nautilus_sandbox", "本地模拟"
        model = None
        if managed:
            # Labels supplied by the configured registry or this run's manifest
            # describe this instance; another baseline's rules are not evidence.
            recorded_name = manifest.get("group_name")
            registry_name = self.group_name if isinstance(self.group_name, str) and self.group_name.strip() else None
            group_name = registry_name or (recorded_name if isinstance(recorded_name, str) and recorded_name.strip() else self.group_id or "未记录")
            recorded_description = manifest.get("strategy_description")
            description = recorded_description if isinstance(recorded_description, str) and recorded_description.strip() else "未记录"
            metadata = manifest.get("alpha_metadata")
            alpha = [{key: row.get(key) for key in ("name", "rule", "source", "asOf")}
                     for row in metadata[:128] if isinstance(row, dict)
                     and all(isinstance(row.get(key), str) and row[key].strip() for key in ("name", "rule"))
                     and all(row.get(key) is None or isinstance(row.get(key), str) for key in ("source", "asOf"))] if isinstance(metadata, list) else []
            leverage = numeric(manifest.get("leverage"))
            account_type = manifest.get("account_type")
            matching_label = (f"Nautilus Sandbox · {account_type} · {leverage:g}x"
                              if manifest.get("execution") == "nautilus_sandbox" and isinstance(account_type, str) and leverage is not None else None)
            if manifest.get("mode") == "shadow":
                mode, mode_label, matching_label = "shadow", "影子运行", "预测与目标仓位"
            elif manifest.get("mode") == "live":
                mode, mode_label, matching_label = "live", "OKX 实盘", "OKX 现货 · CASH · 1x"
            model_scope = (manifest.get("run_id") == self.path.name and status.get("run_id") == self.path.name
                           and isinstance(manifest.get("group_id"), str) and status.get("group_id") == manifest["group_id"]
                           and (not self.group_id or manifest["group_id"] == self.group_id))
            if model_scope:
                model = bound_model(manifest, status.get("model"))
                if model is None and sources.get("view") is True and view.get("runId") == self.path.name and view.get("groupId") == manifest["group_id"]:
                    model = bound_model(manifest, view.get("model"))
        value = {
            "id": self.group_id or "baseline", "name": group_name, "description": description,
            "mode": mode, "modeLabel": mode_label, "matchingLabel": matching_label,
            **({"model": model} if model is not None else {}),
            "accountId": view.get("accountId") or manifest.get("account_id") or (f"SANDBOX:{self.path.name}" if managed else "PAPER-OKX-001"),
            "sources": sources, "ordersTotal": orders_total, "fillsTotal": fills_total, "marketSource": market_label,
            "runId": self.path.name, "status": state, "observedAt": observed,
            "signalAsOf": signal_date, "signalVersion": signal_version, "executionVersion": execution_version,
            "startedAt": started_at, "scheduledStopAt": scheduled_stop_at, "completedAt": completed_at,
            "elapsedSeconds": observed - started_at if observed is not None and started_at is not None and observed >= started_at else None,
            "metrics": {"nav": nav, "pnl": pnl, "capital": capital,
                        "returnPct": pnl / capital * 100 if pnl is not None and capital and capital > 0 else None,
                        "maxDrawdownPct": drawdown * 100 if drawdown is not None else None,
                        "fees": numeric(status.get("fees_usdt")), "fills": fills_total},
            "capabilities": {"start": False, "reason": "运行实例历史视图"},
            "alpha": alpha,
            "version": [
                {"key": "运行实例", "value": self.path.name},
                {"key": "策略组", "value": group_name},
                {"key": "信号日期", "value": signal_date},
                {"key": "信号版本", "value": signal_version},
                {"key": "执行版本", "value": execution_version},
                {"key": "运行器版本", "value": manifest.get("worker_sha256")},
                {"key": "配置版本", "value": manifest.get("settings_sha256")},
                {"key": "注册清单版本", "value": manifest.get("registry_sha256")},
                {"key": "启动时间 / UTC", "value": manifest.get("run_started_at")},
                {"key": "计划运行秒数", "value": numeric(manifest.get("planned_seconds"))},
                {"key": "行情来源", "value": market_label},
                {"key": "撮合方式", "value": matching_label},
                {"key": "手续费假设", "value": fee_label},
            ],
            "health": {"snapshotAgeSeconds": age, "instrumentsSeen": status.get("instruments_seen"),
                       "snapshotAvailable": bool(status), "detailAvailable": bool(view) and sources.get("view") is not False,
                       "tradingState": view.get("tradingState")},
        }
        for key in ("positions", "orders", "fills", "strategies", "systems", "exceptions", "runtimeConfig"):
            rows = view.get(key)
            value[key] = rows[-500:] if isinstance(rows, list) else []
        inventory=manifest.get('inventory') or {}
        pairs={row['instrument']+'.OKX':row for row in inventory.get('pairs',[]) if isinstance(row,dict) and isinstance(row.get('instrument'),str)}
        planned=manifest.get('instruments') or {}
        if not isinstance(planned,dict):planned={key:{} for key in planned if isinstance(key,str)}
        positions={row['instrument']:row for row in value['positions'] if isinstance(row,dict) and isinstance(row.get('instrument'),str)}
        value['universe']=[{'instrument':symbol.removesuffix('.OKX'),'weight':numeric(pairs.get(symbol,{}).get('weight',planned.get(symbol,{}).get('weight'))),
            'sector':pairs.get(symbol,{}).get('sector') or planned.get(symbol,{}).get('sector'),
            'quantity':positions.get(symbol,{}).get('quantity','0'),
            'notional':numeric(positions.get(symbol,{}).get('notional')),
            'targetQuantity':pairs.get(symbol,{}).get('targetQuantity')}
            for symbol in sorted(set(pairs)|set(planned)|set(positions))]
        if inventory.get('strategy')=='sector_regime_core_60_momentum_40':
            value['composition']={'core':.6,'momentum':.4,'description':'基础篮子 60% · 动量优选 40%',
                'universeLabel':'股票代币 / USDT','targetAssets':sum((row.get('weight') or 0)>0 for row in value['universe'])}
        value['allocation']=view.get('strategies',[{}])[0].get('allocation') if view.get('strategies') else None
        self.cached_at, self.cached_detail = now, value
        return value

    def _enhanced(self):
        return {
            "id": "enhanced", "name": "四板块 Alpha 叠加", "description": "板块组合 · Alpha 叠加",
            "mode": None, "modeLabel": "未配置", "accountId": None, "runId": None,
            "status": "pending_validation", "observedAt": None,
            "signalAsOf": None, "signalVersion": None, "executionVersion": None,
            "startedAt": None, "scheduledStopAt": None, "completedAt": None, "elapsedSeconds": None,
            "metrics": {key: None for key in ("nav", "pnl", "capital", "returnPct", "maxDrawdownPct", "fees", "fills")},
            "capabilities": {"start": False, "reason": "新增 Alpha 的组合验证、模型包和运行配置尚未发布"},
            "alpha": [
                {"name": "板块组合", "rule": "四板块 60/40 配置", "source": "四板块 60/40", "asOf": None},
                {"name": "叠加 Alpha", "rule": "待组合验证与版本发布", "source": "研究", "asOf": None},
            ],
            "version": [], "health": {"snapshotAvailable": False, "detailAvailable": False},
            **{key: [] for key in ("positions", "orders", "fills", "strategies", "systems", "exceptions", "runtimeConfig")},
        }

    def snapshot(self):
        with self.lock:
            groups = [self._baseline(), self._enhanced()]
            return {"groups": [copy.deepcopy({key: value for key, value in group.items()
                                             if key not in {"positions", "orders", "fills", "strategies", "systems", "exceptions", "runtimeConfig"}})
                               for group in groups]}

    def detail(self, group_id):
        self._validate(group_id)
        with self.lock:
            return copy.deepcopy(self._baseline() if self.group_id or group_id == "baseline" else self._enhanced())

    def _reduce_samples(self):
        if len(self.samples) <= self.MAX_POINTS:
            return
        # Retain each bucket's endpoints plus NAV and drawdown extremes. Peak and
        # drawdown are computed on every observation, before any downsampling.
        buckets = max(1, (self.MAX_POINTS - 2) // 6)
        width = math.ceil((len(self.samples) - 2) / buckets)
        output = [self.samples[0]]
        for start in range(1, len(self.samples) - 1, width):
            part = self.samples[start:min(start + width, len(self.samples) - 1)]
            indices = {0, len(part) - 1}
            for axis in (1, 2):
                indices.add(min(range(len(part)), key=lambda index: part[index][axis]))
                indices.add(max(range(len(part)), key=lambda index: part[index][axis]))
            output.extend(part[index] for index in sorted(indices))
        self.samples = [*output, self.samples[-1]]

    def _read_curve(self):
        now = time.monotonic()
        if self.curve_at is not None and now - self.curve_at < self.CACHE_SECONDS:
            return
        self.curve_at = now
        try:
            with (self.path / "equity.jsonl").open("rb") as stream:
                self.source_available, self.source_status, self.source_issue = True, "available", None
                stat = os.fstat(stream.fileno())
                identity = (stat.st_dev, stat.st_ino)
                if identity != self.file_identity or stat.st_size < self.offset:
                    self.file_identity, self.offset = identity, 0
                    self.samples, self.sample_count, self.invalid_lines, self.peak = [], 0, 0, 0.0
                    self._discarding_line = False
                stream.seek(self.offset)
                read_bytes = 0
                while read_bytes < self.MAX_BYTES:
                    before = stream.tell()
                    line = stream.readline(min(self.MAX_LINE + 1, self.MAX_BYTES - read_bytes))
                    if not line:
                        break
                    read_bytes += len(line)
                    if self._discarding_line:
                        self.offset = stream.tell()
                        self._discarding_line = not line.endswith(b"\n")
                        continue
                    if not line.endswith(b"\n"):
                        if len(line) > self.MAX_LINE:
                            self.invalid_lines += 1
                            self._discarding_line = True
                            self.offset = stream.tell()
                        else:
                            self.offset = before
                        break
                    self.offset = stream.tell()
                    try:
                        row = json.loads(line)
                        if self.accounting_ledger is not None:
                            corrected = cash_snapshot(self.accounting_ledger, row)
                            if corrected is None:
                                raise ValueError("Incomplete strategy accounting")
                            row = {**row, **corrected}
                        at, nav = timestamp(row.get("observed_at")), numeric(row.get("nav_usdt"))
                        if at is None or nav is None or nav <= 0:
                            raise ValueError("Invalid observation")
                        if self.samples and at <= self.samples[-1][0]:
                            raise ValueError("Non-increasing observation")
                        pnl = numeric(row.get("pnl_usdt"))
                        self.peak = max(self.peak, nav, nav - pnl if pnl is not None else nav)
                        recorded = numeric(row.get("max_drawdown"))
                        drawdown = recorded * 100 if recorded is not None else (nav / self.peak - 1) * 100
                        self.samples.append((at, nav, drawdown))
                        self.sample_count += 1
                        if len(self.samples) > self.MAX_POINTS * 2:
                            self._reduce_samples()
                    except (ValueError, TypeError, AttributeError):
                        self.invalid_lines += 1
                self.partial = self.offset < stat.st_size
                self._reduce_samples()
        except OSError as error:
            # Preserve already read points, but never describe source loss as an
            # empty history or a catch-up that will automatically succeed.
            self.source_available = False
            self.source_status = "missing" if isinstance(error, FileNotFoundError) else "unreadable"
            self.source_issue = "未读取到 equity.jsonl" if self.source_status == "missing" else "无法读取 equity.jsonl"
            self.partial = False

    def equity(self, group_id):
        self._validate(group_id)
        with self.lock:
            self._baseline()
            try:
                current = self.paper.snapshot()
            except (OSError, ValueError, KeyError, TypeError):
                current = {}
            if isinstance(current, dict) and (current.get('sources') or {}).get('accounting') is False:
                return {'groupId': group_id, 'runId': self.path.name, 'points': [], 'drawdown': [],
                        'sampleCount': 0, 'partial': False, 'invalidLines': 0, 'sourceAvailable': False,
                        'sourceStatus': 'invalid_attribution', 'sourceIssue': '历史成交已从本次运行统计排除'}
            if group_id == "enhanced" and not self.group_id:
                return {"groupId": group_id, "runId": None, "points": [], "drawdown": [],
                        "sampleCount": 0, "partial": False, "invalidLines": 0,
                        "sourceAvailable": False, "sourceStatus": "unconfigured", "sourceIssue": None}
            self._read_curve()
            return {"groupId": group_id, "runId": self.path.name,
                    "points": [{"time": at, "value": nav} for at, nav, _ in self.samples],
                    "drawdown": [{"time": at, "value": drawdown} for at, _, drawdown in self.samples],
                    "sampleCount": self.sample_count, "partial": self.partial, "invalidLines": self.invalid_lines,
                    "sourceAvailable": self.source_available, "sourceStatus": self.source_status,
                    "sourceIssue": self.source_issue}
