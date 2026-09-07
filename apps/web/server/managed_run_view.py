"""RunView-compatible projections for one supervisor-owned native sandbox run.

The caller resolves run_dir from GroupSupervisor, never from a browser path. Missing
order/fill projections are represented by source flags and an exception, not by
claiming there have been zero orders. No account-wide balances enter this view.
"""
from __future__ import annotations

import json
import copy
import math
import time
from datetime import datetime
from pathlib import Path

import psutil


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


def read_coherent_pair(status_path, view_path, attempts=6, retry_delay=0.005):
    """Read one matching status/view generation across atomic file swaps.

    The worker publishes status first and view second. Reading view before
    status gives us either a coherent previous generation or, during the
    narrow swap window, a mismatch which is retried without relaxing identity
    validation.
    """
    status = {}
    view = {}
    for attempt in range(attempts):
        view = read_object(view_path)
        status = read_object(status_path)
        if view.get("observed_at") == status.get("observed_at"):
            break
        if attempt + 1 < attempts:
            time.sleep(retry_delay)
    return status, view


def number(value):
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError):
        return None


def timestamp(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.timestamp() if parsed.tzinfo is not None else None
    except (ValueError, TypeError, AttributeError):
        return None


def bound_model(manifest, value):
    """Attach only a model snapshot matching this run's frozen release identity."""
    if (not isinstance(value, dict) or not manifest.get("model_sha256") or not manifest.get("model_release")
            or value.get("modelHash") != manifest["model_sha256"] or value.get("releaseId") != manifest["model_release"]
            or value.get("runId", manifest.get("run_id")) != manifest.get("run_id")
            or value.get("groupId", manifest.get("group_id")) != manifest.get("group_id")):
        return None
    return {**copy.deepcopy(value), "runId": manifest["run_id"], "groupId": manifest["group_id"]}


class ManagedRunView:
    def __init__(self, run_dir: Path, expected_group_id: str):
        if not isinstance(expected_group_id, str) or not expected_group_id:
            raise ValueError("Expected strategy group is required")
        self.path = run_dir
        self.expected_group_id = expected_group_id

    def process_alive(self, manifest):
        try:
            process = psutil.Process(int(manifest["pid"]))
            return (process.create_time() == manifest["process_create_time"]
                    and process.is_running() and process.status() != psutil.STATUS_ZOMBIE)
        except (psutil.Error, ValueError, TypeError, KeyError):
            return False

    def snapshot(self):
        manifest = read_object(self.path / "manifest.json")
        final = (self.path / "final.json").exists()
        status, view = read_coherent_pair(
            self.path / ("final.json" if final else "status.json"),
            self.path / "view.json",
        )
        expected = self.path.name
        valid_manifest = (manifest.get("run_id") == expected and manifest.get("execution") in {"nautilus_sandbox", "okx_live"}
                          and manifest.get("group_id") == self.expected_group_id
                          and manifest.get("account_type") == "CASH" and isinstance(manifest.get("instruments"), dict))
        valid_status = (valid_manifest and status.get("run_id") == expected
                        and status.get("group_id") == self.expected_group_id and bool(status))
        observed = timestamp(status.get("observed_at")) if valid_status else None
        age = max(0, time.time() - observed) if observed is not None else None
        alive = self.process_alive(manifest) if valid_manifest and not final else False
        valid_view = (valid_manifest and valid_status and view.get("schemaVersion") == 1 and view.get("runId") == expected
                      and view.get("groupId") == self.expected_group_id
                      and view.get("observed_at") == status.get("observed_at")
                      and view.get("completed") is final
                      and all(isinstance(view.get(key), list) for key in ("positions", "orders", "fills", "strategies"))
                      and all(type(view.get(key)) is int and view[key] >= 0 for key in ("ordersTotal", "fillsTotal")))
        if not valid_view:
            view = {}
        foreign_only = bool(view.get('orders') and all(str(o.get('strategy', '')).startswith('EXTERNAL') for o in view['orders']))
        if foreign_only:
            view = {**view, 'orders': [], 'fills': [], 'positions': [], 'ordersTotal': 0, 'fillsTotal': 0}
        mode = ("live" if valid_manifest and (manifest.get("mode") == "live" or manifest.get('execution') == 'okx_live') else
                "shadow" if valid_manifest and manifest.get("mode") == "shadow" else "nautilus_sandbox")
        model = bound_model(manifest, status.get("model")) if valid_status else None
        if model is None and valid_view:
            model = bound_model(manifest, view.get("model"))
        state = "UNKNOWN"
        if valid_status:
            recorded = status.get("trading_state")
            state = recorded if recorded in {"ACTIVE", "HALTED", "REDUCING", "RECOVERING", "STOPPED", "ERROR"} else "UNKNOWN"
            if final:
                state = "ERROR" if status.get("error") else "STOPPED"
            elif not alive or age is None or age > 120:
                state = "UNKNOWN"
        exceptions = []
        if foreign_only:
            exceptions.append({'id': 'historical-fill-attribution', 'severity': 'WARNING',
                'title': '历史成交已排除', 'detail': '对账导入的旧成交不计入本次运行，原始记录保留。', 'status': 'OPEN'})
        if not valid_manifest or not valid_status:
            exceptions.append({"id": "run-source", "severity": "CRITICAL", "title": "实例数据不可用",
                               "detail": "运行编号与文件内容不一致或快照缺失", "status": "OPEN"})
        if not valid_view:
            exceptions.append({"id": "view-source", "severity": "WARNING", "title": "订单与成交数据未同步",
                               "detail": "结构化视图缺失或与当前快照不一致", "status": "OPEN"})
        if valid_manifest and manifest.get("model_release") and model is None:
            exceptions.append({"id": "model-source", "severity": "WARNING", "title": "模型状态未同步",
                               "detail": "模型状态缺失或与该实例固定的版本不一致", "status": "OPEN"})
        if not final and valid_status and (not alive or age is None or age > 120):
            exceptions.append({"id": "run-status", "severity": "CRITICAL", "title": "运行状态未确认",
                               "detail": "进程身份不匹配或快照已过期", "status": "OPEN"})
        if status.get("error"):
            exceptions.append({"id": "run-error", "severity": "CRITICAL", "title": "运行异常",
                               "detail": str(status["error"]), "status": "OPEN"})
        health = view.get("health") if isinstance(view.get("health"), dict) else {}
        alerts = health.get("alerts") if isinstance(health.get("alerts"), list) else []
        for row in alerts:
            if isinstance(row, dict):
                exceptions.append({"id": row.get("key"), "severity": row.get("level"), "title": row.get("title"),
                    "detail": row.get("detail"), "firstSeen": row.get("since"), "status": "OPEN"})
        expected_count = len(manifest.get("instruments", {})) if valid_manifest else None
        seen = number(status.get("instruments_seen")) if valid_status else None
        connected = view.get("connected") if isinstance(view.get("connected"), dict) else {}
        market_source = manifest.get("market_data") if valid_manifest else None
        healthy_market = valid_view and connected.get("data") is True and seen == expected_count and age is not None and age <= 30
        systems = [
            {"id": "market", "label": "行情", "level": "HEALTHY" if healthy_market and not final else "DEGRADED",
             "detail": f"{seen:g}/{expected_count} · {market_source}" if seen is not None else "尚无行情快照"},
            {"id": "execution", "label": "执行", "level": "HEALTHY" if alive and connected.get("exec") is True else "DEGRADED",
             "detail": state},
            {"id": "event-store", "label": "记录", "level": "HEALTHY" if valid_view else "DEGRADED",
             "detail": f"{view.get('eventCount')} 事件" if valid_view else "结构化视图未同步"},
        ]
        config = {
            "运行实例": expected, "策略组": manifest.get("group_id"), "初始模拟资金 / USDT": number(manifest.get("capital_usdt")),
            "行情来源": market_source, "撮合环境": manifest.get("execution"), "账户类型": manifest.get("account_type"),
            "杠杆": number(manifest.get("leverage")), "信号日期": manifest.get("signal_as_of"),
            "信号版本": manifest.get("signals_sha256"), "策略版本": manifest.get("runner_sha256"),
            "运行器版本": manifest.get("worker_sha256"), "配置版本": manifest.get("settings_sha256"),
            "再平衡规则": manifest.get("rebalance_policy"), "计划结束时间 / Unix": number(manifest.get("stop_at_unix")),
            "生成模拟订单": manifest.get("orders_enabled"), "手续费假设": manifest.get("fee_assumption"),
        } if valid_manifest else {"运行实例": expected}
        if mode == "shadow":
            config.update({"运行模式": "影子运行", "撮合环境": "预测与目标仓位", "生成模拟订单": False})
        elif mode == "live":
            config.pop("生成模拟订单", None)
            config.pop("初始模拟资金 / USDT", None)
            config["分配资金 / USDT"] = number(status.get("capital_usdt"))
            config.update({"运行模式": "OKX 实盘", "撮合环境": "OKX 现货", "订单启用": True})
        if model is not None:
            config.update({"模型发布版本": model["releaseId"], "模型权重版本": model["modelHash"],
                           "模型运行设备": model.get("device"), "模型预热完成": model.get("warmupComplete")})
        return {"schemaVersion": 1, "runId": expected, "groupId": manifest.get("group_id") if valid_manifest else None,
                "environment": "LIVE" if mode == "live" else "PAPER",
                "accountId": manifest.get("account_id") if mode == "live" else f"SANDBOX:{expected}", "tradingState": state,
                "mode": mode, "modeLabel": "OKX 实盘" if mode == "live" else "影子运行" if mode == "shadow" else "本地模拟",
                "matchingLabel": "OKX 现货 · CASH · 1x" if mode == "live" else "预测与目标仓位" if mode == "shadow" else "Nautilus Sandbox · CASH · 1x",
                "readyToTrade": bool(mode != "shadow" and valid_view and alive and state == "ACTIVE" and healthy_market and connected.get("exec") is True
                                     and (model is None or model.get("warmupComplete") is True)),
                "observedAtNs": int(observed * 1e9) if observed is not None else None,
                "sources": {"manifest": valid_manifest, "status": valid_status, "view": valid_view,
                            "accounting": not foreign_only,
                            "orders": valid_view, "fills": valid_view, "model": model is not None,
                            "snapshotAgeSeconds": age, "processAlive": alive},
                "positions": view.get("positions", []), "orders": view.get("orders", []), "fills": view.get("fills", []),
                "ordersTotal": number(view.get("ordersTotal")) if valid_view else None,
                "fillsTotal": number(view.get("fillsTotal")) if valid_view else None,
                "ordersAccepted": number(view.get("ordersAccepted")) if valid_view else None,
                "ordersDenied": number(view.get("ordersDenied")) if valid_view else None,
                "ordersRejected": number(view.get("ordersRejected")) if valid_view else None,
                "strategies": view.get("strategies", []), "systems": systems, "exceptions": exceptions,
                **({"model": model} if model is not None else {}),
                "runtimeConfig": [{"key": key, "value": value, "previous": value, "changed": False} for key, value in config.items()]}
