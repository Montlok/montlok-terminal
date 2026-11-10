# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
# -------------------------------------------------------------------------------------------------
"""
Durable, bounded and idempotent alpha-signal inbox.

The inbox is deliberately not an order API. An external model may express a short-lived target
intent; a locally configured Nautilus strategy remains responsible for sizing and order creation.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sqlite3
import stat
import time
from dataclasses import asdict
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Final
from typing import Self


try:
    import fcntl
except ImportError:  # pragma: no cover - Nautilus live deployments target Unix hosts
    fcntl = None  # type: ignore[assignment]


ALPHA_SIGNAL_SCHEMA_VERSION: Final[int] = 1
MAX_TEXT_BYTES: Final[int] = 255
MAX_REASON_BYTES: Final[int] = 1_024
MAX_FUTURE_SKEW_NS: Final[int] = 1_000_000_000
DEFAULT_BUSY_TIMEOUT_MS: Final[int] = 1_000
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,254}$")
_HEX_DIGEST = re.compile(r"^[0-9a-f]{64}$")


class SignalAction(StrEnum):
    """Agent actions supported by the first, long-only boundary."""

    LONG = "LONG"
    FLAT = "FLAT"
    HOLD = "HOLD"


class SignalStatus(StrEnum):
    """Durable signal lifecycle states."""

    PENDING = "PENDING"
    CLAIMED = "CLAIMED"
    DISPATCHED = "DISPATCHED"
    IGNORED = "IGNORED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    ABANDONED = "ABANDONED"


TERMINAL_STATUSES: Final[frozenset[SignalStatus]] = frozenset(
    {
        SignalStatus.DISPATCHED,
        SignalStatus.IGNORED,
        SignalStatus.REJECTED,
        SignalStatus.EXPIRED,
        SignalStatus.ABANDONED,
    },
)


@dataclass(frozen=True, slots=True)
class AlphaSignalPolicy:
    """Host-owned admission policy for external model signals."""

    allowed_model_artifacts: frozenset[tuple[str, str]]
    allowed_route_ids: frozenset[str]
    allowed_instrument_ids: frozenset[str]
    max_ttl_ns: int = 30_000_000_000
    max_pending_signals: int = 1_024

    def validate(self) -> None:
        if not self.allowed_model_artifacts:
            raise ValueError("allowed_model_artifacts must not be empty")
        if not self.allowed_route_ids:
            raise ValueError("allowed_route_ids must not be empty")
        if not self.allowed_instrument_ids:
            raise ValueError("allowed_instrument_ids must not be empty")
        for model_id, digest in self.allowed_model_artifacts:
            _require_safe_id("allowed_model_artifacts.model_id", model_id)
            if not _HEX_DIGEST.fullmatch(digest):
                raise ValueError(
                    "allowed_model_artifacts digest must be a lowercase SHA-256 hex digest",
                )
        for field_name, values in (
            ("allowed_route_ids", self.allowed_route_ids),
            ("allowed_instrument_ids", self.allowed_instrument_ids),
        ):
            for value in values:
                _require_safe_id(field_name, value)
        if not 1_000_000 <= self.max_ttl_ns <= 300_000_000_000:
            raise ValueError("max_ttl_ns must be between 1ms and 300s")
        if not 1 <= self.max_pending_signals <= 100_000:
            raise ValueError("max_pending_signals must be between 1 and 100000")


@dataclass(frozen=True, slots=True)
class AlphaSignal:
    """A model-produced target intent, never a venue order."""

    signal_id: str
    model_id: str
    model_version: str
    model_artifact_digest: str
    route_id: str
    instrument_id: str
    action: SignalAction
    confidence: float
    generated_at_ns: int
    expires_at_ns: int
    rationale_digest: str | None = None
    schema_version: int = ALPHA_SIGNAL_SCHEMA_VERSION

    def validate(self, policy: AlphaSignalPolicy, now_ns: int) -> None:
        policy.validate()
        if self.schema_version != ALPHA_SIGNAL_SCHEMA_VERSION:
            raise ValueError(f"unsupported alpha signal schema version {self.schema_version}")
        self._validate_identity(policy)
        self._validate_timing(policy, now_ns)
        if not math.isfinite(self.confidence) or not 0.0 <= self.confidence <= 1.0:
            raise ValueError("confidence must be finite and between 0 and 1")

    def _validate_identity(self, policy: AlphaSignalPolicy) -> None:
        for field_name, value in (
            ("signal_id", self.signal_id),
            ("model_id", self.model_id),
            ("model_version", self.model_version),
            ("route_id", self.route_id),
            ("instrument_id", self.instrument_id),
        ):
            _require_safe_id(field_name, value)
        if not _HEX_DIGEST.fullmatch(self.model_artifact_digest):
            raise ValueError("model_artifact_digest must be a lowercase SHA-256 hex digest")
        if self.rationale_digest is not None and not _HEX_DIGEST.fullmatch(self.rationale_digest):
            raise ValueError("rationale_digest must be a lowercase SHA-256 hex digest")
        if (self.model_id, self.model_artifact_digest) not in policy.allowed_model_artifacts:
            raise PermissionError("model identity and artifact digest are not admitted together")
        if self.route_id not in policy.allowed_route_ids:
            raise PermissionError(f"route_id '{self.route_id}' is not admitted")
        if self.instrument_id not in policy.allowed_instrument_ids:
            raise PermissionError(f"instrument_id '{self.instrument_id}' is not admitted")

    def _validate_timing(self, policy: AlphaSignalPolicy, now_ns: int) -> None:
        if self.generated_at_ns <= 0:
            raise ValueError("generated_at_ns must be positive")
        if self.generated_at_ns > now_ns + MAX_FUTURE_SKEW_NS:
            raise ValueError("generated_at_ns is too far in the future")
        if self.expires_at_ns <= now_ns:
            raise ValueError("signal is already expired")
        if self.expires_at_ns <= self.generated_at_ns:
            raise ValueError("expires_at_ns must be after generated_at_ns")
        if self.expires_at_ns - self.generated_at_ns > policy.max_ttl_ns:
            raise ValueError("signal TTL exceeds the host policy")

    def request_hash(self) -> str:
        payload = asdict(self)
        payload["action"] = self.action.value
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class SignalReceipt:
    """Durable result for an admitted signal."""

    signal_id: str
    status: SignalStatus
    request_hash: str
    created_at_ns: int
    updated_at_ns: int
    detail: str | None

    def to_dict(self) -> dict[str, object]:
        result = asdict(self)
        result["status"] = self.status.value
        return result


class InboxConsumerLease:
    """Process-held exclusive lease for the single alpha-signal consumer."""

    def __init__(self, database_path: Path) -> None:
        if fcntl is None:
            raise RuntimeError("exclusive alpha consumer leases require a Unix host")
        self._path = Path(f"{database_path}.consumer.lock")
        if self._path.is_symlink():
            raise ValueError("alpha consumer lease must not be a symlink")
        flags = os.O_CREAT | os.O_RDWR
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        self._fd = os.open(self._path, flags, stat.S_IRUSR | stat.S_IWUSR)
        try:
            os.fchmod(self._fd, stat.S_IRUSR | stat.S_IWUSR)
            file_stat = os.fstat(self._fd)
            if not stat.S_ISREG(file_stat.st_mode):
                raise ValueError("alpha consumer lease must be a regular file")
            if hasattr(os, "getuid") and file_stat.st_uid != os.getuid():
                raise PermissionError("alpha consumer lease must be owned by the current user")
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            self.close()
            raise RuntimeError("another alpha signal consumer already holds the inbox") from error
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        fd = getattr(self, "_fd", -1)
        if fd < 0:
            return
        self._fd = -1
        if fcntl is not None:
            fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class AlphaSignalInbox:
    """SQLite-backed single-consumer inbox with at-most-once claiming."""

    def __init__(
        self,
        path: str | Path,
        policy: AlphaSignalPolicy,
        *,
        create: bool = True,
        busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
    ) -> None:
        self.path = _validate_private_database_path(Path(path), create=create)
        self.policy = policy
        self.policy.validate()
        if not 1 <= busy_timeout_ms <= 30_000:
            raise ValueError("busy_timeout_ms must be between 1 and 30000")
        self._connection = sqlite3.connect(
            self.path,
            timeout=busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute(f"PRAGMA busy_timeout={busy_timeout_ms}")
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._initialize_schema()
        os.chmod(self.path, stat.S_IRUSR | stat.S_IWUSR)

    def close(self) -> None:
        self._connection.close()

    def acquire_consumer_lease(self) -> InboxConsumerLease:
        """Acquire the process-held lease required before claiming signals."""
        return InboxConsumerLease(self.path)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def submit(self, signal: AlphaSignal, *, now_ns: int | None = None) -> SignalReceipt:
        now_ns = time.time_ns() if now_ns is None else now_ns
        signal.validate(self.policy, now_ns)
        request_hash = signal.request_hash()
        payload = json.dumps(
            {**asdict(signal), "action": signal.action.value},
            sort_keys=True,
            separators=(",", ":"),
        )

        self._connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self._connection.execute(
                "SELECT * FROM alpha_signals WHERE signal_id = ?",
                (signal.signal_id,),
            ).fetchone()
            if existing is not None:
                if existing["request_hash"] != request_hash:
                    raise ValueError("signal_id was reused with different content")
                self._connection.execute("COMMIT")
                return _receipt_from_row(existing)

            pending_count = self._connection.execute(
                "SELECT COUNT(*) FROM alpha_signals WHERE status = ?",
                (SignalStatus.PENDING.value,),
            ).fetchone()[0]
            if pending_count >= self.policy.max_pending_signals:
                raise RuntimeError("alpha signal inbox capacity exhausted")

            self._connection.execute(
                """
                INSERT INTO alpha_signals (
                    signal_id, request_hash, route_id, instrument_id, expires_at_ns,
                    status, payload_json, created_at_ns, updated_at_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.signal_id,
                    request_hash,
                    signal.route_id,
                    signal.instrument_id,
                    signal.expires_at_ns,
                    SignalStatus.PENDING.value,
                    payload,
                    now_ns,
                    now_ns,
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM alpha_signals WHERE signal_id = ?",
                (signal.signal_id,),
            ).fetchone()
            self._connection.execute("COMMIT")
        except BaseException:
            self._connection.execute("ROLLBACK")
            raise
        return _receipt_from_row(row)

    def receipt(self, signal_id: str) -> SignalReceipt | None:
        _require_safe_id("signal_id", signal_id)
        row = self._connection.execute(
            "SELECT * FROM alpha_signals WHERE signal_id = ?",
            (signal_id,),
        ).fetchone()
        return None if row is None else _receipt_from_row(row)

    def claim_next(
        self,
        route_id: str,
        instrument_id: str,
        *,
        now_ns: int | None = None,
    ) -> AlphaSignal | None:
        _require_safe_id("route_id", route_id)
        _require_safe_id("instrument_id", instrument_id)
        now_ns = time.time_ns() if now_ns is None else now_ns
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            self._connection.execute(
                """
                UPDATE alpha_signals
                SET status = ?, detail = 'expired before claim', updated_at_ns = ?
                WHERE status = ? AND expires_at_ns <= ?
                """,
                (
                    SignalStatus.EXPIRED.value,
                    now_ns,
                    SignalStatus.PENDING.value,
                    now_ns,
                ),
            )
            row = self._connection.execute(
                """
                SELECT * FROM alpha_signals
                WHERE status = ? AND route_id = ? AND instrument_id = ?
                ORDER BY created_at_ns, signal_id
                LIMIT 1
                """,
                (SignalStatus.PENDING.value, route_id, instrument_id),
            ).fetchone()
            if row is None:
                self._connection.execute("COMMIT")
                return None
            signal = _signal_from_json(row["payload_json"])
            try:
                signal.validate(self.policy, now_ns)
            except (ValueError, PermissionError):
                updated = self._connection.execute(
                    """
                    UPDATE alpha_signals
                    SET status = ?, detail = 'failed consumer admission policy', updated_at_ns = ?
                    WHERE signal_id = ? AND status = ?
                    """,
                    (
                        SignalStatus.REJECTED.value,
                        now_ns,
                        row["signal_id"],
                        SignalStatus.PENDING.value,
                    ),
                ).rowcount
                if updated != 1:
                    raise RuntimeError("invalid alpha signal could not be terminalized")
                self._connection.execute("COMMIT")
                return None
            updated = self._connection.execute(
                """
                UPDATE alpha_signals
                SET status = ?, updated_at_ns = ?
                WHERE signal_id = ? AND status = ?
                """,
                (
                    SignalStatus.CLAIMED.value,
                    now_ns,
                    row["signal_id"],
                    SignalStatus.PENDING.value,
                ),
            ).rowcount
            if updated != 1:
                raise RuntimeError("alpha signal claim lost its atomic admission")
            self._connection.execute("COMMIT")
        except BaseException:
            self._connection.execute("ROLLBACK")
            raise
        return signal

    def terminalize(
        self,
        signal_id: str,
        status: SignalStatus,
        *,
        detail: str | None = None,
        now_ns: int | None = None,
    ) -> SignalReceipt:
        _require_safe_id("signal_id", signal_id)
        if status not in TERMINAL_STATUSES:
            raise ValueError(f"status {status.value} is not terminal")
        if detail is not None and len(detail.encode()) > MAX_REASON_BYTES:
            raise ValueError("detail exceeds 1024 UTF-8 bytes")
        now_ns = time.time_ns() if now_ns is None else now_ns
        updated = self._connection.execute(
            """
            UPDATE alpha_signals
            SET status = ?, detail = ?, updated_at_ns = ?
            WHERE signal_id = ? AND status = ?
            """,
            (
                status.value,
                detail,
                now_ns,
                signal_id,
                SignalStatus.CLAIMED.value,
            ),
        ).rowcount
        if updated != 1:
            raise RuntimeError("signal is not in CLAIMED state")
        receipt = self.receipt(signal_id)
        if receipt is None:  # pragma: no cover - guarded by the update above
            raise RuntimeError("terminalized signal disappeared")
        return receipt

    def abandon_claimed(self, *, reason: str = "consumer restarted before terminal receipt") -> int:
        if len(reason.encode()) > MAX_REASON_BYTES:
            raise ValueError("reason exceeds 1024 UTF-8 bytes")
        now_ns = time.time_ns()
        return self._connection.execute(
            """
            UPDATE alpha_signals
            SET status = ?, detail = ?, updated_at_ns = ?
            WHERE status = ?
            """,
            (
                SignalStatus.ABANDONED.value,
                reason,
                now_ns,
                SignalStatus.CLAIMED.value,
            ),
        ).rowcount

    def _initialize_schema(self) -> None:
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS alpha_signals (
                signal_id TEXT PRIMARY KEY,
                request_hash TEXT NOT NULL,
                route_id TEXT NOT NULL,
                instrument_id TEXT NOT NULL,
                expires_at_ns INTEGER NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                detail TEXT,
                created_at_ns INTEGER NOT NULL,
                updated_at_ns INTEGER NOT NULL
            ) STRICT
            """,
        )
        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS alpha_signals_pending_route
            ON alpha_signals(status, route_id, instrument_id, created_at_ns)
            """,
        )


def _require_safe_id(field_name: str, value: str) -> None:
    if not isinstance(value, str) or not _SAFE_ID.fullmatch(value):
        raise ValueError(f"{field_name} must match {_SAFE_ID.pattern}")
    if len(value.encode()) > MAX_TEXT_BYTES:
        raise ValueError(f"{field_name} exceeds {MAX_TEXT_BYTES} UTF-8 bytes")


def _validate_private_database_path(path: Path, *, create: bool) -> Path:
    if not path.is_absolute():
        raise ValueError("alpha signal inbox path must be absolute")
    _validate_private_parent(path.parent)
    _validate_existing_database(path, create=create)
    return path


def _validate_private_parent(parent: Path) -> None:
    if not parent.exists() or not parent.is_dir():
        raise ValueError("alpha signal inbox parent directory must already exist")
    if parent.is_symlink():
        raise ValueError("alpha signal inbox parent directory must not be a symlink")
    parent_stat = parent.stat()
    if hasattr(os, "getuid") and parent_stat.st_uid != os.getuid():
        raise PermissionError("alpha signal inbox directory must be owned by the current user")
    if stat.S_IMODE(parent_stat.st_mode) & (stat.S_IRWXG | stat.S_IRWXO):
        raise PermissionError("alpha signal inbox directory must not grant group/world access")


def _validate_existing_database(path: Path, *, create: bool) -> None:
    if path.is_symlink():
        raise ValueError("alpha signal inbox must not be a symlink")
    if path.exists():
        if not path.is_file():
            raise ValueError("alpha signal inbox must be a regular non-symlink file")
        file_stat = path.stat()
        if hasattr(os, "getuid") and file_stat.st_uid != os.getuid():
            raise PermissionError("alpha signal inbox must be owned by the current user")
        if stat.S_IMODE(file_stat.st_mode) & (stat.S_IRWXG | stat.S_IRWXO):
            raise PermissionError("alpha signal inbox must not grant group/world access")
    elif not create:
        raise FileNotFoundError(path)


def _signal_from_json(payload: str) -> AlphaSignal:
    values = json.loads(payload)
    values["action"] = SignalAction(values["action"])
    return AlphaSignal(**values)


def _receipt_from_row(row: sqlite3.Row) -> SignalReceipt:
    return SignalReceipt(
        signal_id=row["signal_id"],
        status=SignalStatus(row["status"]),
        request_hash=row["request_hash"],
        created_at_ns=row["created_at_ns"],
        updated_at_ns=row["updated_at_ns"],
        detail=row["detail"],
    )
