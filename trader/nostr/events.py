"""
Nostr events used by the trading bot MVP.

Each event is a thin wrapper around `pynostr.event.Event` with stable kinds
and predictable tags to keep parsing simple on the platform side.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

from pynostr.event import Event


# Kind reservations for trading bot events
TRADE_SIGNAL_KIND = 30931
COPYTRADE_INTENT_KIND = 30932
HEARTBEAT_KIND = 30933
EXECUTION_REPORT_KIND = 30934
AGENT_REGISTER_KIND = 30935

DEFAULT_VERSION = "v1"


def _compact_json(data: Dict[str, Any]) -> str:
    """Serialize to compact JSON for nostr content."""
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


class BotEvent(Event):
    """Base event that adds common tags such as sid and version."""

    def __init__(
        self,
        *,
        kind: int,
        content: str,
        sid: str,
        tags: Optional[List[List[str]]] = None,
        version: str = DEFAULT_VERSION,
    ) -> None:
        base_tags: List[List[str]] = [
            ["d", "subspace_op"],
            ["sid", sid],
            ["ver", version],
        ]
        if tags:
            base_tags.extend(tags)
        super().__init__(content=content, kind=kind, tags=base_tags)


# === Payload helpers ===


@dataclass
class TradeSignalPayload:
    symbol: str
    signal: str
    strength: float
    price: float
    size: float
    strategy: str
    test_mode: bool = False
    account: Optional[str] = None
    indicators: Optional[Dict[str, Any]] = None
    note: Optional[str] = None


@dataclass
class CopyTradeIntentPayload:
    follower_pubkey: str
    symbol: Optional[str] = None
    max_slippage_pct: Optional[float] = None
    size_pct: Optional[float] = None
    note: Optional[str] = None


@dataclass
class ExecutionReportPayload:
    symbol: str
    side: str
    strategy: Optional[str]
    size: float
    price: float
    status: str
    tx_hash: Optional[str] = None
    pnl: Optional[float] = None
    pnl_percent: Optional[float] = None
    test_mode: bool = False
    account: Optional[str] = None
    note: Optional[str] = None


@dataclass
class AgentRegisterPayload:
    bot_pubkey: str
    nostr_pubkey: str
    eth_address: str
    name: str


@dataclass
class HeartbeatPayload:
    status: str
    balance: Optional[float] = None
    open_positions: Optional[int] = None


# === Concrete event classes ===


class TradeSignalEvent(BotEvent):
    """Encrypted trading signal from the bot to the platform."""

    KIND = TRADE_SIGNAL_KIND
    OP = "trade_signal"

    @classmethod
    def build(
        cls,
        *,
        sid: str,
        encrypted_content: str,
        symbol: str,
        strategy: str,
        signal: str,
        test_mode: bool = False,
    ) -> "TradeSignalEvent":
        tags: List[List[str]] = [
            ["op", cls.OP],
            ["symbol", symbol],
            ["strategy", strategy],
            ["signal", signal],
            ["test", "1" if test_mode else "0"],
        ]
        return cls(kind=cls.KIND, content=encrypted_content, sid=sid, tags=tags)


class CopyTradeIntentEvent(BotEvent):
    """Follower declares subscription or updates copy-trade preferences."""

    KIND = COPYTRADE_INTENT_KIND
    OP = "copytrade_intent"

    @classmethod
    def build(
        cls,
        *,
        sid: str,
        encrypted_content: str,
        follower_pubkey: str,
        symbol: Optional[str] = None,
    ) -> "CopyTradeIntentEvent":
        tags: List[List[str]] = [["op", cls.OP], ["p", follower_pubkey]]
        if symbol:
            tags.append(["symbol", symbol])
        return cls(kind=cls.KIND, content=encrypted_content, sid=sid, tags=tags)


class ExecutionReportEvent(BotEvent):
    """Report fills or simulated executions back to the platform."""

    KIND = EXECUTION_REPORT_KIND
    OP = "execution_report"

    @classmethod
    def build(
        cls,
        *,
        sid: str,
        encrypted_content: str,
        symbol: str,
        side: str,
        status: str,
        test_mode: bool = False,
    ) -> "ExecutionReportEvent":
        tags: List[List[str]] = [
            ["op", cls.OP],
            ["symbol", symbol],
            ["side", side],
            ["status", status],
            ["test", "1" if test_mode else "0"],
        ]
        return cls(kind=cls.KIND, content=encrypted_content, sid=sid, tags=tags)


class HeartbeatEvent(BotEvent):
    """Lightweight health ping so the platform knows the bot is alive."""

    KIND = HEARTBEAT_KIND
    OP = "heartbeat"

    @classmethod
    def build(
        cls,
        *,
        sid: str,
        content: HeartbeatPayload,
    ) -> "HeartbeatEvent":
        json_content = _compact_json(asdict(content))
        tags: List[List[str]] = [["op", cls.OP], ["status", content.status]]
        return cls(kind=cls.KIND, content=json_content, sid=sid, tags=tags)


class AgentRegisterEvent(BotEvent):
    """Plaintext agent registration event so the relayer can auto-upsert the bot."""

    KIND = AGENT_REGISTER_KIND
    OP = "agent_register"

    @classmethod
    def build(
        cls,
        *,
        sid: str,
        content: AgentRegisterPayload,
    ) -> "AgentRegisterEvent":
        json_content = _compact_json(asdict(content))
        tags: List[List[str]] = [
            ["op", cls.OP],
            ["p", content.nostr_pubkey],
            ["eth", content.eth_address],
            ["name", content.name],
        ]
        return cls(kind=cls.KIND, content=json_content, sid=sid, tags=tags)


__all__ = [
    "BotEvent",
    "TradeSignalEvent",
    "CopyTradeIntentEvent",
    "ExecutionReportEvent",
    "HeartbeatEvent",
    "AgentRegisterEvent",
    "TradeSignalPayload",
    "CopyTradeIntentPayload",
    "ExecutionReportPayload",
    "HeartbeatPayload",
    "AgentRegisterPayload",
    "TRADE_SIGNAL_KIND",
    "COPYTRADE_INTENT_KIND",
    "EXECUTION_REPORT_KIND",
    "HEARTBEAT_KIND",
    "AGENT_REGISTER_KIND",
]


