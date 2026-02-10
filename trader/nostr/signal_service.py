"""
Signal broadcasting helpers for the trading bot.

Responsibilities:
- Initialize the global Nostr publisher
- Encrypt payloads using DM (pynostr EncryptedDirectMessage)
- Publish trade signals, copy-trade intents, and execution reports
"""

import json
import logging
from typing import Any, Dict, Optional

from dataclasses import asdict
import requests
from pynostr.encrypted_dm import EncryptedDirectMessage
from pynostr.key import PrivateKey, PublicKey

from nostr.events import (
    TradeSignalEvent,
    CopyTradeIntentEvent,
    ExecutionReportEvent,
    AgentRegisterEvent,
    TradeSignalPayload,
    CopyTradeIntentPayload,
    ExecutionReportPayload,
    AgentRegisterPayload,
)
from nostr.publisher import init_global_publisher, get_publisher

logger = logging.getLogger(__name__)


class SignalBroadcaster:
    """Encrypt and publish trading signals to Nostr."""

    def __init__(self, config: Dict[str, Any]):
        nostr_cfg = config.get("nostr", {})
        self.nsec = nostr_cfg.get("nsec")
        self.enabled = bool(self.nsec)
        self.platform_key_raw = nostr_cfg.get("relayer_nostr_pubkey")
        self.sid = nostr_cfg.get("sid", "bot-main")
        self.role = nostr_cfg.get("role", "bot")
        self.relays = nostr_cfg.get("relays", [])
        self.relayer_api = config.get("relayer_api")
        self.settlement_token = nostr_cfg.get("settlement_token")

        if not self.enabled:
            logger.info("Nostr broadcasting disabled: missing nsec")
            self.publisher = None
            return

        self.recipient_pubkey_hex = self._derive_recipient_pubkey(self.platform_key_raw)
        if not self.recipient_pubkey_hex:
            logger.warning(
                "Nostr broadcasting disabled: relayer_nostr_pubkey must be npub/nsec/hex of relayer pubkey"
            )
            self.publisher = None
            self.enabled = False
            return

        init_global_publisher(
            nostr_cfg["nsec"],
            self.relays,
            sid=self.sid,
            role=self.role,
            listen_kinds=nostr_cfg.get("listen_kinds"),
        )
        self.publisher = get_publisher()
        if self.publisher is None:
            logger.warning("Nostr publisher not initialized; broadcasting disabled")
            self.enabled = False
        else:
            logger.info(
                "Nostr broadcaster ready (relays=%d, sid=%s, role=%s)",
                len(self.relays),
                self.sid,
                self.role,
            )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _publish(self, event) -> bool:
        if not self.enabled or self.publisher is None:
            return False
        try:
            self.publisher.publish_event(event)
            return True
        except Exception as exc:
            logger.warning("Failed to publish Nostr event: %s", exc)
            return False

    def _encrypt(self, payload: Dict[str, Any]) -> Optional[str]:
        if not self.nsec or not self.recipient_pubkey_hex:
            return None
        try:
            dm = EncryptedDirectMessage()
            dm.encrypt(
                PrivateKey.from_nsec(self.nsec).hex(),
                recipient_pubkey=self.recipient_pubkey_hex,
                cleartext_content=json.dumps(payload),
            )
            dm_event = dm.to_event()
            dm_event.sign(PrivateKey.from_nsec(self.nsec).hex())
            return dm_event.content
        except Exception as exc:
            logger.warning("Failed to encrypt payload via DM: %s", exc)
            return None

    @staticmethod
    def _derive_recipient_pubkey(platform_key_raw: Optional[str]) -> Optional[str]:
        if not platform_key_raw:
            return None
        try:
            if platform_key_raw.startswith("nsec"):
                priv = PrivateKey.from_nsec(platform_key_raw)
                return priv.public_key.hex()
            if platform_key_raw.startswith("npub"):
                if hasattr(PublicKey, "from_npub"):
                    return PublicKey.from_npub(platform_key_raw).hex()  # type: ignore[attr-defined]
                try:
                    from pynostr import nip19

                    hrp, data = nip19.decode(platform_key_raw)
                    if hrp == "npub" and isinstance(data, bytes):
                        return data.hex()
                except Exception:
                    pass
            # assume hex
            PublicKey.from_hex(platform_key_raw)  # validate
            return platform_key_raw
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Public APIs
    # ------------------------------------------------------------------
    def send_trade_signal(
        self,
        *,
        symbol: str,
        signal: Dict[str, Any],
        strategy: str,
        test_mode: bool,
        account: Optional[str],
    ) -> bool:
        if not self.enabled:
            return False

        payload = TradeSignalPayload(
            symbol=symbol,
            signal=signal.get("signal", "hold"),
            strength=float(signal.get("strength", 0)),
            price=float(signal.get("price", 0)),
            size=float(signal.get("size", 0)),
            strategy=strategy,
            test_mode=test_mode,
            account=account,
            indicators=signal.get("indicators"),
            note=signal.get("note"),
        )
        encrypted = self._encrypt(asdict(payload))
        if not encrypted:
            return False

        event = TradeSignalEvent.build(
            sid=self.sid,
            encrypted_content=encrypted,
            symbol=symbol,
            strategy=strategy,
            signal=payload.signal,
            test_mode=test_mode,
        )
        return self._publish(event)

    def send_copytrade_intent(
        self,
        *,
        follower_pubkey: str,
        symbol: Optional[str],
        max_slippage_pct: Optional[float],
        size_pct: Optional[float],
        note: Optional[str] = None,
    ) -> bool:
        if not self.enabled:
            return False

        payload = CopyTradeIntentPayload(
            follower_pubkey=follower_pubkey,
            symbol=symbol,
            max_slippage_pct=max_slippage_pct,
            size_pct=size_pct,
            note=note,
        )
        encrypted = self._encrypt(asdict(payload))
        if not encrypted:
            return False

        event = CopyTradeIntentEvent.build(
            sid=self.sid,
            encrypted_content=encrypted,
            follower_pubkey=follower_pubkey,
            symbol=symbol,
        )
        return self._publish(event)

    def send_execution_report(
        self,
        *,
        symbol: str,
        side: str,
        size: float,
        price: float,
        status: str,
        tx_hash: Optional[str],
        pnl: Optional[float],
        pnl_percent: Optional[float],
        test_mode: bool,
        account: Optional[str] = None,
        note: Optional[str] = None,
        oid: Optional[str] = None,
        follower_pubkey: Optional[str] = None,
    ) -> bool:
        if not self.enabled:
            return False

        payload = ExecutionReportPayload(
            symbol=symbol,
            side=side,
            size=size,
            price=price,
            status=status,
            tx_hash=tx_hash,
            pnl=pnl,
            pnl_percent=pnl_percent,
            test_mode=test_mode,
            account=account,
            note=note or oid,
        )
        encrypted = self._encrypt(asdict(payload))
        if not encrypted:
            return False

        event = ExecutionReportEvent.build(
            sid=self.sid,
            encrypted_content=encrypted,
            symbol=symbol,
            side=side,
            status=status,
            test_mode=test_mode,
        )
        published = self._publish(event)

        if self.relayer_api and self.publisher is not None:
            self._report_trade_tx(
                account=account,
                tx_hash=tx_hash,
                symbol=symbol,
                side=side,
                size=size,
                price=price,
                role=self.role,
                follower_pubkey=follower_pubkey,
                oid=oid,
            )

        return published

    def send_agent_register(
        self,
        *,
        bot_pubkey: str,
        nostr_pubkey: str,
        eth_address: str,
        name: str,
    ) -> bool:
        if not self.enabled or self.publisher is None:
            return False

        payload = AgentRegisterPayload(
            bot_pubkey=bot_pubkey,
            nostr_pubkey=nostr_pubkey,
            eth_address=eth_address,
            name=name,
        )

        event = AgentRegisterEvent.build(sid=self.sid, content=payload)
        return self._publish(event)

    def _report_trade_tx(
        self,
        *,
        account: Optional[str],
        tx_hash: Optional[str],
        symbol: str,
        side: str,
        size: float,
        price: float,
        role: str,
        follower_pubkey: Optional[str] = None,
        oid: Optional[str] = None,
    ) -> None:
        if not account:
            logger.warning("Failed to report trade tx: missing account/wallet for bot_pubkey")
            return
        db_role = "leader" if role.lower() in ("bot", "leader") else "follower"
        url = f"{self.relayer_api.rstrip('/')}/api/trades/record"
        headers = {"Content-Type": "application/json"}
        if self.settlement_token:
            headers["X-Settlement-Token"] = self.settlement_token

        payload = {
            "bot_pubkey": account,
            "follower_pubkey": follower_pubkey,
            "role": db_role,
            "symbol": symbol,
            "side": side,
            "size": size,
            "price": price,
            "tx_hash": tx_hash,
            "oid": oid,
        }

        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=5)
            if resp.status_code >= 300:
                logger.warning(
                    "Failed to report trade tx (status=%s): %s", resp.status_code, resp.text
                )
        except Exception as exc:
            logger.warning("Failed to report trade tx to relayer: %s", exc)


__all__ = ["SignalBroadcaster"]
