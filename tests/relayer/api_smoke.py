#!/usr/bin/env python3
"""
Simple REST smoke tests for the relayer.
Set RELAYER_BASE_URL (default http://localhost:8080) and optionally RELAYER_SETTLEMENT_TOKEN.
Optional trade exercise: set RELAYER_TEST_TX_HASH plus RELAYER_TEST_BOT_PK; follower/role/symbol/side/size/price optional.
"""
import json
import os
import sys
from typing import Any, Dict

import requests

BASE_URL = os.getenv("RELAYER_BASE_URL", "http://localhost:8080").rstrip("/")
SETTLEMENT_TOKEN = os.getenv("RELAYER_SETTLEMENT_TOKEN")
TIMEOUT = float(os.getenv("RELAYER_TIMEOUT", "5"))

# Demo bot identities (for eth-based bots, bot_pubkey == eth_address)
LEADER_ETH = os.getenv("RELAYER_LEADER_ETH_ADDRESS", "0xc8c160905c71f2b3ee5de2e6bb597b596b05a3d4")
FOLLOWER_ETH = os.getenv("RELAYER_FOLLOWER_ETH_ADDRESS", "0xf2b3ee5de2e6bb597b596b05a3d4c8c160905c71")
LEADER_NOSTR = os.getenv("RELAYER_LEADER_NOSTR_PUB", "npub1leaderdemo")
FOLLOWER_NOSTR = os.getenv("RELAYER_FOLLOWER_NOSTR_PUB", "npub1followdemo")
SUB_SHARED_SECRET = os.getenv("RELAYER_SUB_SHARED_SECRET", "shared_secret_demo")


def call(method: str, path: str, *, include_token: bool = False, **kwargs):
    url = f"{BASE_URL}{path}"
    headers: Dict[str, str] = kwargs.pop("headers", {})
    if include_token and SETTLEMENT_TOKEN:
        headers.setdefault("X-Settlement-Token", SETTLEMENT_TOKEN)
    try:
        resp = requests.request(method, url, headers=headers, timeout=TIMEOUT, **kwargs)
        return resp
    except Exception as exc:
        print(f"[FAIL] {method.upper()} {url} error: {exc}")
        sys.exit(1)


def expect_ok(resp, label: str) -> None:
    if resp.status_code >= 300:
        body = resp.text
        print(f"[FAIL] {label}: status={resp.status_code}, body={body}")
        sys.exit(1)
    print(f"[OK] {label}: status={resp.status_code}")


def smoke_core() -> None:
    expect_ok(call("get", "/health"), "health")
    expect_ok(call("get", "/status"), "status")
    expect_ok(call("get", "/api/metrics/summary"), "metrics summary")
    expect_ok(call("get", "/api/metrics/memory"), "memory")
    expect_ok(call("get", "/api/credits"), "credits")


def register_bots_and_subscription() -> None:
    # Register leader bot
    leader_payload = {
        "bot_pubkey": LEADER_ETH,
        "nostr_pubkey": LEADER_NOSTR,
        "eth_address": LEADER_ETH,
        "name": "leader-bot",
    }
    resp = call(
        "post",
        "/api/bots/register",
        headers={"Content-Type": "application/json"},
        data=json.dumps(leader_payload),
    )
    expect_ok(resp, "register leader bot")

    # Register follower bot
    follower_payload = {
        "bot_pubkey": FOLLOWER_ETH,
        "nostr_pubkey": FOLLOWER_NOSTR,
        "eth_address": FOLLOWER_ETH,
        "name": "follower-bot",
    }
    resp = call(
        "post",
        "/api/bots/register",
        headers={"Content-Type": "application/json"},
        data=json.dumps(follower_payload),
    )
    expect_ok(resp, "register follower bot")

    # Follower subscribes to leader
    sub_payload = {
        "bot_pubkey": LEADER_ETH,
        "follower_pubkey": FOLLOWER_ETH,
        "shared_secret": SUB_SHARED_SECRET,
    }
    resp = call(
        "post",
        "/api/subscriptions",
        headers={"Content-Type": "application/json"},
        data=json.dumps(sub_payload),
    )
    expect_ok(resp, "subscription follower->leader")


def maybe_exercise_trade() -> None:
    tx_hash = os.getenv("RELAYER_TEST_TX_HASH")
    bot_pk = os.getenv("RELAYER_TEST_BOT_PK", LEADER_ETH)
    follower_pk = os.getenv("RELAYER_TEST_FOLLOWER_PK", FOLLOWER_ETH)

    if not tx_hash:
        print("[SKIP] trade record/settlement (set RELAYER_TEST_TX_HASH to run)")
        return

    role = os.getenv("RELAYER_TEST_ROLE", "leader")
    symbol = os.getenv("RELAYER_TEST_SYMBOL", "ETH-USDC")
    side = os.getenv("RELAYER_TEST_SIDE", "buy")
    size = float(os.getenv("RELAYER_TEST_SIZE", "1.0"))
    price = float(os.getenv("RELAYER_TEST_PRICE", "2500"))

    record_payload: Dict[str, Any] = {
        "bot_pubkey": bot_pk,
        "follower_pubkey": follower_pk,
        "role": role,
        "symbol": symbol,
        "side": side,
        "size": size,
        "price": price,
        "tx_hash": tx_hash,
    }

    resp = call(
        "post",
        "/api/trades/record",
        include_token=True,
        headers={"Content-Type": "application/json"},
        data=json.dumps(record_payload),
    )
    expect_ok(resp, "record trade")

    settlement_payload = {
        "tx_hash": tx_hash,
        "status": "confirmed",
        "pnl": float(os.getenv("RELAYER_TEST_PNL", "0")),
        "pnl_usd": float(os.getenv("RELAYER_TEST_PNL_USD", "0")),
    }
    resp = call(
        "post",
        "/api/trades/settlement",
        include_token=True,
        headers={"Content-Type": "application/json"},
        data=json.dumps(settlement_payload),
    )
    expect_ok(resp, "settlement update")


def main() -> None:
    print(f"Relayer base URL: {BASE_URL}")
    smoke_core()
    register_bots_and_subscription()
    maybe_exercise_trade()
    print("[DONE] relayer smoke tests passed")


if __name__ == "__main__":
    main()
