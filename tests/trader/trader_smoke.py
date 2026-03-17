#!/usr/bin/env python3
"""
Lightweight smoke test for the Python trader SignalBroadcaster.
This exercises the disabled path (no nsec provided) to ensure imports/config parsing do not crash.
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from trader.nostr.signal_service import SignalBroadcaster  # noqa: E402


def main() -> None:
    cfg = {
        "nostr": {
            "nsec": "",  # keep disabled to avoid network work
            "relayer_nostr_pubkey": "",
            "sid": "smoke",
            "role": "bot",
            "relays": [],
            "relayer_api": None,
            "settlement_token": None,
        }
    }
    sb = SignalBroadcaster(cfg)
    assert not sb.enabled, "Broadcaster should be disabled without nsec"

    # Calls should no-op and return False when disabled
    assert sb.send_trade_signal(
        symbol="ETH-USDC",
        signal={"signal": "buy", "strength": 0.5, "price": 2500, "size": 1},
        strategy="demo",
        test_mode=True,
        account=None,
    ) is False

    assert sb.send_execution_report(
        symbol="ETH-USDC",
        side="buy",
        strategy="demo",
        size=1,
        price=2500,
        status="filled",
        tx_hash=None,
        pnl=None,
        pnl_percent=None,
        test_mode=True,
        account=None,
        note=None,
    ) is False

    print("[DONE] trader smoke tests passed")


if __name__ == "__main__":
    main()
