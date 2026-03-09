"""
Exchange client factory.

Keeps the bot decoupled from a specific venue so we can add new
adapters (e.g., polymarket, other DEXes) without touching core logic.
"""

from typing import Any, Dict

from hyperliquid_api import HyperliquidClient
from binance_api import BinanceClient


def get_exchange_client(config: Dict[str, Any], *, test_mode: bool = False):
    trading_cfg = config.get("trading", {})
    venue = trading_cfg.get("exchange", "hyperliquid").lower()

    if venue == "hyperliquid":
        return HyperliquidClient(
            wallet_address=config["wallet_address"],
            private_key=config["private_key"],
            testnet=test_mode,
        )

    if venue == "binance":
        binance_cfg = config.get("binance", {})
        if test_mode:
            # Binance testnet (testnet.binance.vision) requires dedicated keys
            # separate from mainnet keys.  Fall back to mainnet keys only when
            # testnet-specific keys are not configured.
            api_key = binance_cfg.get("testnet_api_key") or binance_cfg.get("api_key", "")
            api_secret = binance_cfg.get("testnet_api_secret") or binance_cfg.get("api_secret", "")
        else:
            api_key = binance_cfg.get("api_key", "")
            api_secret = binance_cfg.get("api_secret", "")
        if not api_key or not api_secret:
            raise ValueError(
                "Binance requires 'binance.api_key' and 'binance.api_secret' in config."
            )
        return BinanceClient(
            api_key=api_key,
            api_secret=api_secret,
            testnet=test_mode,
        )

    raise ValueError(f"Unsupported exchange '{venue}'. Add an adapter in exchanges.factory.")
