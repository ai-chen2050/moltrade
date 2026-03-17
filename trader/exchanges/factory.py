"""
Exchange client factory.

Keeps the bot decoupled from a specific venue so we can add new
adapters (e.g., polymarket, other DEXes) without touching core logic.
"""

from typing import Any, Dict

import sys
from pathlib import Path
# Add trader to path so factory can import from trader root if called from outside
sys.path.append(str(Path(__file__).resolve().parent.parent))

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

    if venue == "uniswap":
        uni_cfg = config.get("uniswap", {})
        rpc_url = uni_cfg.get("rpc_url")
        private_key = uni_cfg.get("private_key")
        if not rpc_url or not private_key:
            raise ValueError(
                "Uniswap requires 'uniswap.rpc_url' and 'uniswap.private_key' in config."
            )
        from exchanges.uniswap_api import UniswapClient
        return UniswapClient(
            rpc_url=rpc_url,
            private_key=private_key,
            chain_id=uni_cfg.get("chain_id", 1),
            router_address=uni_cfg.get("router_address", "0xE592427A0AEce92De3Edee1F18E0157C05861564"),
            slippage_tolerance=uni_cfg.get("slippage_tolerance", 0.005),
            default_token_in=uni_cfg.get("default_token_in", "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
            default_token_out=uni_cfg.get("default_token_out", "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
        )

    if venue == "polymarket":
        poly_cfg = config.get("polymarket", {})
        api_key = poly_cfg.get("api_key")
        api_secret = poly_cfg.get("api_secret")
        api_passphrase = poly_cfg.get("api_passphrase", "") # Sometimes optional
        private_key = poly_cfg.get("private_key")
        
        if not api_key or not api_secret or not private_key:
            raise ValueError(
                "Polymarket requires 'polymarket.api_key', 'polymarket.api_secret', and 'polymarket.private_key' in config."
            )
        from exchanges.polymarket_api import PolymarketClient
        return PolymarketClient(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
            private_key=private_key,
            chain_id=poly_cfg.get("chain_id", 137)
        )

    raise ValueError(f"Unsupported exchange '{venue}'. Add an adapter in exchanges.factory.")
