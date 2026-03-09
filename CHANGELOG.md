# Changelog

## 2026.3.9

### Changes

- Add `trader/binance_api.py`: full Binance Spot exchange adapter (`BinanceClient`) using `binance-sdk-spot`, implementing the same interface as `HyperliquidClient` (`get_candles`, `get_balance`, `get_positions`, `place_order`, `cancel_order`, `cancel_all_orders`, etc.).
- Update `trader/exchanges/factory.py`: register `"binance"` venue; reads `binance.api_key` / `binance.api_secret` from config and constructs `BinanceClient`; when `--test` is passed, prefers `binance.testnet_api_key` / `testnet_api_secret` (Binance testnet requires keys generated at testnet.binance.vision, separate from mainnet keys).
- Update `trader/config.example.json`: add `binance` block with mainnet and testnet placeholder API key/secret fields.

### Fixes

- None noted for this date.

---

## 2026.2.5

### Changes

- Add CI workflow for relayer builds and RocksDB dependencies.
- Enhance Docker docs/config, add subscription by ETH address, and improve API error handling.
- Remove downstream forwarding functionality and related configurations.
- Implement subscription limits and token validation for relay management.
- Add settlement worker for trade settlement and credit tracking.
- Enhance Nostr integration and relay config (secret key support, bot registration with Nostr pubkey and ETH address, encrypted fanout handling), plus doc refresh.
- Implement event routing and processing with deduplication.
- Rename Hyperliquid Trading Bot to Moltrade Trading Bot and update feature descriptions.
- Add SKILL.md for Moltrade bot operations and configuration.
- Add signal broadcasting and trading strategies.
- Update README structure for clearer content.
- Update code structure and add initial README.

### Fixes

- None noted for this date.
