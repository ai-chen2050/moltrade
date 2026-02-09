# Moltrade Trading Bot

Hyperliquid by default, but the bot is built to support additional venues (e.g., Polymarket or other DEXs) via the exchange factory.

## Features

- Multi-strategy: momentum, mean reversion, grid (plus pluggable strategies)
- Multi-venue ready: exchange factory defaults to Hyperliquid, can extend via `trader/exchanges/`
- Risk controls: stop-loss/take-profit, position sizing, trade cooldowns
- Notifications: Telegram, optional encrypted Nostr signal broadcast
- Test mode for dry runs; trade logging and history

## Install

If you are inside **OpenClaw**, you can install directly via ClawHub:

```bash
clawhub search moltrade
clawhub install moltrade
```

Otherwise, clone the repo and install dependencies:
```bash
git clone https://github.com/hetu-project/moltrade.git
cd trader
pip install -r requirements.txt
```

## Configure

1. Copy the example:

```bash
cp config.example.json config.json
```

2. Edit `config.json`:

- `trading.exchange`: `hyperliquid` (default) or another key you add in `exchanges/factory.py`
- `wallet_address`, `private_key`: required for Hyperliquid (keep safe)
- `trading.default_strategy`, `position_size`, stops
- Optional: `telegram` block for alerts; `nostr` block for encrypted signal broadcast
- Copy-trade follower: set `copytrade.enabled=true`, `copytrade.role="follower"`, and `copytrade.follow_pubkeys=["<leader_eth_address>"]`. You can copy the leader ETH address from the leaderboard on https://moltrade.ai/.

### Telegram (optional)

```bash
python3 telegram_setup.py
```

Follow prompts; see TELEGRAM_SETUP.md for details.

## Run

```bash
# Test mode (no real trades)
python main.py --config config.json --test --strategy momentum --symbol HYPE

# Live trading (be sure keys/risks are set)
python main.py --config config.json --strategy momentum --symbol HYPE
```

## Backtest

```bash
python backtest.py --config config.example.json --strategy momentum --symbol HYPE --interval 1h --limit 500
```

## Strategies (built-in)

- Mean Reversion (`mean_reversion`): Bollinger Bands based
- Momentum (`momentum`): RSI + MACD
- Grid (`grid`): laddered buy/sell bands

To add your own, see `strategies/INTEGRATION.md` and register it in `get_strategy`.

## Exchanges

- Default: Hyperliquid (wallet address + private key)
- Extend: add an adapter in `exchanges/` implementing `get_candles`, `get_balance`, `get_positions`, `place_order`, then register in `exchanges/factory.py` and set `trading.exchange` in config.

## Nostr Signals (optional)

- Requires `nostr` config (`nsec`, `relayer_nostr_pubkey`, `relays`).
- Bot broadcasts encrypted trade signals and execution reports via `SignalBroadcaster` (already wired in `main.py`).

## Risk Warning

Crypto trading is risky. Use test mode first, start small, and never expose private keys or nsec/shared keys in logs or chat.
