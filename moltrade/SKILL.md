---
name: moltrade
description: Operate the Moltrade trading bot (config, backtest, test-mode runs, Nostr signal broadcast, exchange adapters, strategy integration) in OpenClaw.
metadata:
  openclaw:
    emoji: "🤖"
    requires:
      bins: ["python", "pip"]
    homepage: https://github.com/hetu-project/moltrade.git
---

# Moltrade Bot Skill

Paths are repo-root relative. Keep actions deterministic and redact secrets.

## Inspect Config

- Read `trader/config.example.json` or `config.json` if present.
- Summarize `trading` (exchange/default_strategy/sizing/stops), `risk_management`, `nostr`, `telegram`.
- Do not log keys; redact `wallet_address`, `private_key`, `nostr.nsec`, `platform_shared_key`.

## Update Config Safely

- Backup or show planned diff before edits.
- Change only requested fields (e.g., `trading.exchange`, `trading.default_strategy`, `nostr.relays`).
- Validate JSON; keep types intact. Remind user to provide real secrets themselves.

## Run Backtest (local)

- Install deps: `pip install -r trader/requirements.txt`.
- Command: `python trader/backtest.py --config trader/config.example.json --strategy <name> --symbol <symbol> --interval 1h --limit 500`.
- Report PnL/win rate/trade count/drawdown if available. Use redacted config (no real keys).

## Start Bot (test mode)

- Ensure `config.json` exists and `trading.exchange` set (default hyperliquid).
- Command: `python trader/main.py --config config.json --test --strategy <name> --symbol <symbol> --interval 300`.
- Watch `trading_bot.log`; never switch to live without explicit user approval.

## Broadcast Signals to Nostr

- Check `nostr` block: `nsec`, `platform_shared_key`, `relays`, `sid`.
- `SignalBroadcaster` is wired in `main.py`. In test mode, verify `send_trade_signal` / `send_execution_report` run without errors.

## Add Exchange Adapter

- Implement adapter in `trader/exchanges/` matching `HyperliquidClient` interface (`get_candles`, `get_balance`, `get_positions`, `place_order`, etc.).
- Register in `trader/exchanges/factory.py` keyed by `trading.exchange`.
- Update config `trading.exchange` and rerun backtest/test-mode.

## Integrate New Strategy

- Follow `trader/strategies/INTEGRATION.md` to subclass `BaseStrategy` and register in `get_strategy`.
- Add config under `strategies.<name>`; backtest, then test-mode before live.

## Safety / Secrets

- Never print or commit private keys, mnemonics, nsec, or shared keys.
- Default to test mode; require explicit consent for live trading.
