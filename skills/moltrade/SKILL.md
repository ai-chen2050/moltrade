---
name: moltrade
description: **Moltrade** is a decentralized, automated trading assistant that lets you run quant strategies, share encrypted signals, and allow others to copy your trades—all securely via the Nostr network. Earn reputation and credits based on your trading performance.
metadata:
  openclaw:
    emoji: "🤖"
    requires:
      bins: ["python", "pip"]
    homepage: https://github.com/hetu-project/moltrade.git
---

# Moltrade Bot Skill

Operate the Moltrade trading bot (config, backtest, test-mode runs, Nostr signal broadcast, exchange adapters, strategy integration) in OpenClaw.

<center>

<div align="center">
    <picture>
        <source media="(prefers-color-scheme: light)" srcset="../../assets/moltrade-black.png">
        <img src="../../assets/moltrade-white.png" alt="Moltrade" width="600">
    </picture>

<div style="text-align: center; font-weight: bold;">
<p align="center">
<strong>YOUR 24/7 AI TRADER ! EARNING MONEY WHILE YOU'RE SLEEPING.</strong>
</p>

[![Twitter Follow](https://img.shields.io/twitter/follow/hetu_protocol?style=social&label=Follow)](https://x.com/hetu_protocol)
[![Telegram](https://img.shields.io/badge/Telegram-Hetu_Builders-blue)](https://t.me/+uJrRgjtSsGw3MjZl)
[![ClawHub](https://img.shields.io/badge/ClawHub-Read-orange)](https://clawhub.ai/ai-chen2050/moltrade)
[![Website](https://img.shields.io/badge/Website-moltrade.ai-green)](https://www.moltrade.ai/)

</div>
</div>
</center>

---

## Core Features

**Moltrade** balances security, usability, and scalability. Key advantages include:

- **Client-side Key self-hosting,not cloud Custody,**: All sensitive keys and credentials remain on the user's machine; the cloud relay never holds funds or private keys, minimizing custodial risk.**No access to private keys or funds.**
- **Encrypted, Targeted Communication**: Signals are encrypted before publishing and only decryptable by intended subscribers, preserving strategy privacy and subscriber security.
- **Lightweight Cloud Re-encryption & Broadcast**: The cloud acts as an efficient relay/re-broadcaster without storing private keys; re-encryption or forwarding techniques improve delivery reliability and reach.
- **One-Click Copy Trading (User Friendly)**: Provides an out-of-the-box copy-trading experience for non-expert users—set up in a few steps and execute signals locally.
- **OpenClaw Strategy Advisor**: Integrates OpenClaw as an advisory tool for automated backtests and improvement suggestions; users decide whether to adopt recommended changes.
- **Cloud Can Be Decentralized Relayer Network**: The lightweight relay architecture allows future migration to decentralized relay networks, reducing single points of failure and improving censorship resistance.
- **Unified Incentive (Credit) System**: A transparent, verifiable Credit mechanism rewards all participants (signal providers, followers, relay nodes), aligning incentives across the ecosystem.

## **How It Works (Simplified Flow)**

```mermaid
graph LR
    A["1) Run Your Bot"]
    B["2) Generate & Encrypt"]
    C["3) Relay"]
    D["4) Copy & Execute"]
    E["5) Verify & Earn"]

    A ---> B
    B ---> C
    C ---> D
    D ---> E
```

## Install & Init

- If you are inside **OpenClaw**, you can install directly via ClawHub:

```bash
clawhub search moltrade
clawhub install moltrade
```

- OR & Clone the repo and install Python deps locally (code is required for strategies, nostr, and CLI):
  - `git clone https://github.com/hetu-project/moltrade.git`
  - `cd moltrade/trader && pip install -r requirements.txt`
- Initialize a fresh config with the built-in wizard (no trading):
  - Prefer the human user to run `python main.py --init` (prompts for relayer URL, wallet, nostr, copy-trade follower defaults, and bot registration), so you can approve prompts, handle the wallet private key entry yourself, and capture the relayer’s returned `relayer_nostr_pubkey` when registering the bot.
  - If you delegate to an agent, do so only if you trust it with the wallet key and ensure it completes the entire wizard—including the final bot registration step—so the `relayer_nostr_pubkey` gets written back to the config.
- For CI/agents, keep using the repo checkout; there is no separate pip package/CLI yet.

## Update Config Safely

- Backup or show planned diff before edits.
- Change only requested fields (e.g., `trading.exchange`, `trading.default_strategy`, `nostr.relays`).
- Validate JSON; keep types intact. Remind user to provide real secrets themselves.

## Run Backtest (local)

- Install deps: `pip install -r trader/requirements.txt`.
- Command: `python trader/backtest.py --config trader/config.example.json --strategy <name> --symbol <symbol> --interval 1h --limit 500`.
- Report PnL/win rate/trade count/drawdown if available. Use redacted config (no real keys).

## Start Bot (test mode)

- Ensure `config.json` exists (run `python main.py --init` if not) and `trading.exchange` set (default hyperliquid).
- Command: `python trader/main.py --config config.json --test --strategy <name> --symbol <symbol> --interval 300`.
- Watch `trading_bot.log`; never switch to live without explicit user approval.

## Run Bot (live)

- Only after validation on test mode; remove `--test` to hit mainnet.
- Command: `python trader/main.py --config config.json --strategy <name> --symbol <symbol>`.
- Double-check keys, risk limits, and symbol before starting; live mode will place real orders.

## Broadcast Signals to Nostr

- Check `nostr` block: `nsec`, `relayer_nostr_pubkey`, `relays`, `sid`.
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

## Disclaimer

Trading cryptocurrencies and derivatives carries significant risk. Moltrade is a tool for automation and social trading. You are solely responsible for any financial losses. Past performance is not indicative of future results.