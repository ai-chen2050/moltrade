# 🦉 Moltrade — The Automated AI Trading Assistant

<center>

<div align="center">
    <picture>
        <source media="(prefers-color-scheme: light)" srcset="./assets/moltrade-black.png">
        <img src="./assets/moltrade-white.png" alt="Moltrade" width="600">
    </picture>

<div style="text-align: center; font-weight: bold;">
<p align="center">
<strong>YOUR 24/7 AI TRADER ! EARNING MONEY WHILE YOU'RE SLEEPING.</strong>
</p>

</div>
</div>
</center>

---

## **Overview**

**Moltrade** is a decentralized, automated trading assistant that lets you run quant strategies, share encrypted signals, and allow others to copy your trades—all securely via the Nostr network. Earn reputation and credits based on your trading performance.

## Core Advantages

**Moltrade** balances security, usability, and scalability. Key advantages include:

- **Client-side Key self-hosting,not cloud Custody,**: All sensitive keys and credentials remain on the user's machine; the cloud relay never holds funds or private keys, minimizing custodial risk.**No access to private keys or funds.**
- **Encrypted, Targeted Communication**: Signals are encrypted before publishing and only decryptable by intended subscribers, preserving strategy privacy and subscriber security.
- **Lightweight Cloud Re-encryption & Broadcast**: The cloud acts as an efficient relay/re-broadcaster without storing private keys; re-encryption or forwarding techniques improve delivery reliability and reach.
- **One-Click Copy Trading (User Friendly)**: Provides an out-of-the-box copy-trading experience for non-expert users—set up in a few steps and execute signals locally.
- **OpenClaw Strategy Advisor**: Integrates OpenClaw as an advisory tool for automated backtests and improvement suggestions; users decide whether to adopt recommended changes.
- **Local Hot-Reload of Profitable Strategies**: Strategy code and parameters can be iterated locally and take effect immediately, so performance improvements are under user control.
- **Cloud Can Be Decentralized Relayer Network**: The lightweight relay architecture allows future migration to decentralized relay networks, reducing single points of failure and improving censorship resistance.
- **Unified Incentive (Credit) System**: A transparent, verifiable Credit mechanism rewards all participants (signal providers, followers, relay nodes), aligning incentives across the ecosystem. 

## **How It Works (Simplified Flow)**

<div align="center">
<table cellpadding="8" cellspacing="0" align="center">
    <tr>
        <td align="center" style="border:1px solid #ccc; font-family:monospace; padding:12px; max-width:640px;">
            <strong>1) Run Your Bot</strong><br/>- Clone, configure local bot with your Hyperliquid keys
        </td>
    </tr>
    <tr><td align="center" style="font-size:20px;">↓</td></tr>
    <tr>
        <td align="center" style="border:1px solid #ccc; font-family:monospace; padding:12px; max-width:640px;">
            <strong>2) Generate & Encrypt</strong><br/>- Bot creates trade signal, encrypts for subscribers
        </td>
    </tr>
    <tr><td align="center" style="font-size:20px;">↓</td></tr>
    <tr>
        <td align="center" style="border:1px solid #ccc; font-family:monospace; padding:12px; max-width:640px;">
            <strong>3) Relay</strong><br/>- Moltrade relayer (super node) picks up and re-broadcasts
        </td>
    </tr>
    <tr><td align="center" style="font-size:20px;">↓</td></tr>
    <tr>
        <td align="center" style="border:1px solid #ccc; font-family:monospace; padding:12px; max-width:640px;">
            <strong>4) Copy & Execute</strong><br/>- Subscribers decrypt signal and execute trades locally
        </td>
    </tr>
    <tr><td align="center" style="font-size:20px;">↓</td></tr>
    <tr>
        <td align="center" style="border:1px solid #ccc; font-family:monospace; padding:12px; max-width:640px;">
            <strong>5) Verify & Earn</strong><br/>- Bots submit tx hash → on-chain verify → update credits
        </td>
    </tr>
</table>
</div>

## **Getting Started**

**Prerequisites**

- Python 3.10+
- A Hyperliquid account (Testnet recommended for initial use)
- A Nostr key pair (generated automatically by the bot)

**Installation & Setup**

1.  **Clone & Install**:
    ```bash
    git clone https://your-repo-url/moltrade-bot.git
    cd moltrade-bot
    pip install -r requirements.txt  # Installs nostr, hyperliquid-py, etc.
    ```
2.  **Configure Your Secrets**:
    - Copy `config.example.json` to `config.json`.
    - **Fill in your Hyperliquid account details (API keys and wallet)**.
    - **Important**: Never commit this file or share your private keys.
3.  **Run the Bot**:
    ```bash
    python main.py
    ```
    The bot will start, generate a Nostr key pair for communication, and await configuration via its local API or admin panel.

## **Docker Setup**

**For Relayer:**
```bash
cd docker && docker compose -f docker-compose.relayer.yml up --build
```

**For Trader:**
```bash
cd docker && docker compose -f docker-compose.trader.yml up
```

## **Architecture & Integration with Your Ecosystem**

- **Core (MVP)**: This `moltrade-bot` and the cloud relay form the self-sustaining MVP.
- **OpenClaw/MoltBot (Future)**: In the future, you can use OpenClaw or MoltBot as an **external advisor**. You can manually ask it to review or optimize your strategy logic, then update your bot's `config.json`. This keeps the core trading system simple and robust.
- **Prakasa Network (Roadmap)**: For users who don't want to run their own machine, future versions could offer to host this bot on the decentralized Prakasa compute network.

## **Important Disclaimer**

Trading cryptocurrencies and derivatives carries significant risk. Moltrade is a tool for automation and social trading. You are solely responsible for any financial losses. Past performance is not indicative of future results.
