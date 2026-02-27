"""
Improved Trading Bot - Enhanced Risk Management
"""
import argparse
import json
import logging
import os
import secrets
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import requests

from exchanges.factory import get_exchange_client
from strategies.strategies import get_strategy
from telegram_notifier import get_notifier
from nostr.signal_service import SignalBroadcaster
from nostr.copytrade_listener import CopyTradeListener
from pynostr.key import PrivateKey, PublicKey

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trading_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def _pubkey_to_hex(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    try:
        if raw.startswith('nsec'):
            priv = PrivateKey.from_nsec(raw)
            return priv.public_key.hex()
        if raw.startswith('npub'):
            if hasattr(PublicKey, 'from_npub'):
                return PublicKey.from_npub(raw).hex()  # type: ignore[attr-defined]
            try:
                from pynostr import nip19

                hrp, data = nip19.decode(raw)
                if hrp == 'npub' and isinstance(data, bytes):
                    return data.hex()
            except Exception:
                pass
        PublicKey.from_hex(raw)
        return raw
    except Exception:
        logger.warning("Invalid relayer nostr pubkey; expected npub/nsec/hex", exc_info=False)
        return None


class TradingBot:
    """Improved Trading Bot - Focused on optimizing risk management"""

    def __init__(self, config_path: str = "config.json", test_mode: bool = False, strategy_name: Optional[str] = None):
        """Initialize the trading bot"""
        self.test_mode = test_mode
        self.config = self._load_config(config_path)

        # Initialize exchange client (defaults to hyperliquid, extendable via config)
        self.client = get_exchange_client(self.config, test_mode=test_mode)

        # Position tracking (synchronized with API in real-time)
        self.positions_tracker = {}  # {symbol: {'entry_price': float, 'size': float, 'side': str, 'entry_time': float}}

        # Trade history
        self.trade_history = []

        # Strategy
        if not strategy_name:
            strategy_name = self.config['trading']['default_strategy']
        self.strategy = get_strategy(strategy_name, self.config)

        # Telegram notifications
        self.notifier = get_notifier(self.config)

        # Nostr signal broadcaster
        self.signal_broadcaster = SignalBroadcaster(self.config)

        # Copy-trade listener (optional)
        self.copytrade_cfg = self.config.get('copytrade', {})
        self.copytrade_role = (self.copytrade_cfg.get('role') or 'leader').lower()
        self.copytrade_listener = None
        if self.copytrade_cfg.get('enabled') and self.copytrade_role == 'follower':
            nostr_cfg = self.config.get('nostr', {})
            shared_key = _pubkey_to_hex(nostr_cfg.get('relayer_nostr_pubkey'))
            nsec = nostr_cfg.get('nsec')
            relays = nostr_cfg.get('relays', [])
            follow_pubkeys = self.copytrade_cfg.get('follow_pubkeys', [])
            if shared_key and nsec and relays:
                self.copytrade_listener = CopyTradeListener(
                    nsec=nsec,
                    relays=relays,
                    shared_key_hex=shared_key,
                    allowed_pubkeys=follow_pubkeys,
                    on_signal=self._process_copytrade_signal,
                )
                self.copytrade_listener.start()
            else:
                logger.warning("Copy-trade enabled for follower mode but relayer_nostr_pubkey/nsec/relays missing or invalid; listener not started")

        logger.info(f"🤖 Trading bot initialized | Strategy: {strategy_name} | Test mode: {test_mode}")

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration file"""
        path = Path(config_path)
        with path.open('r') as f:
            config = json.load(f)

        # Resolve secrets that may be provided via environment variables (e.g., "$PRIVATE_KEY")
        def resolve_env_or_literal(value: Optional[str], field: str) -> Optional[str]:
            if value and isinstance(value, str) and value.startswith('$'):
                env_name = value[1:]
                env_val = os.getenv(env_name)
                if env_val:
                    logger.info(f"Loaded {field} from env: {env_name}")
                    return env_val
                logger.warning(f"{field} references env {env_name} but it is not set")
                return None
            return value

        config['private_key'] = resolve_env_or_literal(config.get('private_key'), 'private_key')

        # Auto-provision nostr keys and relays if missing to improve UX
        nostr_cfg = config.setdefault('nostr', {})
        updated = False

        legacy_shared = nostr_cfg.pop('platform_shared_key', None)
        if legacy_shared and not nostr_cfg.get('relayer_nostr_pubkey'):
            nostr_cfg['relayer_nostr_pubkey'] = legacy_shared
            logger.info("Migrated nostr.platform_shared_key to nostr.relayer_nostr_pubkey")

        nsec = nostr_cfg.get('nsec')
        relays = nostr_cfg.get('relays', [])

        def generate_keys():
            priv = PrivateKey()
            nostr_cfg['nsec'] = priv.bech32()
            nostr_cfg['npub'] = priv.public_key.bech32()
            logger.info("Generated nostr keypair and wrote to config.json")

        # Treat placeholder or invalid nsec as missing and auto-generate
        if not nsec or nsec == "nsec1yourprivatekey":
            generate_keys()
            updated = True
        else:
            try:
                PrivateKey.from_nsec(nsec)
                # populate npub if absent
                if not nostr_cfg.get('npub'):
                    priv = PrivateKey.from_nsec(nsec)
                    nostr_cfg['npub'] = priv.public_key.bech32()
                    updated = True
            except Exception:
                logger.warning("Invalid nsec in config; generating a new keypair")
                generate_keys()
                updated = True

        if not relays:
            nostr_cfg['relays'] = ["wss://nostr.parallel.hetu.org:8443"]
            updated = True

        if updated:
            with path.open('w') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
                f.write('\n')

        return config

    def sync_positions(self, symbol: str):
        """Synchronize position status (from API)"""
        try:
            positions = self.client.get_positions()

            found = False
            for pos in positions:
                if pos.get('position', {}).get('coin') == symbol:
                    size = float(pos['position']['szi'])
                    entry_price = float(pos['position']['entryPx'])

                    if size != 0:
                        self.positions_tracker[symbol] = {
                            'entry_price': entry_price,
                            'size': abs(size),
                            'side': 'long' if size > 0 else 'short',
                            'entry_time': time.time()
                        }
                        found = True
                        logger.info(f"📊 Synchronized position: {symbol} | {size} | Entry: ${entry_price:.4f}")
                    break

            # If not found, clear the tracker
            if not found and symbol in self.positions_tracker:
                del self.positions_tracker[symbol]
                logger.info(f"✅ Cleared position tracker: {symbol}")

        except Exception as e:
            logger.error(f"Sync positions failed: {e}")

    def get_market_data(self, symbol: str, interval: str = "1h", limit: int = 100) -> pd.DataFrame:
        """Fetch market data and convert to DataFrame"""
        try:
            candles = self.client.get_candles(symbol, interval, limit)

            if not candles:
                logger.warning(f"No data fetched for {symbol}")
                return pd.DataFrame()

            df = pd.DataFrame(candles)
            df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col])

            return df
        except Exception as e:
            logger.error(f"Fetch market data failed: {e}")
            return pd.DataFrame()

    def check_risk_management(self, symbol: str, current_price: float, signal_data: Dict):
        """
        Improved risk management:
        - Use dynamic ATR stop-loss
        - Trailing stop to protect profits
        - Time-based stop-loss (close positions held too long)
        """
        if symbol not in self.positions_tracker:
            return

        position = self.positions_tracker[symbol]
        entry_price = position['entry_price']
        side = position['side']
        entry_time = position['entry_time']

        # Calculate profit/loss percentage
        if side == 'long':
            pnl_percent = (current_price - entry_price) / entry_price
        else:
            pnl_percent = (entry_price - current_price) / entry_price

        # Holding time (hours)
        holding_hours = (time.time() - entry_time) / 3600

        # === 1. Dynamic stop-loss/stop-profit (based on ATR)===
        dynamic_stops = signal_data.get('dynamic_stops', {})
        stop_loss_atr = dynamic_stops.get('stop_loss_atr')
        take_profit_atr = dynamic_stops.get('take_profit_atr')

        close_reason = None

        # Use ATR dynamic stop-loss
        if stop_loss_atr:
            stop_loss_percent = stop_loss_atr / current_price
            if pnl_percent <= -stop_loss_percent:
                close_reason = f"Dynamic stop-loss (ATR: {stop_loss_atr:.4f})"
        else:
            # Fall back to fixed stop-loss
            stop_loss = self.config['trading'].get('stop_loss_percent', 0.03)
            if pnl_percent <= -stop_loss:
                close_reason = f"Fixed stop-loss ({stop_loss*100:.1f}%)"

        # Use ATR dynamic stop-profit
        if take_profit_atr:
            take_profit_percent = take_profit_atr / current_price
            if pnl_percent >= take_profit_percent:
                close_reason = f"Dynamic stop-profit (ATR: {take_profit_atr:.4f})"
        else:
            # Fall back to fixed stop-profit
            take_profit = self.config['trading'].get('take_profit_percent', 0.05)
            if pnl_percent >= take_profit:
                close_reason = f"Fixed stop-profit ({take_profit*100:.1f}%)"

        # === 2. Trailing stop (protect profits)===
        trailing_stop = self.config['trading'].get('trailing_stop_percent', 0.02)
        if pnl_percent > 0.03:  # Profit over 3% triggers trailing stop
            # If drawdown exceeds 2%, close to protect profits
            max_pnl = position.get('max_pnl', pnl_percent)
            position['max_pnl'] = max(max_pnl, pnl_percent)

            if pnl_percent < max_pnl - trailing_stop:
                close_reason = f"Trailing stop (protecting profit {max_pnl*100:.1f}% → {pnl_percent*100:.1f}%)"

        # === 3. Time-based stop-loss (hold too long)===
        max_holding_hours = self.config['trading'].get('max_holding_hours', 24)
        if holding_hours > max_holding_hours:
            if pnl_percent < -0.01:  # Still losing after 24h, force close
                close_reason = f"Time stop-loss (holding {holding_hours:.1f}h)"

        # === 4. Reverse signal stop-loss ===
        # If the strategy gives a strong reverse signal, should also close
        current_signal = signal_data.get('signal', 'hold')
        signal_strength = signal_data.get('strength', 0)

        if side == 'long' and current_signal == 'sell' and signal_strength > 0.6:
            close_reason = f"Reverse signal stop-loss (SELL signal strength {signal_strength*100:.0f}%)"
        elif side == 'short' and current_signal == 'buy' and signal_strength > 0.6:
            close_reason = f"Reverse signal stop-loss (BUY signal strength {signal_strength*100:.0f}%)"

        # Execute close
        if close_reason:
            logger.warning(f"⚠️ Triggered close: {close_reason} | PnL: {pnl_percent*100:.2f}%")

            success = self.close_position(symbol, reason=close_reason)

            if success:
                # Send close notification
                if self.notifier and self.config.get('telegram', {}).get('notify_closures', True):
                    pnl = position['size'] * (current_price - entry_price) if side == 'long' else position['size'] * (entry_price - current_price)
                    self.notifier.notify_position_closed(
                        symbol, entry_price, current_price, pnl, pnl_percent, close_reason, self.test_mode
                    )

                # Record to trade history
                self.trade_history.append({
                    'timestamp': datetime.now(),
                    'symbol': symbol,
                    'type': 'close',
                    'side': side,
                    'entry_price': entry_price,
                    'exit_price': current_price,
                    'size': position['size'],
                    'pnl': pnl if 'pnl' in locals() else 0,
                    'pnl_percent': pnl_percent,
                    'reason': close_reason,
                    'holding_hours': holding_hours
                })

    def close_position(self, symbol: str, reason: str = "") -> bool:
        """Close position"""
        if symbol not in self.positions_tracker:
            logger.warning(f"No {symbol} position record")
            return False

        position = self.positions_tracker[symbol]

        if self.test_mode:
            logger.info(f"[Test mode] Closing {symbol} | Reason: {reason}")
            del self.positions_tracker[symbol]
            return True

        try:
            # Use market_close to close
            from hyperliquid.exchange import Exchange
            exchange = Exchange(self.client.account)

            close_result = exchange.market_close(symbol, sz=position['size'])
            logger.info(f"✅ Closed position: {close_result}")

            # Clear position tracker
            del self.positions_tracker[symbol]

            return True

        except Exception as e:
            logger.error(f"Close failed: {e}")
            if self.notifier:
                self.notifier.notify_error(f"Close failed ({symbol}): {str(e)}")
            return False

    def execute_trade(self, symbol: str, signal: Dict) -> bool:
        """Execute trade (improved version)"""
        # First, sync position status
        self.sync_positions(symbol)

        if not self.strategy.should_trade():
            logger.warning("Risk control: Paused trading")
            return False

        signal_type = signal['signal']
        if signal_type == 'hold':
            return False

        # Send signal notification
        if self.notifier and self.config.get('telegram', {}).get('notify_signals', True):
            self.notifier.notify_trade_signal(symbol, signal, signal['price'])

        # Broadcast encrypted signal to Nostr (optional)
        if getattr(self, "signal_broadcaster", None) and self.signal_broadcaster.enabled:
            self.signal_broadcaster.send_trade_signal(
                symbol=symbol,
                signal=signal,
                strategy=self.strategy.name,
                test_mode=self.test_mode,
                account=self.config.get('wallet_address'),
            )

        try:
            # Check current position
            has_position = symbol in self.positions_tracker

            if has_position:
                current_side = self.positions_tracker[symbol]['side']

                # If signal and position direction are consistent, skip
                if (signal_type == 'buy' and current_side == 'long') or \
                   (signal_type == 'sell' and current_side == 'short'):
                    logger.info(f"Already holding {current_side} position, skipping {signal_type} signal")
                    return False

                # If signal and position direction are opposite, close first
                logger.info(f"Signal reversed, closing {current_side}")
                self.close_position(symbol, reason=f"Reversed signal: {signal_type}")
                time.sleep(1)  # Wait for close to complete

            # Get account balance
            balance = self.client.get_balance()
            logger.info(f"Current balance: ${balance:.2f}")

            # Calculate trade amount
            position_value = signal['size'] * balance
            min_order_value = 10.0

            if position_value < min_order_value:
                position_value = min_order_value

            trade_size = position_value / signal['price']

            logger.info(f"Calculating trade: Position=${position_value:.2f}, Price=${signal['price']:.4f}, Size={trade_size:.2f}")

            if self.test_mode:
                logger.info(f"[Test mode] {signal_type.upper()} {symbol} | Size: {trade_size:.4f}")

                # Test mode also records position
                self.positions_tracker[symbol] = {
                    'entry_price': signal['price'],
                    'size': trade_size,
                    'side': 'long' if signal_type == 'buy' else 'short',
                    'entry_time': time.time()
                }

                if getattr(self, "signal_broadcaster", None) and self.signal_broadcaster.enabled:
                    self.signal_broadcaster.send_execution_report(
                        symbol=symbol,
                        side='long' if signal_type == 'buy' else 'short',
                        strategy=self.strategy.name,
                        size=trade_size,
                        price=signal['price'],
                        status='simulated',
                        tx_hash=None,
                        pnl=None,
                        pnl_percent=None,
                        test_mode=True,
                        account=self.config.get('wallet_address'),
                    )

                return True

            # Actual order
            is_buy = signal_type == 'buy'
            order = self.client.place_order(
                symbol=symbol,
                is_buy=is_buy,
                size=trade_size,
                price=signal['price'],
                order_type='limit'
            )

            logger.info(f"📤 Order submitted: {order}")

            # Update position tracker
            self.positions_tracker[symbol] = {
                'entry_price': signal['price'],
                'size': trade_size,
                'side': 'long' if is_buy else 'short',
                'entry_time': time.time()
            }

            # Send trade notification
            if self.notifier and self.config.get('telegram', {}).get('notify_trades', True):
                self.notifier.notify_trade_executed(
                    symbol, signal_type, trade_size, signal['price'], test_mode=False
                )

            if getattr(self, "signal_broadcaster", None) and self.signal_broadcaster.enabled:
                oid = None
                if isinstance(order, dict):
                    try:
                        oid = order.get('response', {}).get('data', {}).get('statuses', [{}])[0].get('resting', {}).get('oid')
                    except Exception:
                        oid = None

                self.signal_broadcaster.send_execution_report(
                    symbol=symbol,
                    side='long' if is_buy else 'short',
                    strategy=self.strategy.name,
                    size=trade_size,
                    price=signal['price'],
                    status='submitted',
                    tx_hash=None,
                    pnl=None,
                    pnl_percent=None,
                    test_mode=False,
                    account=self.config.get('wallet_address'),
                    note=f"oid={oid}" if oid else None,
                )

            # Record trade
            self.trade_history.append({
                'timestamp': datetime.now(),
                'symbol': symbol,
                'type': signal_type,
                'size': trade_size,
                'price': signal['price'],
                'order': order
            })

            self.strategy.last_trade_time = time.time()
            self.strategy.trade_count += 1

            return True

        except Exception as e:
            logger.error(f"Trade execution failed: {e}")
            if self.notifier:
                self.notifier.notify_error(f"Trade execution failed: {str(e)}")
            return False

    # ------------------------------------------------------------------
    # Copy-trade handling
    # ------------------------------------------------------------------

    def _process_copytrade_signal(self, payload: Dict, sender_pubkey: str):
        """Handle incoming copy-trade signal payload and place a mirrored order."""
        try:
            if not self.copytrade_cfg.get('enabled'):
                return

            symbol = payload.get('symbol')
            signal_type = payload.get('signal')
            price = float(payload.get('price', 0) or 0)

            leader_eth = payload.get('account') or payload.get('agent_eth_address') or payload.get('eth_address')
            allowed_eth = self.copytrade_cfg.get('follow_pubkeys', [])
            if allowed_eth and leader_eth not in allowed_eth:
                logger.debug("Copy-trade: leader %s not in allowlist", leader_eth)
                return

            if not symbol or signal_type not in ('buy', 'sell') or price <= 0:
                logger.debug("Copy-trade: invalid payload %s", payload)
                return

            allowed_symbols = self.copytrade_cfg.get('symbols')
            if allowed_symbols and symbol not in allowed_symbols:
                logger.debug("Copy-trade: symbol %s not allowed", symbol)
                return

            size_pct = float(self.copytrade_cfg.get('size_pct', 0.05))
            min_order_value = float(self.copytrade_cfg.get('min_order_value', 10.0))

            balance = self.client.get_balance()
            position_value = max(balance * size_pct, min_order_value)
            trade_size = position_value / price if price > 0 else 0

            if trade_size <= 0:
                logger.debug("Copy-trade: computed size <= 0 for payload %s", payload)
                return

            logger.info(
                "Copy-trade signal from %s | %s %s | price=%.4f | size=%.4f (value=%.2f)",
                sender_pubkey[:12], signal_type, symbol, price, trade_size, position_value,
            )

            order = self.client.place_order(
                symbol=symbol,
                is_buy=signal_type == 'buy',
                size=trade_size,
                price=price,
                order_type='limit',
            )

            self.trade_history.append({
                'timestamp': datetime.now(),
                'symbol': symbol,
                'type': f'copy_{signal_type}',
                'size': trade_size,
                'price': price,
                'order': order,
                'from_pubkey': sender_pubkey,
            })

            if self.notifier and self.config.get('telegram', {}).get('notify_trades', True):
                self.notifier.notify_trade_executed(
                    symbol, signal_type, trade_size, price, test_mode=self.test_mode
                )

        except Exception as exc:
            logger.warning("Copy-trade handling failed: %s", exc)

    def run(self, symbol: Optional[str] = None, interval: Optional[int] = None):
        """Run the trading bot (refresh interval from config unless overridden)"""
        trading_cfg = self.config.get('trading', {})
        resolved_interval = interval or int(trading_cfg.get('refresh_interval_seconds', 300))
        symbol = symbol or trading_cfg.get('default_symbol')
        candle_interval = str(trading_cfg.get('candle_interval', '1h'))

        logger.info(
            f"🚀 Starting trading bot | Symbol: {symbol} | Refresh interval: {resolved_interval} seconds | Candle interval: {candle_interval}"
        )

        # In follower mode we only listen to copy-trade signals and skip strategy trading.
        if self.copytrade_cfg.get('enabled') and self.copytrade_role == 'follower':
            logger.info("Copy-trade follower mode active: strategy loop disabled; awaiting Nostr signals (kind 30931)")
            try:
                while True:
                    time.sleep(resolved_interval)
            except KeyboardInterrupt:
                logger.info("Received stop signal, shutting down...")
                self.shutdown()
            except Exception as e:
                logger.error(f"Runtime error (follower idle loop): {e}")
                if self.notifier:
                    self.notifier.notify_error(f"Bot runtime exception: {str(e)}")
                self.shutdown()
            return

        # Send startup notification
        if self.notifier and self.config.get('telegram', {}).get('notify_startup', True):
            self.notifier.notify_startup(
                self.strategy.name, 
                symbol, 
                test_mode=self.test_mode
            )

        # Initialize: sync positions
        self.sync_positions(symbol)

        try:
            while True:
                # Fetch market data
                df = self.get_market_data(symbol, interval=candle_interval)

                if df.empty:
                    logger.warning("Unable to fetch market data")
                    time.sleep(interval)
                    continue

                current_price = df['close'].iloc[-1]
                logger.info(f"{symbol} Current price: ${current_price:.4f}")

                # Strategy analysis
                signal = self.strategy.analyze(df)

                # Record strategy analysis
                logger.info(f"Strategy analysis | Signal: {signal['signal'].upper()} | Strength: {signal['strength']:.2%}")

                if 'indicators' in signal:
                    ind = signal['indicators']
                    logger.info(f"Technical indicators | RSI: {ind.get('rsi', 0):.2f} | "
                              f"MACD: {ind.get('macd', 0):.4f} | "
                              f"ADX: {ind.get('adx', 0):.2f}")

                if 'conditions' in signal and signal['conditions']:
                    for cond_type, conds in signal['conditions'].items():
                        if conds:
                            satisfied = sum(conds.values())
                            total = len(conds)
                            logger.info(f"{cond_type.upper()} Conditions: {satisfied}/{total} | {conds}")

                # Risk management check (every loop)
                self.check_risk_management(symbol, current_price, signal)

                # Trade signal
                if signal['signal'] != 'hold':
                    logger.warning(f"⚠️ Trade signal triggered: {signal['signal'].upper()} | Strength: {signal['strength']:.2%}")
                    self.execute_trade(symbol, signal)

                # Wait for next loop
                time.sleep(resolved_interval)

        except KeyboardInterrupt:
            logger.info("Received stop signal, shutting down...")
            self.shutdown()
        except Exception as e:
            logger.error(f"Runtime error: {e}")
            if self.notifier:
                self.notifier.notify_error(f"Bot runtime exception: {str(e)}")
            self.shutdown()

    def shutdown(self):
        """Shutdown safely"""
        logger.info("💾 Saving trade history...")

        # Calculate statistics
        total_trades = len(self.trade_history)
        closed_trades = [t for t in self.trade_history if t.get('type') == 'close']
        winning_trades = sum(1 for t in closed_trades if t.get('pnl', 0) > 0)
        total_pnl = sum(t.get('pnl', 0) for t in closed_trades)
        win_rate = winning_trades / len(closed_trades) if closed_trades else 0

        stats = {
            'total_trades': total_trades,
            'closed_trades': len(closed_trades),
            'winning_trades': winning_trades,
            'total_pnl': total_pnl,
            'win_rate': win_rate
        }

        logger.info(f"📊 Trade statistics: {stats}")

        # Send shutdown notification
        if self.notifier:
            self.notifier.notify_shutdown(stats)

        # Save trade history
        history_file = f"trade_history_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
        with open(history_file, 'w') as f:
            json.dump(self.trade_history, f, indent=2, default=str)

        logger.info(f"✅ Trade history saved to {history_file}")
        logger.info("👋 Bot safely shutdown")


def run_init(config_path: Path) -> None:
    print("\033[1;36m" + "="*60 + "\033[0m")
    print("\033[1;35m🦉 MOLTRADE TRADER INIT\033[0m")
    print("\033[1;33m⚡ Autonomous AI Trading Engine\033[0m")
    print("\033[1;36m" + "="*60 + "\033[0m")
    print("\033[1;32m💎 YOUR 24/7 AI TRADER — EARNING WHILE YOU SLEEP\033[0m")
    print("\033[1;31m🔥 POWERED BY ADVANCED STRATEGIES & Privacy NOSTR\033[0m")
    print("\033[1;36m" + "="*60 + "\033[0m")

    example = config_path.parent / "config.example.json"
    if not config_path.exists():
        if example.exists():
            shutil.copyfile(example, config_path)
            print(f"Copied template to {config_path} for initialization")
        else:
            print("config.example.json not found; cannot bootstrap config")
            return

    with config_path.open('r') as f:
        config = json.load(f)

    def prompt(msg: str, default: Optional[str] = None, allow_empty: bool = False) -> str:
        suffix = f" [{default}]" if default is not None else ""
        while True:
            val = input(f"{msg}{suffix}: ").strip()
            if val == '' and default is not None:
                return default
            if val == '' and allow_empty:
                return ''
            if val:
                return val

    total_steps = 6

    def print_step(i: int, title: str) -> None:
        step_color = "\033[1;36m"
        title_color = "\033[1;37m"
        reset = "\033[0m"
        print(f"\n{step_color}[{i}/{total_steps}]{reset} {title_color}{title}{reset}")

    # [5/6] Nostr keys is intentionally silent (auto-provision only)
    def ensure_nostr_keys_silent() -> None:
        nostr_cfg = config.setdefault('nostr', {})
        nsec = nostr_cfg.get('nsec')

        if not nsec or nsec == "nsec1yourprivatekey":
            priv = PrivateKey()
            nostr_cfg['nsec'] = priv.bech32()
            nostr_cfg['npub'] = priv.public_key.bech32()
        else:
            try:
                priv = PrivateKey.from_nsec(nsec)
                if not nostr_cfg.get('npub'):
                    nostr_cfg['npub'] = priv.public_key.bech32()
            except Exception:
                priv = PrivateKey()
                nostr_cfg['nsec'] = priv.bech32()
                nostr_cfg['npub'] = priv.public_key.bech32()

        if not nostr_cfg.get('relays'):
            nostr_cfg['relays'] = ["wss://nostr.parallel.hetu.org:8443"]

        return

    # [4/6] Trading basics may be skipped for copy-trade followers
    def ensure_trading_defaults_silent() -> None:
        trading = config.setdefault('trading', {})
        trading.setdefault('exchange', 'hyperliquid')
        trading.setdefault('default_symbol', 'HYPE')
        trading.setdefault('default_strategy', 'test')
        trading.setdefault('candle_interval', '1h')
        return

    # Copy-trade first (beginner default)
    print_step(1, "Copy-trade setup")
    copy_cfg = config.setdefault('copytrade', {})
    use_copytrade = prompt("Enable copy-trade follower mode? (Y/n)", "y").strip().lower()
    is_copytrade_follower = (use_copytrade == '') or use_copytrade.startswith('y')

    if is_copytrade_follower:
        copy_cfg['enabled'] = True
        copy_cfg['role'] = 'follower'
        leader_eth = prompt(
            "Leader ETH address to follow",
            (copy_cfg.get('follow_pubkeys') or [None])[0] or "0x...",
            allow_empty=True,
        ).strip()
        if leader_eth:
            copy_cfg['follow_pubkeys'] = [leader_eth]
        else:
            copy_cfg['enabled'] = False
            copy_cfg['follow_pubkeys'] = []
            print("Copy-trade disabled: missing leader ETH address")

        copy_cfg['symbols'] = []
        copy_cfg['size_pct'] = float(copy_cfg.get('size_pct', 0.05))
        copy_cfg['min_order_value'] = float(copy_cfg.get('min_order_value', 10.0))
    else:
        copy_cfg['enabled'] = False
        copy_cfg['role'] = (copy_cfg.get('role') or 'leader').lower()

    # Base URL for relayer API
    print_step(2, "Relayer setup")
    base_url = prompt(
        "Relayer base URL (for bot registration)",
        config.get('relayer_api', 'http://localhost:8080'),
    )
    config['relayer_api'] = base_url.rstrip('/')

    # Wallet setup
    print_step(3, "Wallet setup")
    print("Choose to generate a new private key or use your own wallet.")
    print("1) Generate new private key (recommended for testing)\n2) Use existing wallet")
    choice = prompt("Select option", "2")

    wallet_address = config.get('wallet_address', '')
    private_key = config.get('private_key')

    if choice == '1':
        generated_pk = secrets.token_hex(32)
        print("Generated 32-byte hex private key for you.")

        # Derive an EVM address from the generated private key to avoid asking users
        # to manually enter an address that must match the key.
        derived_address: Optional[str] = None
        try:
            from eth_account import Account  # type: ignore

            derived_address = Account.from_key(bytes.fromhex(generated_pk)).address
        except Exception:
            derived_address = None

        if derived_address:
            wallet_address = derived_address
            print(f"Derived wallet_address from generated private key: {wallet_address}")
        else:
            wallet_address = prompt(
                "Enter wallet_address (must correspond to the private key)",
                wallet_address or "0x...",
            )

        store_env = prompt("Store private key as env reference? (y/N)", "y")
        if store_env.lower().startswith('y'):
            env_name = prompt("Env var name for private key", "PRIVATE_KEY")
            private_key = f"${env_name}"
            print(f"\033[91mRemember to export {env_name}={generated_pk}\033[0m")
        else:
            private_key = generated_pk
    else:
        wallet_address = prompt("Enter wallet_address", wallet_address or "0x...")
        print("Private key is sensitive; using an env var is recommended (export PRIVATE_KEY=yourkey before running the bot)")
        store_env = prompt("Use env var for private_key? (y/N)", "y")
        if store_env.lower().startswith('y'):
            env_name = prompt("Env var name for private key", "PRIVATE_KEY")
            private_key = f"${env_name}"
            print(f"\033[91mAfter init, run: export {env_name}=<your_private_key> before starting the bot\033[0m")
        else:
            private_key = prompt("Enter private_key (will be stored in config)", private_key or "")

    config['wallet_address'] = wallet_address
    config['private_key'] = private_key

    # Trading essentials (skipped for copy-trade followers)
    if not is_copytrade_follower:
        print_step(4, "Trading basics")
        trading = config.setdefault('trading', {})
        trading['exchange'] = prompt("Trading.exchange", trading.get('exchange', 'hyperliquid'))
        trading['default_symbol'] = prompt("Trading.default_symbol", trading.get('default_symbol', 'HYPE'))
        trading['default_strategy'] = prompt("Trading.default_strategy", trading.get('default_strategy', 'test'))
        trading['candle_interval'] = prompt("Trading.candle_interval", trading.get('candle_interval', '1h'))
        print("Other trading/risk fields can be adjusted later in config.json.")
    else:
        ensure_trading_defaults_silent()

    # Nostr keys (silent)
    ensure_nostr_keys_silent()

    nostr_cfg = config.setdefault('nostr', {})

    # Save config after prompts
    with config_path.open('w') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
        f.write('\n')
    print(f"Config saved to {config_path}.")

    # Bot registration
    print_step(6, "Bot registration")
    try_register = prompt("Register bot with relayer now? (y/N)", "y")
    if try_register.lower().startswith('y'):
        bot_name = prompt("Bot name", "my-bot-1")
        register_url = f"{config['relayer_api']}/api/bots/register"
        payload = {
            "bot_pubkey": wallet_address,
            "nostr_pubkey": nostr_cfg.get('npub'),
            "eth_address": wallet_address,
            "name": bot_name,
        }
        try:
            resp = requests.post(register_url, json=payload, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            print(f"Bot registration response: {data}")
            platform_pubkey = data.get('platform_pubkey')
            if platform_pubkey:
                nostr_cfg['relayer_nostr_pubkey'] = platform_pubkey
                with config_path.open('w') as f:
                    json.dump(config, f, indent=2, ensure_ascii=False)
                    f.write('\n')
                print("Saved platform_pubkey into nostr.relayer_nostr_pubkey")

                # Emit plaintext agent register event over nostr
                try:
                    from nostr.signal_service import SignalBroadcaster

                    sb = SignalBroadcaster(config)
                    if sb.enabled and sb.publisher is not None:
                        sb.send_agent_register(
                            bot_pubkey=wallet_address,
                            nostr_pubkey=nostr_cfg.get('npub', ''),
                            eth_address=wallet_address,
                            name=bot_name,
                        )
                        print("Published agent_register nostr event")
                except Exception as exc:
                    print(f"Failed to publish agent_register nostr event: {exc}")

            # Auto-subscribe follower to leader if copy-trade follower was configured
            if is_copytrade_follower and copy_cfg.get('follow_pubkeys'):
                leader_eth = (copy_cfg.get('follow_pubkeys') or [None])[0]
                follower_eth = wallet_address
                shared_secret = nostr_cfg.get('npub', '')
                if leader_eth and follower_eth and shared_secret:
                    sub_payload = {
                        "bot_pubkey": leader_eth,
                        "follower_pubkey": follower_eth,
                        "shared_secret": shared_secret,
                    }
                    sub_url = f"{config['relayer_api']}/api/subscriptions"
                    try:
                        sub_resp = requests.post(sub_url, json=sub_payload, timeout=10)
                        sub_resp.raise_for_status()
                        print("Follower subscription created for copy-trade.")
                    except Exception as exc:
                        print(f"Failed to create follower subscription: {exc}")
        except Exception as exc:
            print(f"Bot registration failed: {exc}")
    else:
        print("Skipped bot registration.")

    print("\nInit complete. You can now run the bot normally.")


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description='Moltrade Trading Bot(Hyperliquid, Polymarket, etc.)')
    parser.add_argument('--config', type=str, default='config.json', help='Config file path')
    parser.add_argument('--test', action='store_true', help='Test mode')
    parser.add_argument('--strategy', type=str, help='Strategy name')
    parser.add_argument('--symbol', type=str, help='Trading pair')
    parser.add_argument('--interval', type=int, default=None, help='Refresh interval override in seconds (otherwise from config)')
    parser.add_argument('--init', action='store_true', help='Initialize config interactively (no trading)')

    args = parser.parse_args()

    if args.init:
        run_init(Path(args.config))
        return

    strategy_name = args.strategy
    if not strategy_name:
        with open(args.config) as f:
            config = json.load(f)
            strategy_name = config['trading'].get('default_strategy', 'test')

    bot = TradingBot(config_path=args.config, test_mode=args.test, strategy_name=strategy_name)
    bot.run(symbol=args.symbol, interval=args.interval)


if __name__ == "__main__":
    main()