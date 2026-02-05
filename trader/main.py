"""
Improved Trading Bot - Enhanced Risk Management
"""
import json
import time
import pandas as pd
import argparse
import logging
from datetime import datetime
from typing import Dict, Optional
from pathlib import Path

from exchanges.factory import get_exchange_client
from strategies.strategies import get_strategy
from telegram_notifier import get_notifier
from nostr.signal_service import SignalBroadcaster
from nostr.copytrade_listener import CopyTradeListener
from pynostr.key import PrivateKey

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trading_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ImprovedTradingBot:
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
        self.copytrade_listener = None
        if self.copytrade_cfg.get('enabled'):
            nostr_cfg = self.config.get('nostr', {})
            shared_key = nostr_cfg.get('platform_shared_key')
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
                logger.warning("Copy-trade enabled but nostr shared key/nsec/relays missing; listener not started")

        logger.info(f"🤖 Trading bot initialized | Strategy: {strategy_name} | Test mode: {test_mode}")

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration file"""
        path = Path(config_path)
        with path.open('r') as f:
            config = json.load(f)

        # Auto-provision nostr keys and relays if missing to improve UX
        nostr_cfg = config.setdefault('nostr', {})
        updated = False

        nsec = nostr_cfg.get('nsec')
        relays = nostr_cfg.get('relays', [])
        if not nsec:
            priv = PrivateKey()
            nostr_cfg['nsec'] = priv.bech32()
            nostr_cfg['npub'] = priv.public_key.bech32()
            updated = True
            logger.info("Generated nostr keypair and wrote to config.json")

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
                        size=trade_size,
                        price=signal['price'],
                        status='simulated',
                        tx_hash=None,
                        pnl=None,
                        pnl_percent=None,
                        test_mode=True,
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
                self.signal_broadcaster.send_execution_report(
                    symbol=symbol,
                    side='long' if is_buy else 'short',
                    size=trade_size,
                    price=signal['price'],
                    status='submitted',
                    tx_hash=order.get('status') if isinstance(order, dict) else None,
                    pnl=None,
                    pnl_percent=None,
                    test_mode=False,
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

    def run(self, symbol: Optional[str] = None, interval: int = 300):
        """Run the trading bot (refresh interval: 5 minutes)"""
        symbol = symbol or self.config['trading']['default_symbol']

        logger.info(f"🚀 Starting trading bot | Symbol: {symbol} | Refresh interval: {interval} seconds")

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
                df = self.get_market_data(symbol)

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
                time.sleep(interval)

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


def main():
    parser = argparse.ArgumentParser(description='Improved Hyperliquid Trading Bot')
    parser.add_argument('--config', type=str, default='config.json', help='Config file path')
    parser.add_argument('--test', action='store_true', help='Test mode')
    parser.add_argument('--strategy', type=str, help='Strategy name')
    parser.add_argument('--symbol', type=str, help='Trading pair')
    parser.add_argument('--interval', type=int, default=300, help='Refresh interval (seconds)')

    args = parser.parse_args()

    strategy_name = args.strategy
    if not strategy_name:
        with open(args.config) as f:
            config = json.load(f)
            strategy_name = config['trading'].get('default_strategy', 'test')

    bot = ImprovedTradingBot(config_path=args.config, test_mode=args.test, strategy_name=strategy_name)
    bot.run(symbol=args.symbol, interval=args.interval)


if __name__ == "__main__":
    main()