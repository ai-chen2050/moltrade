"""
Binance API Client - Using the Official binance-sdk-spot SDK

Implements the same interface as HyperliquidClient so the factory and
strategy layers require no changes when switching exchange to "binance".

Install:
    pip install binance-sdk-spot
"""
import time
from typing import Dict, List, Optional

from binance_sdk_spot import Spot

# Binance kline interval mapping matches HyperLiquid conventions
_INTERVAL_MAP = {
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "6h": "6h",
    "12h": "12h",
    "1d": "1d",
    "1w": "1w",
}

_TESTNET_URL = "https://testnet.binance.vision"
_MAINNET_URL = "https://api.binance.com"


class BinanceClient:
    """Binance Spot exchange adapter.

    Args:
        api_key:    Binance API key.
        api_secret: Binance API secret.
        testnet:    When True, routes all requests to the Binance testnet.
    """

    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet

        base_url = _TESTNET_URL if testnet else _MAINNET_URL
        self.client = Spot(
            api_key=api_key,
            api_secret=api_secret,
            base_url=base_url,
        )

        # Cache exchange info to avoid repeated network calls
        self._exchange_info_cache: Optional[Dict] = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_exchange_info(self) -> Dict:
        """Return (cached) exchange info."""
        if self._exchange_info_cache is None:
            self._exchange_info_cache = self.client.exchange_info()
        return self._exchange_info_cache

    def _symbol_filters(self, symbol: str) -> Dict:
        """Return the filter map for *symbol* from exchange info."""
        info = self._get_exchange_info()
        for s in info.get("symbols", []):
            if s["symbol"] == symbol:
                return {f["filterType"]: f for f in s.get("filters", [])}
        return {}

    def _round_quantity(self, symbol: str, quantity: float) -> float:
        """Round *quantity* to the step size required by the exchange."""
        filters = self._symbol_filters(symbol)
        lot = filters.get("LOT_SIZE", {})
        step = lot.get("stepSize")
        if step:
            step_f = float(step)
            if step_f > 0:
                import math
                precision = max(0, round(-math.log10(step_f)))
                quantity = round(math.floor(quantity / step_f) * step_f, precision)
        return quantity

    def _round_price(self, symbol: str, price: float) -> float:
        """Round *price* to the tick size required by the exchange."""
        filters = self._symbol_filters(symbol)
        tick_filter = filters.get("PRICE_FILTER", {})
        tick = tick_filter.get("tickSize")
        if tick:
            tick_f = float(tick)
            if tick_f > 0:
                import math
                precision = max(0, round(-math.log10(tick_f)))
                price = round(round(price / tick_f) * tick_f, precision)
        return price

    # ------------------------------------------------------------------
    # Market data
    # ------------------------------------------------------------------

    def get_market_data(self, symbol: str) -> Dict:
        """Return basic exchange metadata for *symbol* (lot size, tick size, etc.)."""
        try:
            info = self._get_exchange_info()
            for s in info.get("symbols", []):
                if s["symbol"] == symbol:
                    return s
            return {}
        except Exception as e:
            print(f"Failed to fetch market data: {e}")
            return {}

    def get_orderbook(self, symbol: str) -> Dict:
        """Return the current order book snapshot for *symbol*."""
        try:
            return self.client.depth(symbol, limit=20)
        except Exception as e:
            print(f"Failed to fetch order book: {e}")
            return {}

    def get_candles(self, symbol: str, interval: str = "1m", limit: int = 100) -> List:
        """Return candlestick data as a list of [ts, open, high, low, close, volume].

        The returned format matches the HyperLiquid adapter so strategies
        that consume candles need no modification.
        """
        try:
            binance_interval = _INTERVAL_MAP.get(interval, interval)
            raw = self.client.klines(symbol, binance_interval, limit=limit)
            # Binance kline row:
            # [open_time, open, high, low, close, volume, close_time,
            #  quote_vol, num_trades, taker_buy_base, taker_buy_quote, ignore]
            candles = [
                [
                    row[0],           # timestamp (ms)
                    float(row[1]),    # open
                    float(row[2]),    # high
                    float(row[3]),    # low
                    float(row[4]),    # close
                    float(row[5]),    # volume
                ]
                for row in raw
            ]
            return candles
        except Exception as e:
            print(f"Failed to fetch candlestick data: {e}")
            import traceback
            traceback.print_exc()
            return []

    # ------------------------------------------------------------------
    # Account / position data
    # ------------------------------------------------------------------

    def get_user_state(self) -> Dict:
        """Return the full Binance account snapshot."""
        try:
            return self.client.account()
        except Exception as e:
            print(f"Failed to fetch account information: {e}")
            return {}

    def get_open_orders(self) -> List:
        """Return all currently open orders (all symbols)."""
        try:
            return self.client.get_open_orders()
        except Exception as e:
            print(f"Failed to fetch open orders: {e}")
            return []

    def get_positions(self) -> List:
        """Return non-zero asset balances as a simplified position list.

        Binance Spot has no margin positions; we represent holdings as
        ``{"asset": str, "free": float, "locked": float}`` dicts so the
        caller can inspect what is held.
        """
        try:
            account = self.get_user_state()
            balances = account.get("balances", [])
            return [
                {
                    "asset": b["asset"],
                    "free": float(b["free"]),
                    "locked": float(b["locked"]),
                }
                for b in balances
                if float(b["free"]) > 0 or float(b["locked"]) > 0
            ]
        except Exception as e:
            print(f"Failed to fetch positions: {e}")
            return []

    def get_balance(self, symbol: str = "USDT") -> float:
        """Return the *free* balance for the given asset symbol.

        Pass ``"USDT"`` (default) to get the quote-currency balance used
        for sizing orders.  Accepts both bare asset codes (``"USDT"``) and
        trading-pair suffixed names (``"BTC"`` from ``"BTCUSDT"``).
        """
        try:
            account = self.get_user_state()
            for b in account.get("balances", []):
                if b["asset"] == symbol:
                    return float(b["free"])
            return 0.0
        except Exception as e:
            print(f"Failed to fetch balance: {e}")
            return 0.0

    # ------------------------------------------------------------------
    # Order management
    # ------------------------------------------------------------------

    def place_order(
        self,
        symbol: str,
        is_buy: bool,
        size: float,
        price: Optional[float] = None,
        order_type: str = "limit",
        reduce_only: bool = False,  # not applicable for spot; accepted for interface compat
    ) -> Dict:
        """Place a spot order on Binance.

        Args:
            symbol:     Trading pair, e.g. ``"BTCUSDT"``.
            is_buy:     ``True`` for BUY, ``False`` for SELL.
            size:       Quantity in base asset.
            price:      Limit price (required when order_type is ``"limit"``).
            order_type: ``"limit"`` or ``"market"``.
            reduce_only: Ignored for spot; present for interface compatibility.

        Returns:
            The raw response dict from the Binance API.
        """
        try:
            side = "BUY" if is_buy else "SELL"
            quantity = self._round_quantity(symbol, size)

            if quantity <= 0:
                print(f"Trade size too small after rounding: {quantity}")
                return {"error": "size_too_small"}

            if order_type == "limit" and price is not None:
                rounded_price = self._round_price(symbol, price)
                print(
                    f"Order parameters: symbol={symbol}, side={side}, "
                    f"qty={quantity}, price={rounded_price}, type=LIMIT"
                )
                result = self.client.new_order(
                    symbol=symbol,
                    side=side,
                    type="LIMIT",
                    timeInForce="GTC",
                    quantity=str(quantity),
                    price=str(rounded_price),
                )
            else:
                # Market order
                print(
                    f"Order parameters: symbol={symbol}, side={side}, "
                    f"qty={quantity}, type=MARKET"
                )
                result = self.client.new_order(
                    symbol=symbol,
                    side=side,
                    type="MARKET",
                    quantity=str(quantity),
                )

            return result

        except Exception as e:
            print(f"Failed to place order: {e}")
            import traceback
            traceback.print_exc()
            raise

    def cancel_order(self, order_id: int, symbol: str) -> Dict:
        """Cancel a specific open order by *order_id* and *symbol*."""
        try:
            return self.client.cancel_order(symbol, orderId=order_id)
        except Exception as e:
            print(f"Failed to cancel order: {e}")
            return {}

    def cancel_all_orders(self, symbol: Optional[str] = None) -> Dict:
        """Cancel all open orders.

        When *symbol* is provided only orders for that pair are cancelled;
        otherwise the method iterates over all pairs that have open orders.
        """
        try:
            if symbol:
                return {"cancelled": self.client.cancel_open_orders(symbol)}

            open_orders = self.get_open_orders()
            symbols_with_orders = {o["symbol"] for o in open_orders}
            results: Dict = {}
            for sym in symbols_with_orders:
                results[sym] = self.client.cancel_open_orders(sym)
            return results
        except Exception as e:
            print(f"Failed to cancel all orders: {e}")
            return {}

    # ------------------------------------------------------------------
    # Convenience / price helpers
    # ------------------------------------------------------------------

    def get_ticker_price(self, symbol: str) -> float:
        """Return the latest price for *symbol*."""
        try:
            resp = self.client.ticker_price(symbol=symbol)
            return float(resp.get("price", 0))
        except Exception as e:
            print(f"Failed to fetch ticker price: {e}")
            return 0.0
