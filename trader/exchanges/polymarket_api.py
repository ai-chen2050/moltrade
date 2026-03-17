"""
Polymarket API Client

Implements the standard exchange interface for trading on Polymarket.
Requires Polymarket API credentials and a wallet private key.

Install:
    pip install py-clob-client
"""
import time
from typing import Dict, List, Optional
try:
    from py_clob_client.client import ClobClient, ApiCreds
    from py_clob_client.constants import POLYGON
    from py_clob_client.clob_types import OrderArgs, OrderType, FilterParams
except ImportError:
    pass # Handled in factory or runtime

class PolymarketClient:
    """Polymarket prediction market adapter."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        api_passphrase: str,
        private_key: str,
        chain_id: int = 137,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.private_key = private_key
        self.chain_id = chain_id
        
        creds = ApiCreds(
            api_key=self.api_key,
            api_secret=self.api_secret,
            api_passphrase=self.api_passphrase,
        )
        
        self.client = ClobClient(
            "https://clob.polymarket.com",
            key=self.private_key,
            chain_id=self.chain_id,
            creds=creds,
        )
        
        # Derive Ethereum address from private key for internal reference
        # py-clob-client requires setting credentials manually or via its utilities
        
    def get_market_data(self, token_id: str) -> Dict:
        """Fetch market data via token_id."""
        try:
            market = self.client.get_market(token_id)
            return market if market else {}
        except Exception as e:
            print(f"Failed to fetch market data: {e}")
            return {}

    def get_orderbook(self, token_id: str) -> Dict:
        """Return the current order book snapshot for *token_id*."""
        try:
            return self.client.get_order_book(token_id)
        except Exception as e:
            print(f"Failed to fetch order book: {e}")
            return {}

    def get_candles(self, token_id: str, interval: str = "1h", limit: int = 100) -> List:
        """
        Polymarket CLOB API does not expose historical k-line data natively via the client SDK.
        Returns empty list or mock data.
        """
        print(f"Warning: get_candles not directly supported via py_clob_client for {token_id}.")
        return []

    def get_user_state(self) -> Dict:
        """Fetch full account state or mock since CLOB API differs."""
        return {"note": "Full account state not natively supported by CLOB, see balances."}

    def get_open_orders(self) -> List:
        """Return all current open orders."""
        try:
            return self.client.get_orders(FilterParams())
        except Exception as e:
            print(f"Failed to fetch open orders: {e}")
            return []

    def get_positions(self) -> List:
        """Return held token ID balances."""
        print("Warning: get_positions requires querying all assets, which is intensive on Polymarket. Returning empty.")
        return []

    def get_balance(self, symbol: str = "USDC") -> float:
        """
        Return the free balance for the given asset.
        On Polymarket, we typically care about USDC or specific outcome tokens.
        """
        print(f"Warning: automatic balance fetching requires direct web3 or Gamma API calls for Polymarket. Returning 0.")
        return 0.0

    def place_order(
        self,
        token_id: str,
        is_buy: bool,
        size: float,
        price: Optional[float] = None,
        order_type: str = "limit",
        reduce_only: bool = False,
    ) -> Dict:
        """
        Place a limit or market order on Polymarket.
        
        Args:
            token_id:    Polymarket outcome token ID.
            is_buy:      True for BUY, False for SELL.
            size:        Quantity.
            price:       Limit price.
            order_type:  "limit" or "market" (most CLOB implementations recommend FOK/IOC for market).
        """
        try:
            # We must map generic string order types to py_clob_client.clob_types.OrderType enum
            if order_type.lower() == "limit":
                ot = OrderType.GTC
            elif order_type.lower() == "fok":
                ot = OrderType.FOK
            elif order_type.lower() == "ioc":
                ot = OrderType.IOC
            else:
                ot = OrderType.GTC # Default to GTC limit

            # Determine side
            from py_clob_client.clob_types import BUY, SELL
            side = BUY if is_buy else SELL
            
            # Format price and size
            # According to polymarket docs, price and size might need rounding/formatting
            
            order_args = OrderArgs(
                price=float(price) if price else 0.5, # Dummy default price if market missing
                size=float(size),
                side=side,
                token_id=token_id,
            )
            
            print(f"Placing order on Polymarket: {token_id}, side: {side}, size: {size}, price: {price}")
            
            signed_order = self.client.create_order(order_args)
            resp = self.client.post_order(signed_order, ot)
            
            return resp
        except Exception as e:
            print(f"Failed to place order: {e}")
            import traceback
            traceback.print_exc()
            raise

    def cancel_order(self, order_id: str, symbol: str) -> Dict:
        """Cancel a specific open order by *order_id*."""
        try:
            return self.client.cancel(order_id)
            # The cancel response is typically a confirmation object or success bool
        except Exception as e:
            print(f"Failed to cancel order: {e}")
            return {}

    def cancel_all_orders(self, symbol: Optional[str] = None) -> Dict:
        """Cancel all open orders."""
        try:
            return self.client.cancel_all()
        except Exception as e:
            print(f"Failed to cancel all orders: {e}")
            return {}

    def get_ticker_price(self, token_id: str) -> float:
        """Fetch real-time spot price from the CLOB."""
        try:
            price = self.client.get_mid_point(token_id)
            return float(price) if price else 0.0
        except Exception as e:
            print(f"Failed to fetch ticker price: {e}")
            return 0.0
