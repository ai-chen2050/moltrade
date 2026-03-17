"""
Uniswap V3 API Client

Implements the standard exchange interface for trading on Uniswap V3.
Requires an RPC URL and a wallet private key. Swaps are executed on-chain.

Install:
    pip install web3
"""
import time
from typing import Dict, List, Optional
import requests
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

# Minimal ERC20 ABI to fetch decimals, balance, and approve
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function"
    },
    {
        "constant": False,
        "inputs": [
            {"name": "_spender", "type": "address"},
            {"name": "_value", "type": "uint256"}
        ],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "type": "function"
    }
]

# Minimal Uniswap V3 Router ABI for exactInputSingle
UNISWAP_V3_ROUTER_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"internalType": "address", "name": "tokenIn", "type": "address"},
                    {"internalType": "address", "name": "tokenOut", "type": "address"},
                    {"internalType": "uint24", "name": "fee", "type": "uint24"},
                    {"internalType": "address", "name": "recipient", "type": "address"},
                    {"internalType": "uint256", "name": "deadline", "type": "uint256"},
                    {"internalType": "uint256", "name": "amountIn", "type": "uint256"},
                    {"internalType": "uint256", "name": "amountOutMinimum", "type": "uint256"},
                    {"internalType": "uint160", "name": "sqrtPriceLimitX96", "type": "uint160"}
                ],
                "internalType": "struct ISwapRouter.ExactInputSingleParams",
                "name": "params",
                "type": "tuple"
            }
        ],
        "name": "exactInputSingle",
        "outputs": [{"internalType": "uint256", "name": "amountOut", "type": "uint256"}],
        "stateMutability": "payable",
        "type": "function"
    }
]

class UniswapClient:
    """Uniswap V3 DEX adapter."""
    
    def __init__(
        self,
        rpc_url: str,
        private_key: str,
        chain_id: int = 1,
        router_address: str = "0xE592427A0AEce92De3Edee1F18E0157C05861564",
        slippage_tolerance: float = 0.005,
        default_token_in: str = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", # WETH
        default_token_out: str = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48" # USDC
    ):
        self.w3 = Web3(Web3.HTTPProvider(rpc_url))
        self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        
        # Verify connection
        if not self.w3.is_connected():
            raise ConnectionError(f"Failed to connect to RPC node at {rpc_url}")

        self.account = self.w3.eth.account.from_key(private_key)
        self.wallet_address = self.account.address
        self.chain_id = chain_id
        
        self.router_address = self.w3.to_checksum_address(router_address)
        self.router_contract = self.w3.eth.contract(address=self.router_address, abi=UNISWAP_V3_ROUTER_ABI)
        
        self.slippage_tolerance = slippage_tolerance
        self.default_token_in = self.w3.to_checksum_address(default_token_in)
        self.default_token_out = self.w3.to_checksum_address(default_token_out)

    def _get_token_contract(self, token_address: str):
        checksum_address = self.w3.to_checksum_address(token_address)
        return self.w3.eth.contract(address=checksum_address, abi=ERC20_ABI)

    def _get_token_decimals(self, token_address: str) -> int:
        contract = self._get_token_contract(token_address)
        return contract.functions.decimals().call()

    def get_market_data(self, symbol: str) -> Dict:
        """Not directly supported on-chain. Returning basic metadata."""
        return {"name": symbol, "type": "ERC20"}

    def get_orderbook(self, symbol: str) -> Dict:
        """Uniswap V3 does not have a traditional order book."""
        return {"bids": [], "asks": []}

    def get_candles(self, symbol: str, interval: str = "1h", limit: int = 100) -> List:
        """
        Fetch synthetic candles via CoinGecko or DeFiLlama for local testing/strategies.
        Since DEXes don't provide native k-lines, we rely on external oracles or return empty.
        For simplicity, we return an empty list or mock response.
        In a real scenario, integrate a GraphQL endpoint (like TheGraph) or an oracle.
        """
        print(f"Warning: get_candles not natively supported for Uniswap. Returning empty data for {symbol}.")
        return []

    def get_user_state(self) -> Dict:
        """Return ETH balance."""
        balance_wei = self.w3.eth.get_balance(self.wallet_address)
        return {"eth_balance_wei": balance_wei, "eth_balance": self.w3.from_wei(balance_wei, "ether")}

    def get_open_orders(self) -> List:
        """DEX swaps are atomic. No open limit orders supported natively without limit order protocols."""
        return []

    def get_positions(self) -> List:
        """Return held token balances (excluding 0). Currently only returns ETH balance."""
        balance_wei = self.w3.eth.get_balance(self.wallet_address)
        bal = float(self.w3.from_wei(balance_wei, "ether"))
        if bal > 0:
            return [{"asset": "ETH", "free": bal, "locked": 0.0}]
        return []

    def get_balance(self, symbol: str = "ETH") -> float:
        """Return token balance or ETH balance."""
        try:
            if symbol.upper() == "ETH":
                balance_wei = self.w3.eth.get_balance(self.wallet_address)
                return float(self.w3.from_wei(balance_wei, "ether"))
            else:
                contract = self._get_token_contract(symbol)
                decimals = self._get_token_decimals(symbol)
                balance_raw = contract.functions.balanceOf(self.wallet_address).call()
                return balance_raw / (10 ** decimals)
        except Exception as e:
            print(f"Failed to fetch balance for {symbol}: {e}")
            return 0.0

    def place_order(
        self,
        symbol: str, 
        is_buy: bool,
        size: float,
        price: Optional[float] = None,
        order_type: str = "market",
        reduce_only: bool = False,
    ) -> Dict:
        """
        Execute a swap on Uniswap V3.
        Note: `symbol` is mapped to `token_in` and `token_out` logic based on config defaults.
        This is a simplified implementation.
        """
        print(f"Executing DEX swap. is_buy: {is_buy}, size: {size}")
        try:
            token_in_addr = self.default_token_out if is_buy else self.default_token_in
            token_out_addr = self.default_token_in if is_buy else self.default_token_out
            
            token_in_contract = self._get_token_contract(token_in_addr)
            token_in_decimals = self._get_token_decimals(token_in_addr)
            
            # Since size is in target token (e.g. buy 1 WETH), we need to estimate amount_in if buying.
            # For simplicity, if is_buy=False, size is amount of token_in (WETH) we want to sell.
            # If is_buy=True, we ideally want to buy `size` of WETH. exactInputSingle requires amount_in.
            # We assume `size` here means amount to spend (amount_in) regardless of direction for simplicity in MVP, 
            # OR we calculate it based on a roughly given price if it is a market order.
            
            if price and price > 0:
                amount_in_float = size * price if is_buy else size
            else:
                amount_in_float = size # Fallback, assumes size = amount to spend
                
            amount_in_raw = int(amount_in_float * (10 ** token_in_decimals))
            
            nonce = self.w3.eth.get_transaction_count(self.wallet_address)
            
            # Check allowance
            allowance = token_in_contract.functions.allowance(self.wallet_address, self.router_address).call()
            if allowance < amount_in_raw:
                print(f"Approving router to spend {amount_in_float} of token {token_in_addr}")
                approve_tx = token_in_contract.functions.approve(self.router_address, amount_in_raw).build_transaction({
                    'from': self.wallet_address,
                    'nonce': nonce,
                    'gasPrice': self.w3.eth.gas_price
                })
                signed_approve = self.w3.eth.account.sign_transaction(approve_tx, private_key=self.account.key)
                tx_hash = self.w3.eth.send_raw_transaction(signed_approve.rawTransaction)
                self.w3.eth.wait_for_transaction_receipt(tx_hash)
                nonce += 1 # Increment nonce for swap tx
            
            deadline = int(time.time()) + 600
            
            # Fee tier: 3000 (0.3% is common for WETH/USDC)
            # In a robust implementation, you would query the factory for the best pool fee tier.
            pool_fee = 3000
            
            params = (
                token_in_addr,
                token_out_addr,
                pool_fee,
                self.wallet_address,
                deadline,
                amount_in_raw,
                0, # amountOutMinimum (set to 0 for MVP, normally use slippage_tolerance)
                0  # sqrtPriceLimitX96
            )
            
            swap_tx = self.router_contract.functions.exactInputSingle(params).build_transaction({
                'from': self.wallet_address,
                'nonce': nonce,
                'gasPrice': self.w3.eth.gas_price,
            })
            
            # Estimate gas
            gas_estimate = self.w3.eth.estimate_gas(swap_tx)
            swap_tx['gas'] = gas_estimate
            
            signed_swap = self.w3.eth.account.sign_transaction(swap_tx, private_key=self.account.key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_swap.rawTransaction)
            
            print(f"Swap transaction sent! Hash: {tx_hash.hex()}")
            return {"status": "submitted", "tx_hash": tx_hash.hex()}
            
        except Exception as e:
            print(f"Failed to execute DEX swap: {e}")
            import traceback
            traceback.print_exc()
            raise

    def cancel_order(self, order_id: int, symbol: str) -> Dict:
        """No-op for DEX swaps."""
        print("cancel_order not applicable for atomic DEX swaps.")
        return {}

    def cancel_all_orders(self, symbol: Optional[str] = None) -> Dict:
        """No-op for DEX swaps."""
        return {}

    def get_ticker_price(self, symbol: str) -> float:
        """
        Fetch real-time spot price from the DEX pool or oracle.
        For MVP, we return 0.0 or a dummy value.
        """
        print("get_ticker_price not fully implemented for Uniswap adapter MVP.")
        return 0.0
