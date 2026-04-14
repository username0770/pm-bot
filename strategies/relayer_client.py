"""
Gasless Split/Merge/Redeem via Polymarket Relayer API.
For PROXY (Magic Link) wallets — uses EIP-712 SafeTx signing.

Required in .env:
  POLYMARKET_PRIVATE_KEY=0x...   (signer EOA private key)
  PROXY_WALLET=0x...             (Magic Link proxy wallet)
  RELAYER_API_KEY=...
  RELAYER_API_KEY_ADDRESS=0x...  (signer address)
"""

import os
import json
import logging
import asyncio
import aiohttp
from web3 import Web3
from eth_abi import encode as abi_encode
from eth_account import Account
from eth_account.messages import encode_typed_data

logger = logging.getLogger("relayer")

CTF_ADDRESS = Web3.to_checksum_address(
    "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
)
USDCE_ADDRESS = Web3.to_checksum_address(
    "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
)
RELAYER_URL = "https://relayer-v2.polymarket.com"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
HASH_ZERO = b"\x00" * 32


class RelayerClient:
    """Gasless CTF operations through Polymarket Relayer (PROXY type)."""

    def __init__(
        self,
        private_key: str,
        proxy_wallet: str,
        relayer_api_key: str,
        relayer_api_key_address: str,
        tx_type: str = "PROXY",
    ):
        self.account = Account.from_key(private_key)
        self.signer = self.account.address
        self.proxy_wallet = Web3.to_checksum_address(proxy_wallet)
        self.api_key = relayer_api_key
        self.api_addr = relayer_api_key_address
        self.tx_type = tx_type  # "PROXY" or "SAFE"

    # ── Helpers ────────────────────────────────────────────

    def _headers(self) -> dict:
        return {
            "RELAYER_API_KEY": self.api_key,
            "RELAYER_API_KEY_ADDRESS": self.api_addr,
            "Content-Type": "application/json",
        }

    async def _get_nonce(self, session: aiohttp.ClientSession) -> str:
        """Get current nonce for wallet (PROXY or SAFE)."""
        async with session.get(
            f"{RELAYER_URL}/nonce",
            params={"address": self.signer, "type": self.tx_type},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            data = await r.json()
            nonce = str(data.get("nonce", ""))
            logger.debug(f"Nonce: {nonce}")
            return nonce

    def _encode_split(self, condition_id_hex: str, amount_units: int) -> str:
        w3 = Web3()
        selector = w3.keccak(
            text="splitPosition(address,bytes32,bytes32,uint256[],uint256)"
        )[:4]
        condition_bytes = bytes.fromhex(condition_id_hex.replace("0x", ""))
        args = abi_encode(
            ["address", "bytes32", "bytes32", "uint256[]", "uint256"],
            [USDCE_ADDRESS, HASH_ZERO, condition_bytes, [1, 2], amount_units],
        )
        return "0x" + (selector + args).hex()

    def _encode_merge(self, condition_id_hex: str, amount_units: int) -> str:
        w3 = Web3()
        selector = w3.keccak(
            text="mergePositions(address,bytes32,bytes32,uint256[],uint256)"
        )[:4]
        condition_bytes = bytes.fromhex(condition_id_hex.replace("0x", ""))
        args = abi_encode(
            ["address", "bytes32", "bytes32", "uint256[]", "uint256"],
            [USDCE_ADDRESS, HASH_ZERO, condition_bytes, [1, 2], amount_units],
        )
        return "0x" + (selector + args).hex()

    def _encode_redeem(self, condition_id_hex: str) -> str:
        w3 = Web3()
        selector = w3.keccak(
            text="redeemPositions(address,bytes32,bytes32,uint256[])"
        )[:4]
        condition_bytes = bytes.fromhex(condition_id_hex.replace("0x", ""))
        args = abi_encode(
            ["address", "bytes32", "bytes32", "uint256[]"],
            [USDCE_ADDRESS, HASH_ZERO, condition_bytes, [1, 2]],
        )
        return "0x" + (selector + args).hex()

    def _sign_proxy_tx(self, to: str, calldata: str, nonce: str) -> str:
        """EIP-712 SafeTx signature for the PROXY wallet.
        Domain: chainId=137, verifyingContract=proxy_wallet
        """
        structured_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "SafeTx": [
                    {"name": "to", "type": "address"},
                    {"name": "value", "type": "uint256"},
                    {"name": "data", "type": "bytes"},
                    {"name": "operation", "type": "uint8"},
                    {"name": "safeTxGas", "type": "uint256"},
                    {"name": "baseGas", "type": "uint256"},
                    {"name": "gasPrice", "type": "uint256"},
                    {"name": "gasToken", "type": "address"},
                    {"name": "refundReceiver", "type": "address"},
                    {"name": "nonce", "type": "uint256"},
                ],
            },
            "domain": {
                "chainId": 137,
                "verifyingContract": self.proxy_wallet,
            },
            "primaryType": "SafeTx",
            "message": {
                "to": Web3.to_checksum_address(to),
                "value": 0,
                "data": bytes.fromhex(calldata.replace("0x", "")),
                "operation": 0,
                "safeTxGas": 0,
                "baseGas": 0,
                "gasPrice": 0,
                "gasToken": ZERO_ADDRESS,
                "refundReceiver": ZERO_ADDRESS,
                "nonce": int(nonce),
            },
        }

        signed = self.account.sign_message(
            encode_typed_data(full_message=structured_data)
        )
        # For SAFE type, Polymarket uses pre-validated sig format:
        # v becomes v+4 (27/28 -> 31/32)
        if self.tx_type == "SAFE":
            r_hex = signed.r.to_bytes(32, "big").hex()
            s_hex = signed.s.to_bytes(32, "big").hex()
            v_raw = signed.v  # 27 or 28
            if v_raw in (0, 1):
                v = v_raw + 31
            elif v_raw in (27, 28):
                v = v_raw + 4
            else:
                v = v_raw
            return "0x" + r_hex + s_hex + format(v, "02x")
        return "0x" + signed.signature.hex()

    async def _submit(
        self,
        to: str,
        calldata: str,
        description: str,
        session: aiohttp.ClientSession,
    ) -> dict | None:
        """POST /submit with Relayer API Key auth.
        For SAFE type, uses py-builder-relayer-client to build+sign request.
        """
        nonce = await self._get_nonce(session)

        if self.tx_type == "SAFE":
            # Use py-builder-relayer-client for Safe tx signing
            from py_builder_relayer_client.models import (
                SafeTransaction, SafeTransactionArgs, OperationType)
            from py_builder_relayer_client.builder.safe import (
                build_safe_transaction_request)
            from py_builder_relayer_client.config import get_contract_config
            from py_builder_relayer_client.signer import Signer as PolySigner

            poly_signer = PolySigner(self.account.key.hex(), 137)
            cfg = get_contract_config(137)
            txn = SafeTransaction(
                to=Web3.to_checksum_address(to),
                value="0",
                data=calldata,
                operation=OperationType.Call,
            )
            args = SafeTransactionArgs(
                from_address=poly_signer.address(),
                nonce=nonce,
                chain_id=137,
                transactions=[txn],
            )
            req = build_safe_transaction_request(poly_signer, args, cfg)
            payload = req.to_dict()
        else:
            # PROXY type (Magic Link)
            signature = self._sign_proxy_tx(to=to, calldata=calldata, nonce=nonce)
            payload = {
                "type": "PROXY",
                "from": self.signer,
                "to": Web3.to_checksum_address(to),
                "proxyWallet": self.proxy_wallet,
                "data": calldata,
                "signature": signature,
                "value": "0",
                "signatureParams": {
                    "gasPrice": "0",
                    "operation": "0",
                    "safeTxnGas": "0",
                    "baseGas": "0",
                    "gasToken": ZERO_ADDRESS,
                    "refundReceiver": ZERO_ADDRESS,
                },
                "nonce": nonce,
                "metadata": "",
            }

        logger.info(f"Relayer -> {description}")
        try:
            async with session.post(
                f"{RELAYER_URL}/submit",
                json=payload,
                headers=self._headers(),
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                text = await resp.text()
                if resp.status == 200:
                    result = json.loads(text)
                    tx_id = result.get("transactionID", "")
                    state = result.get("state", "")
                    logger.info(f"Relayer OK: id={tx_id} state={state}")
                    return result
                else:
                    # Truncate error body so Cloudflare HTML pages don't
                    # flood the log with thousands of lines.
                    snippet = (text or "").strip()
                    if len(snippet) > 200:
                        snippet = snippet[:200] + "... [truncated]"
                    # Single-line
                    snippet = snippet.replace("\n", " ").replace("\r", " ")
                    if resp.status == 429:
                        logger.error("Relayer 429: rate limited by Cloudflare")
                    else:
                        logger.error(f"Relayer {resp.status}: {snippet}")
                    return {"__http_status": resp.status}
        except Exception as e:
            logger.error(f"Relayer request error: {e}")
            return None

    async def _wait_confirmed(
        self,
        tx_id: str,
        session: aiohttp.ClientSession,
        timeout_sec: int = 120,
    ) -> bool:
        """Poll GET /transaction?id={id} until terminal state."""
        if not tx_id:
            return False
        terminal = {"STATE_CONFIRMED", "STATE_FAILED", "STATE_INVALID"}
        last_state = ""
        for _ in range(timeout_sec // 3):
            await asyncio.sleep(3)
            try:
                async with session.get(
                    f"{RELAYER_URL}/transaction",
                    params={"id": tx_id},
                    headers=self._headers(),
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        if not data or not isinstance(data, list):
                            continue
                        tx = data[0]
                        state = tx.get("state", "")
                        last_state = state
                        logger.debug(f"Tx {tx_id[:8]} state: {state}")
                        if state == "STATE_CONFIRMED":
                            logger.info(f"Tx confirmed: {tx_id[:8]}")
                            return True
                        if state in terminal:
                            logger.error(f"Tx failed: {state}")
                            return False
            except Exception as e:
                logger.warning(f"Poll error: {e}")
        logger.error(f"Tx {tx_id[:8]} timeout (last state: {last_state})")
        # Assume success if last known state was NEW/EXECUTED/MINED (progressing)
        if last_state in ("STATE_NEW", "STATE_EXECUTED", "STATE_MINED"):
            logger.info(f"Tx {tx_id[:8]} assumed OK (state={last_state})")
            return True
        return False

    # ── Public API ─────────────────────────────────────────

    async def split(
        self,
        condition_id: str,
        amount_usdc: float,
        session: aiohttp.ClientSession,
        wait: bool = True,
    ) -> bool:
        """Split USDC -> YES + NO (gasless). amount in USDC (6 decimals)."""
        amount_units = int(amount_usdc * 1_000_000)
        calldata = self._encode_split(condition_id, amount_units)
        result = await self._submit(
            to=CTF_ADDRESS,
            calldata=calldata,
            description=f"Split ${amount_usdc} USDC -> YES+NO",
            session=session,
        )
        if result is None or "__http_status" in result:
            return False
        if wait:
            return await self._wait_confirmed(
                result.get("transactionID", ""), session)
        return True

    async def merge(
        self,
        condition_id: str,
        amount_usdc: float,
        session: aiohttp.ClientSession,
        wait: bool = True,
    ) -> bool:
        """Merge YES + NO -> USDC (gasless)."""
        if amount_usdc < 0.001:
            return True
        amount_units = int(amount_usdc * 1_000_000)
        calldata = self._encode_merge(condition_id, amount_units)
        result = await self._submit(
            to=CTF_ADDRESS,
            calldata=calldata,
            description=f"Merge {amount_usdc} YES+NO -> USDC",
            session=session,
        )
        if result is None or "__http_status" in result:
            return False
        if wait:
            return await self._wait_confirmed(
                result.get("transactionID", ""), session)
        return True

    async def redeem(
        self,
        condition_id: str,
        session: aiohttp.ClientSession,
        wait: bool = True,
    ) -> bool:
        """Redeem winning tokens -> USDC after resolution."""
        calldata = self._encode_redeem(condition_id)
        result = await self._submit(
            to=CTF_ADDRESS,
            calldata=calldata,
            description="Redeem winning tokens",
            session=session,
        )
        if result is None or "__http_status" in result:
            return False
        if wait:
            return await self._wait_confirmed(
                result.get("transactionID", ""), session)
        return True


def create_safe_relayer_client() -> "RelayerClient | None":
    """Create a SAFE-type Relayer client from MM2_* env vars (MetaMask + Safe)."""
    pk = os.getenv("MM2_PRIVATE_KEY", "")
    safe = os.getenv("MM2_SAFE", "")
    api_key = os.getenv("MM2_RELAYER_KEY", "")
    api_addr = os.getenv("MM2_RELAYER_KEY_ADDRESS", "")

    missing = []
    if not pk: missing.append("MM2_PRIVATE_KEY")
    if not safe: missing.append("MM2_SAFE")
    if not api_key: missing.append("MM2_RELAYER_KEY")
    if not api_addr: missing.append("MM2_RELAYER_KEY_ADDRESS")
    if missing:
        logger.warning(f"Safe relayer: missing {missing}")
        return None

    return RelayerClient(
        private_key=pk,
        proxy_wallet=safe,
        relayer_api_key=api_key,
        relayer_api_key_address=api_addr,
        tx_type="SAFE",
    )


def create_relayer_client() -> "RelayerClient | None":
    """Create from .env. Returns None if any credentials missing."""
    pk = os.getenv("POLYMARKET_PRIVATE_KEY", "") or os.getenv("PRIVATE_KEY", "")
    proxy = os.getenv("PROXY_WALLET", "") or os.getenv("POLYMARKET_FUNDER", "")
    api_key = os.getenv("RELAYER_API_KEY", "")
    api_addr = os.getenv("RELAYER_API_KEY_ADDRESS", "")

    missing = []
    if not pk: missing.append("POLYMARKET_PRIVATE_KEY")
    if not proxy: missing.append("PROXY_WALLET")
    if not api_key: missing.append("RELAYER_API_KEY")
    if not api_addr: missing.append("RELAYER_API_KEY_ADDRESS")
    if missing:
        logger.warning(f"RelayerClient: missing {missing}")
        return None

    return RelayerClient(
        private_key=pk,
        proxy_wallet=proxy,
        relayer_api_key=api_key,
        relayer_api_key_address=api_addr,
    )
