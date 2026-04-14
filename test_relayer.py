import asyncio
import aiohttp
from dotenv import load_dotenv
load_dotenv()
from strategies.relayer_client import create_relayer_client


async def test():
    client = create_relayer_client()
    if not client:
        print("ERROR: credentials not found")
        return

    print(f"Signer: {client.signer}")
    print(f"Proxy:  {client.proxy_wallet}")

    async with aiohttp.ClientSession() as session:
        # Step 1: get nonce
        nonce = await client._get_nonce(session)
        print(f"Nonce: {nonce}")

        # Step 2: test signature (no submit)
        fake_data = "0x1234"
        sig = client._sign_proxy_tx(
            to=client.proxy_wallet,
            calldata=fake_data,
            nonce=nonce or "0",
        )
        print(f"Signature: {sig[:20]}... ({len(sig)} chars)")
        print("Connection + signing test OK")


asyncio.run(test())
