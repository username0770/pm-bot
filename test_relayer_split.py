"""Real Split $1 test through Relayer."""
import asyncio
import aiohttp
from dotenv import load_dotenv
load_dotenv()
from strategies.relayer_client import create_relayer_client


async def get_current_condition_id(session):
    """Fetch active BTC 5min market's conditionId from Gamma."""
    import time
    # Get current 5min slug
    ts = int(time.time())
    rounded = ts - (ts % 300)
    slug = f"btc-updown-5m-{rounded}"

    url = "https://gamma-api.polymarket.com/events"
    async with session.get(url, params={"slug": slug}) as r:
        events = await r.json()
        if not events:
            return None, None
        for ev in events:
            if ev.get("closed"):
                continue
            for m in ev.get("markets", []):
                if m.get("acceptingOrders"):
                    cid = m.get("conditionId", "")
                    return cid, slug
    return None, slug


async def test():
    client = create_relayer_client()
    if not client:
        print("ERROR: credentials not found")
        return

    print(f"Signer: {client.signer}")
    print(f"Proxy:  {client.proxy_wallet}")

    async with aiohttp.ClientSession() as session:
        condition_id, slug = await get_current_condition_id(session)
        print(f"Slug: {slug}")
        print(f"Condition ID: {condition_id}")

        if not condition_id:
            print("ERROR: could not fetch condition_id")
            return

        # Real split $1 USDC
        print(f"\nAttempting Split $1 USDC...")
        ok = await client.split(condition_id, 1.0, session, wait=True)
        print(f"\nSplit $1 result: {'OK' if ok else 'FAILED'}")

        if ok:
            print("Check balance at https://polymarket.com/profile")


asyncio.run(test())
