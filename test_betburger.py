"""
Тест BetBurger API — полный вывод с данными Polymarket
Запуск: python test_betburger.py
        python test_betburger.py --debug   (JSON первого бета)
"""

import os, sys, json, requests, datetime
from dotenv import load_dotenv
load_dotenv()

try:
    from polymarket_bet import from_betburger, MARKET_NAMES, parse_direct_link
    HAS_PM_PARSER = True
except ImportError:
    HAS_PM_PARSER = False
    MARKET_NAMES = {
        1:"Team1 Win",2:"Team2 Win",11:"1 (Home)",12:"X (Draw)",13:"2 (Away)",
        14:"1X",15:"X2",16:"12",17:"Asian HCP1",18:"Asian HCP2",
        19:"Total Over",20:"Total Under",
    }
    print("⚠️  polymarket_bet.py не найден")

try:
    from db_bets import BetDatabase
    HAS_DB = True
except ImportError:
    HAS_DB = False

TOKEN         = os.getenv("BETBURGER_TOKEN", "")
FILTER_ID     = int(os.getenv("BETBURGER_FILTER_ID_VALUEBET", "0"))
ENDPOINT      = "https://rest-api-pr.betburger.com/api/v1/valuebets/bot_pro_search"
POLYMARKET_ID = 483

BOOKMAKERS = {
    2:"Bet365",3:"Unibet",4:"Pinnacle",9:"Smarkets",10:"Interwetten",
    12:"WilliamHill",13:"Bwin",16:"PS3838",21:"18Bet",24:"Tipico",
    26:"Betsson_SE",27:"Koronabet",30:"Betway",31:"Betfair",34:"Betsson",
    39:"1xBet",48:"Betclic",52:"Boylesports",56:"StoiximanGR",57:"Betvictor",
    74:"Marathonbet",76:"Winline",78:"NordicBet",80:"Betfair_ex",92:"888sport",
    94:"Bwin_RS",95:"BetAtHome",128:"Fonbet",145:"Vbet_AM",148:"Mozzartbet",
    150:"BetCity",162:"Vbet",187:"Betano",188:"Betcris",199:"Betsson_DE",
    200:"Coolbet",204:"Matchbook",308:"BetBoom",314:"Betmaster",432:"GGbet",
    458:"Melbet",464:"Megapari",469:"Parimatch",483:"Polymarket",
    488:"Bwin_AT",489:"Mostbet",700:"Betandyou",702:"BetWinner",710:"Betway_KE",
}
SPORT_NAMES={
    1:"⚾ Baseball",      2:"🏀 Basketball",   4:"🤾 Futsal",
    5:"🤾 Handball",      6:"🏒 Hockey",        7:"⚽ Soccer",
    8:"🎾 Tennis",        9:"🏐 Volleyball",   10:"🏈 Am.Football",
    11:"🎱 Snooker",     12:"🎯 Darts",        13:"🏓 Table Tennis",
    14:"🏸 Badminton",   15:"🏉 Rugby League", 16:"🏊 Water Polo",
    17:"🏒 Bandy",       18:"🥊 Martial Arts", 19:"🏑 Field Hockey",
    20:"🏉 AFL",         21:"🎮 Other eSports",22:"♟️ Chess",
    23:"🏐 Gaelic Sport",24:"🏏 Cricket",      25:"🏎️ Formula 1",
    27:"🏎️ Motorsport",  28:"🚴 Cycling",      29:"🏐 Beach Volley",
    30:"🏇 Horse Racing", 31:"🎿 Biathlon",    32:"🥌 Curling",
    33:"🎾 Squash",      34:"🏐 Netball",      35:"⚽ Beach Soccer",
    36:"🏒 Floorball",   37:"🏑 Hurling",      39:"🎮 E-Soccer",
    41:"🎮 E-Basketball",43:"🏉 Rugby Union",  44:"🥊 Boxing",
    45:"🥋 MMA",         46:"🎮 Dota 2",       47:"🎮 CS2",
    48:"🎮 LoL",         49:"⛳ Golf",         50:"🥍 Lacrosse",
    51:"🎮 Valorant",    52:"🎮 Overwatch",    53:"🎮 PUBG",
    54:"🎮 Fortnite",    55:"🎮 R6 Siege",     56:"🎮 CrossFire",
    57:"🎮 Call of Duty",58:"🎮 Apex Legends", 59:"🎮 Deadlock",
    60:"🎮 Standoff 2",  61:"🎮 King of Glory",62:"🎮 Arena of Valor",
    63:"🎮 Mobile Legends",64:"🎮 HotS",       65:"🎮 StarCraft",
    66:"🎮 Warcraft",    67:"🎮 AoE",          68:"🎮 Hearthstone",
    69:"🎮 Rocket League",70:"🎮 Brawl Stars", 71:"🎮 HALO",
}

def check_env():
    if not TOKEN or "YOUR" in TOKEN.upper():
        print("❌  BETBURGER_TOKEN не задан в .env"); sys.exit(1)
    if FILTER_ID == 0:
        print("❌  BETBURGER_FILTER_ID_VALUEBET = 0 в .env"); sys.exit(1)
    print(f"✅  Токен:     {TOKEN[:8]}...{TOKEN[-4:]}")
    print(f"✅  Filter ID: {FILTER_ID}")

def fetch_raw(endpoint=ENDPOINT, filter_id=FILTER_ID, per_page=100):
    params = {"access_token": TOKEN, "locale": "en"}
    data   = {"search_filter[]": filter_id, "per_page": per_page}
    print(f"\nPOST {endpoint}")
    resp = requests.post(endpoint, params=params, data=data, timeout=15)
    print(f"HTTP {resp.status_code}")
    if resp.status_code != 200:
        print(f"Ошибка: {resp.text[:400]}"); sys.exit(1)
    return resp.json()

def enrich(raw):
    bets = raw.get("bets", raw) if isinstance(raw, dict) else raw
    arbs = raw.get("arbs", []) if isinstance(raw, dict) else []
    arb_by_bet = {}
    for arb in arbs:
        for key in ("bet1_id","bet2_id","bet3_id"):
            bid = arb.get(key)
            if bid:
                arb_by_bet[bid] = arb
    result = []
    for bet in bets:
        bid      = bet.get("id","")
        bk_id    = bet.get("bookmaker_id")
        bk_name  = BOOKMAKERS.get(bk_id, f"ID={bk_id}")
        arb_meta = arb_by_bet.get(bid, {})
        raw_roi  = arb_meta.get("percent") or arb_meta.get("roi") or 0
        roi      = float(raw_roi)
        if abs(roi) > 1:
            roi = roi / 100
        result.append({**bet,"bk_name":bk_name,"roi_pct":roi,"arb_meta":arb_meta})
    return result

def display_summary(bets):
    print(f"\n{'═'*70}")
    print(f"  Всего бетов: {len(bets)}")
    print(f"{'═'*70}")
    bk_stats = {}
    for b in bets:
        k = b["bk_name"]
        bk_stats[k] = bk_stats.get(k,0)+1
    print("\nБукмекеры:")
    for bk,cnt in sorted(bk_stats.items(), key=lambda x:-x[1])[:12]:
        print(f"  {bk:22} {cnt:3}  {'█'*cnt}")
    sport_stats = {}
    for b in bets:
        nm = SPORT_NAMES.get(b.get("sport_id"), f"sport={b.get('sport_id')}")
        sport_stats[nm] = sport_stats.get(nm,0)+1
    print("\nСпорты:")
    for s,cnt in sorted(sport_stats.items(), key=lambda x:-x[1]):
        print(f"  {s:25}  {cnt}")
    print(f"\n{'─'*70}")
    print(f"  Топ-15 по Edge%")
    print(f"{'─'*70}")
    print(f"  {'#':>3}  {'Edge%':>7}  {'Коэф':>8}  {'Implied':>8}  {'Ликв':>9}  Событие / Маркет")
    print(f"  {'─'*67}")
    for i,b in enumerate(sorted(bets, key=lambda x:-x["roi_pct"])[:15],1):
        roi  = b["roi_pct"]
        koef = float(b.get("koef",0) or 0)
        impl = (1/koef) if koef>0 else 0
        liq  = b.get("market_depth",0) or 0
        home = b.get("home","") or b.get("team1_name","")
        away = b.get("away","") or b.get("team2_name","")
        ev   = f"{home} vs {away}"[:35]
        mkt  = MARKET_NAMES.get(b.get("market_and_bet_type"),"?")
        param = b.get("market_and_bet_type_param",0) or 0
        param_s = f" {param}" if param else ""
        roi_s = f"{roi:+.2%}" if roi!=0 else "  n/a "
        print(f"  {i:>3}  {roi_s:>7}  {koef:>8.4f}  {impl:>7.1%}  ${liq:>8,.0f}  {ev}")
        print(f"       [{b['bk_name']:12}]  {mkt}{param_s}")

def display_polymarket(bets, db=None):
    poly = sorted([b for b in bets if b.get("bookmaker_id")==POLYMARKET_ID],
                  key=lambda x:-x["roi_pct"])
    print(f"\n{'═'*70}")
    print(f"  ✅ POLYMARKET СТАВКИ: {len(poly)}")
    print(f"{'═'*70}")
    if not poly:
        print(f"  ⚠️  Нет Polymarket бетов из {len(bets)} всего")
        return

    for i,b in enumerate(poly,1):
        roi   = b["roi_pct"]
        koef  = float(b.get("koef",0) or 0)
        impl  = (1/koef) if koef>0 else 0
        liq   = b.get("market_depth",0) or 0
        home  = b.get("home","") or b.get("team1_name","")
        away  = b.get("away","") or b.get("team2_name","")
        league= b.get("league_name", b.get("league",""))
        mkt_id= b.get("market_and_bet_type",0)
        mkt_nm= MARKET_NAMES.get(mkt_id, f"type={mkt_id}")
        param = b.get("market_and_bet_type_param",0) or 0
        sport_nm = SPORT_NAMES.get(b.get("sport_id"),"")
        started_ts = b.get("started_at",0)
        started_s  = datetime.datetime.fromtimestamp(started_ts).strftime("%d.%m %H:%M") \
                     if started_ts else "?"

        print(f"\n  {'─'*68}")

        # Проверка дубля в БД
        badge = ""
        pm_obj = None
        if HAS_PM_PARSER:
            pm_obj = from_betburger(b, b.get("arb_meta",{}))
            if db and pm_obj and pm_obj.outcome_id:
                existing = db.already_bet(pm_obj.outcome_id)
                badge = f"   ⛔ УЖЕ СТАВИЛИ #{existing.id} ({existing.status})" \
                        if existing else "   ✅ новый"

        print(f"  #{i:02d}  🎯  {home} vs {away}{badge}")
        print(f"         {sport_nm}  {league}   📅 {started_s}")

        print(f"\n         ИСХОД:     {mkt_nm}{f'  линия {param}' if param else ''}")
        print(f"         EDGE:      +{roi:.2%}")
        print(f"         КОЭФ:      {koef:.4f}   implied {impl:.1%}")

        if pm_obj:
            # Данные из direct_link
            print(f"\n         POLYMARKET:")
            print(f"           Исход-name:     {pm_obj.outcome_name}")
            if pm_obj.market_id:
                print(f"           Market ID:      {pm_obj.market_id}")
                print(f"           🔗  https://polymarket.com/event/{pm_obj.market_id}")
            if pm_obj.outcome_id:
                short_id = f"{pm_obj.outcome_id[:28]}...{pm_obj.outcome_id[-8:]}"
                print(f"           Token (outcomeId): {short_id}")
            if pm_obj.neg_risk:
                print(f"           ⚠️  negRisk = True")

            print(f"\n         ЛИКВИДНОСТЬ:")
            print(f"           Рынок (total):  ${pm_obj.total_liquidity:>12,.2f}")
            print(f"           На нашей цене:  ${pm_obj.depth_at_price:>12,.2f}")
            if pm_obj.best_ask > 0:
                print(f"           Best ask:       {pm_obj.best_ask:.4f}  (${pm_obj.best_ask_size:>10,.2f})")
            if pm_obj.competitive > 0:
                comp_pct = pm_obj.competitive * 100
                print(f"           Конкурентность: {comp_pct:.2f}%")

            if pm_obj.order_book:
                print(f"\n         СТАКАН (bestOffers):")
                print(f"           {'Коэф':>8}  {'Price':>7}  {'Implied':>8}  {'Объём':>12}")
                print(f"           {'─'*46}")
                for j,lvl in enumerate(pm_obj.order_book[:8]):
                    is_ours = abs(lvl.price - impl) < 0.025
                    marker  = " ◄ наш уровень" if is_ours else ""
                    flag    = "→ " if is_ours else "  "
                    print(f"           {flag}{lvl.odds:>7.4f}  {lvl.price:>7.4f}  "
                          f"{lvl.implied_pct:>7.1f}%  ${lvl.size:>10,.2f}{marker}")
        else:
            # Фолбэк — raw данные
            dl = b.get("direct_link","") or b.get("bookmaker_event_direct_link","")
            if dl:
                if HAS_PM_PARSER:
                    pd = parse_direct_link(dl)
                    if pd.get("market_id"):
                        print(f"           Market ID:   {pd['market_id']}")
                    if pd.get("outcome_id"):
                        print(f"           Token ID:    {pd['outcome_id'][:40]}...")
                    if pd.get("liquidity_num"):
                        print(f"           Ликвидность: ${pd['liquidity_num']:,.2f}")
                else:
                    print(f"           direct_link: {dl[:100]}")
            print(f"           market_depth:  ${liq:,.2f}")

        print(f"\n         META:")
        print(f"           BetBurger ref event: {b.get('event_id','?')}")
        print(f"           BK event ID:         {b.get('bookmaker_event_id','?')}")

    print(f"\n{'═'*70}")

def fetch_live_test():
    LIVE = "https://rest-api-lv.betburger.com/api/v1/valuebets/bot_pro_search"
    print(f"\n{'─'*70}  LIVE")
    try:
        raw  = fetch_raw(endpoint=LIVE, per_page=50)
        bets = enrich(raw)
        poly = [b for b in bets if b.get("bookmaker_id")==POLYMARKET_ID]
        print(f"  Live бетов: {len(bets)},  Polymarket: {len(poly)}")
        for b in sorted(poly, key=lambda x:-x["roi_pct"])[:5]:
            koef = float(b.get("koef",0) or 0)
            mkt  = MARKET_NAMES.get(b.get("market_and_bet_type"),"?")
            home = b.get("home","") or b.get("team1_name","")
            away = b.get("away","") or b.get("team2_name","")
            print(f"    +{b['roi_pct']:.2%}  {koef:.4f}  ${b.get('market_depth',0) or 0:,.0f}  "
                  f"{home} vs {away}  [{mkt}]")
    except Exception as e:
        print(f"  ошибка: {e}")

if __name__ == "__main__":
    print("═"*70)
    print("  BETBURGER API TEST  —  Polymarket Value Bets")
    print("═"*70)
    check_env()

    db_path = os.getenv("DB_PATH_VALUEBET","valuebets.db")
    db = BetDatabase(db_path) if HAS_DB else None
    if db:
        print(f"✅  БД: {db_path}")

    raw  = fetch_raw()
    bets = enrich(raw)

    display_summary(bets)
    display_polymarket(bets, db=db)

    if db:
        db.print_stats()

    fetch_live_test()

    # Debug mode
    if "--debug" in sys.argv:
        poly = [b for b in bets if b.get("bookmaker_id")==POLYMARKET_ID]
        if poly:
            first = poly[0]
            print("\n[DEBUG] JSON первого Polymarket бета:")
            print(json.dumps({k:v for k,v in first.items() if k!="arb_meta"},
                             indent=2, ensure_ascii=False, default=str))
            if first.get("arb_meta"):
                print("\n[DEBUG] arb_meta:")
                print(json.dumps(first["arb_meta"], indent=2, ensure_ascii=False, default=str))

    print(f"\n{'═'*70}  ЗАВЕРШЁН")