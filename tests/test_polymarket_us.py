import json
from datetime import datetime, timedelta, timezone

import pytest

from bot.exchange.polymarket_us import (
    BookNotReadyError,
    PolymarketUSBookFeed,
    PolymarketUSPaperExchangeClient,
    yes_book_to_no_book,
)
from bot.models import MarketOrderIntent, OrderBookLevel, Side
from bot.us_markets import passes_us_candidate_filters, select_us_standalone_markets


def _iso(days: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _market(slug: str, question: str, **overrides) -> dict:
    market = {
        "id": "1",
        "slug": slug,
        "question": question,
        "title": "By December 31, 2026",
        "category": "politics",
        "endDate": _iso(40),
        "active": True,
        "closed": False,
        "archived": False,
        "hidden": False,
        "status": "MARKET_STATUS_OPEN",
        "outcomes": '["Yes","No"]',
        "minimumTradeQty": 1,
        "tags": [],
    }
    market.update(overrides)
    return market


def _md(slug: str, bids, offers, state="MARKET_STATE_OPEN") -> str:
    def lv(levels):
        return [{"px": {"value": str(p), "currency": "USD"}, "qty": str(q)} for p, q in levels]

    return json.dumps({"marketData": {"marketSlug": slug, "bids": lv(bids), "offers": lv(offers), "state": state}})


def test_filters_drop_sports_closed_far_dated_and_non_binary():
    assert passes_us_candidate_filters(_market("a", "Netanyahu Arrested?"))
    assert not passes_us_candidate_filters(_market("b", "Team wins?", category="sports"))
    assert not passes_us_candidate_filters(_market("c", "Fed cut?", category="macro"))
    assert not passes_us_candidate_filters(_market("d", "X?", status="MARKET_STATUS_RESOLVED"))
    assert not passes_us_candidate_filters(_market("e", "X?", endDate=_iso(400)))
    assert not passes_us_candidate_filters(_market("f", "X?", outcomes='["A","B","C"]'))
    assert not passes_us_candidate_filters(_market("g", "Will Bitcoin hit 200k?"))


def test_markets_sharing_a_question_are_not_standalone():
    raw = [
        _market("spotify-taylor", "Spotify Top Global Artist 2026"),
        _market("spotify-drake", "Spotify Top Global Artist 2026"),
        _market("aliens", "US Confirms That Aliens Exist By"),
    ]
    selected = select_us_standalone_markets(raw)
    assert [m.slug for m in selected] == ["aliens"]
    assert selected[0].no_token_id == "aliens"


def test_yes_book_is_inverted_into_no_book():
    book = yes_book_to_no_book(
        "m",
        yes_bids=(OrderBookLevel(0.40, 10), OrderBookLevel(0.38, 5)),
        yes_offers=(OrderBookLevel(0.45, 7),),
    )
    assert [(lvl.price, lvl.size) for lvl in book.asks] == [(0.60, 10), (0.62, 5)]
    assert [(lvl.price, lvl.size) for lvl in book.bids] == [(0.55, 7)]


def test_paper_client_waits_for_book_then_fills_within_cap():
    feed = PolymarketUSBookFeed(key_id="k", secret_key="s")
    client = PolymarketUSPaperExchangeClient(feed, initial_collateral_balance=100.0)

    with pytest.raises(BookNotReadyError):
        client.get_order_book("m")

    # YES bids 0.45 x 4 and 0.30 x 100 -> NO asks 0.55 x 4 and 0.70 x 100
    feed.handle_message(_md("m", bids=[(0.45, 4), (0.30, 100)], offers=[(0.50, 10)]))
    book = client.get_order_book("m")
    assert book.asks[0].price == pytest.approx(0.55)

    result = client.place_market_order(
        MarketOrderIntent(token_id="m", side=Side.BUY, amount=10.0, reference_price=0.55, price_cap=0.65)
    )
    # Only the 0.55 level is within the 0.65 cap: 4 shares for $2.20.
    assert result.status == "matched"
    assert float(result.raw["takingAmount"]) == pytest.approx(4.0)
    assert float(result.raw["makingAmount"]) == pytest.approx(2.20)
    assert client.get_collateral_balance() == pytest.approx(97.80)


def test_paper_client_reports_unmatched_when_no_liquidity_within_cap():
    feed = PolymarketUSBookFeed(key_id="k", secret_key="s")
    client = PolymarketUSPaperExchangeClient(feed)
    feed.handle_message(_md("m", bids=[(0.20, 50)], offers=[]))
    result = client.place_market_order(
        MarketOrderIntent(token_id="m", side=Side.BUY, amount=5.0, reference_price=0.80, price_cap=0.65)
    )
    assert result.status == "unmatched"
    assert client.get_collateral_balance() == pytest.approx(100.0)


def test_halted_market_has_empty_book():
    feed = PolymarketUSBookFeed(key_id="k", secret_key="s")
    feed.handle_message(_md("m", bids=[(0.4, 10)], offers=[(0.5, 10)], state="MARKET_STATE_HALTED"))
    book = feed.get_no_book("m")
    assert book is not None and book.asks == () and book.bids == ()


@pytest.mark.asyncio
async def test_feed_connects_signs_subscribes_and_caches_books(monkeypatch):
    import asyncio
    import base64

    from nacl.signing import SigningKey
    from websockets.asyncio.server import serve

    for var in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"):
        monkeypatch.delenv(var, raising=False)

    seen = {}

    async def handler(ws):
        seen["path"] = ws.request.path
        seen["auth"] = all(ws.request.headers.get(h) for h in ("X-PM-Access-Key", "X-PM-Timestamp", "X-PM-Signature"))
        async for raw in ws:
            sub = json.loads(raw)["subscribe"]
            seen["sub"] = sub
            for slug in sub["marketSlugs"]:
                await ws.send(_md(slug, bids=[(0.42, 20)], offers=[(0.47, 9)]))

    secret = base64.b64encode(bytes(SigningKey.generate())).decode()
    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        feed = PolymarketUSBookFeed(key_id="k", secret_key=secret, base_url=f"ws://127.0.0.1:{port}")
        client = PolymarketUSPaperExchangeClient(feed)
        shutdown = asyncio.Event()
        task = asyncio.create_task(feed.run(shutdown))
        with pytest.raises(BookNotReadyError):
            client.get_order_book("m")  # registers the slug for subscription
        for _ in range(50):
            await asyncio.sleep(0.05)
            if feed.get_no_book("m") is not None:
                break
        book = client.get_order_book("m")
        shutdown.set()
        await asyncio.wait_for(task, 5)

    assert seen["path"] == "/v1/ws/markets"
    assert seen["auth"]
    assert seen["sub"]["subscriptionType"] == "SUBSCRIPTION_TYPE_MARKET_DATA"
    assert seen["sub"]["marketSlugs"] == ["m"]
    assert book.asks[0].price == pytest.approx(0.58)
    assert book.bids[0].price == pytest.approx(0.53)
