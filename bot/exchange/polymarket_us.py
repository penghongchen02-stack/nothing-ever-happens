"""Polymarket US (polymarket.us) market data and paper trading.

Polymarket US does not publish live order-book depth on its public gateway;
full books are only streamed over the authenticated markets WebSocket
(``wss://api.polymarket.us/v1/ws/markets``). ``PolymarketUSBookFeed`` keeps a
cache of the latest snapshot per market slug, and
``PolymarketUSPaperExchangeClient`` simulates fills against those real books.

Each Polymarket US market is one YES instrument. Its book is quoted in YES
prices, so the NO side is derived as:

    NO ask = 1 - YES bid      (selling YES at the bid == buying NO)
    NO bid = 1 - YES offer

Live order transmission on Polymarket US is not implemented yet.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import threading
import time
from dataclasses import dataclass

from bot.exchange.paper import PaperExchangeClient
from bot.models import (
    MarketOrderIntent,
    MarketRules,
    OpenOrder,
    OrderBookLevel,
    OrderBookSnapshot,
    OrderResult,
    Side,
    Trade,
)

logger = logging.getLogger(__name__)

US_WS_BASE_URL = "wss://api.polymarket.us"
US_MARKETS_WS_PATH = "/v1/ws/markets"
SUBSCRIBE_CHUNK_SIZE = 50
BOOK_STALE_AFTER_SEC = 300.0
DEFAULT_TICK_SIZE = 0.01


class BookNotReadyError(RuntimeError):
    pass


@dataclass(frozen=True)
class _YesBook:
    bids: tuple[OrderBookLevel, ...]
    offers: tuple[OrderBookLevel, ...]
    state: str
    received_at: float


def _amount(value) -> float:
    if isinstance(value, dict):
        value = value.get("value")
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _parse_levels(levels) -> tuple[OrderBookLevel, ...]:
    parsed = []
    for level in levels or []:
        if not isinstance(level, dict):
            continue
        price = _amount(level.get("px"))
        size = _amount(level.get("qty"))
        if 0.0 < price < 1.0 and size > 0.0:
            parsed.append(OrderBookLevel(price=price, size=size))
    return tuple(parsed)


def yes_book_to_no_book(
    slug: str,
    yes_bids: tuple[OrderBookLevel, ...],
    yes_offers: tuple[OrderBookLevel, ...],
    *,
    tick_size: float = DEFAULT_TICK_SIZE,
    min_order_size: float = 1.0,
    timestamp_ms: int = 0,
) -> OrderBookSnapshot:
    no_asks = tuple(
        sorted(
            (OrderBookLevel(price=round(1.0 - lvl.price, 6), size=lvl.size) for lvl in yes_bids),
            key=lambda lvl: lvl.price,
        )
    )
    no_bids = tuple(
        sorted(
            (OrderBookLevel(price=round(1.0 - lvl.price, 6), size=lvl.size) for lvl in yes_offers),
            key=lambda lvl: lvl.price,
            reverse=True,
        )
    )
    return OrderBookSnapshot(
        token_id=slug,
        bids=no_bids,
        asks=no_asks,
        tick_size=tick_size,
        min_order_size=min_order_size,
        timestamp=timestamp_ms,
    )


class PolymarketUSBookFeed:
    """Caches full order books from the authenticated Polymarket US markets WebSocket."""

    def __init__(
        self,
        *,
        key_id: str | None,
        secret_key: str | None,
        base_url: str = US_WS_BASE_URL,
        stale_after_sec: float = BOOK_STALE_AFTER_SEC,
    ) -> None:
        self._key_id = (key_id or "").strip()
        self._secret_key = (secret_key or "").strip()
        self._base_url = base_url
        self._stale_after_sec = stale_after_sec
        self._lock = threading.Lock()
        self._wanted: set[str] = set()
        self._subscribed: set[str] = set()
        self._books: dict[str, _YesBook] = {}
        self._request_ids = itertools.count(1)
        self.connected = False
        self.last_error = ""

    @property
    def has_credentials(self) -> bool:
        return bool(self._key_id and self._secret_key)

    def watch(self, slug: str) -> None:
        with self._lock:
            self._wanted.add(slug)

    def get_no_book(self, slug: str, *, tick_size: float = DEFAULT_TICK_SIZE) -> OrderBookSnapshot | None:
        with self._lock:
            book = self._books.get(slug)
        if book is None or (time.time() - book.received_at) > self._stale_after_sec:
            return None
        if book.state not in ("MARKET_STATE_OPEN", ""):
            return OrderBookSnapshot(token_id=slug, bids=(), asks=(), tick_size=tick_size, min_order_size=1.0)
        return yes_book_to_no_book(
            slug,
            book.bids,
            book.offers,
            tick_size=tick_size,
            timestamp_ms=int(book.received_at * 1000),
        )

    def handle_message(self, raw: str | bytes) -> None:
        try:
            message = json.loads(raw)
        except (TypeError, ValueError):
            return
        if not isinstance(message, dict):
            return
        if "error" in message:
            self.last_error = str(message.get("error"))
            logger.warning("us_book_feed_error err=%s request_id=%s", message.get("error"), message.get("requestId"))
            return
        data = message.get("marketData")
        if not isinstance(data, dict):
            return
        slug = str(data.get("marketSlug") or "")
        if not slug:
            return
        book = _YesBook(
            bids=_parse_levels(data.get("bids")),
            offers=_parse_levels(data.get("offers")),
            state=str(data.get("state") or ""),
            received_at=time.time(),
        )
        with self._lock:
            self._books[slug] = book

    def _pending_subscriptions(self) -> list[str]:
        with self._lock:
            return sorted(self._wanted - self._subscribed)

    async def _subscribe(self, ws, slugs: list[str]) -> None:
        for start in range(0, len(slugs), SUBSCRIBE_CHUNK_SIZE):
            chunk = slugs[start : start + SUBSCRIBE_CHUNK_SIZE]
            request = {
                "subscribe": {
                    "requestId": f"nh-md-{next(self._request_ids)}",
                    "subscriptionType": "SUBSCRIPTION_TYPE_MARKET_DATA",
                    "marketSlugs": chunk,
                }
            }
            await ws.send(json.dumps(request))
            with self._lock:
                self._subscribed.update(chunk)
        if slugs:
            logger.info("us_book_feed_subscribed", extra={"count": len(slugs)})

    async def run(self, shutdown_event: asyncio.Event) -> None:
        if not self.has_credentials:
            self.last_error = "POLYMARKET_KEY_ID / POLYMARKET_SECRET_KEY not set"
            logger.error(
                "us_book_feed_disabled: set POLYMARKET_KEY_ID and POLYMARKET_SECRET_KEY "
                "(create them at polymarket.us/developer); Polymarket US order books need an API key"
            )
            # Stay alive so the supervisor does not restart this task in a loop.
            await shutdown_event.wait()
            return

        from polymarket_us.auth import create_auth_headers
        from websockets.asyncio.client import connect

        backoff = 1.0
        while not shutdown_event.is_set():
            try:
                headers = create_auth_headers(self._key_id, self._secret_key, "GET", US_MARKETS_WS_PATH)
                async with connect(
                    f"{self._base_url}{US_MARKETS_WS_PATH}",
                    additional_headers=headers,
                    open_timeout=20,
                    ping_interval=20,
                ) as ws:
                    self.connected = True
                    self.last_error = ""
                    backoff = 1.0
                    with self._lock:
                        self._subscribed.clear()
                    logger.info("us_book_feed_connected")
                    await self._pump(ws, shutdown_event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = f"{type(exc).__name__}: {exc}"
                logger.warning("us_book_feed_disconnected err=%s retry_in=%.0fs", self.last_error, backoff)
            finally:
                self.connected = False
            if shutdown_event.is_set():
                break
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, 60.0)

    async def _pump(self, ws, shutdown_event: asyncio.Event) -> None:
        while not shutdown_event.is_set():
            pending = self._pending_subscriptions()
            if pending:
                await self._subscribe(ws, pending)
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            self.handle_message(raw)


class PolymarketUSPaperExchangeClient(PaperExchangeClient):
    """Paper trading against real Polymarket US NO-side order books."""

    def __init__(self, feed: PolymarketUSBookFeed, *, initial_collateral_balance: float = 100.0) -> None:
        super().__init__(initial_collateral_balance=initial_collateral_balance)
        self.feed = feed

    def get_order_book(self, token_id: str) -> OrderBookSnapshot:
        self.feed.watch(token_id)
        book = self.feed.get_no_book(token_id)
        if book is None:
            reason = self.feed.last_error or ("waiting_for_snapshot" if self.feed.connected else "feed_not_connected")
            raise BookNotReadyError(f"us_book_not_ready slug={token_id} reason={reason}")
        return book

    def get_mid_price(self, token_id: str) -> float:
        book = self.get_order_book(token_id)
        best_bid = max((lvl.price for lvl in book.bids), default=0.0)
        best_ask = min((lvl.price for lvl in book.asks), default=0.0)
        if best_bid and best_ask:
            return (best_bid + best_ask) / 2
        return best_ask or best_bid

    def get_market_rules(self, token_id: str) -> MarketRules:
        _ = token_id
        return MarketRules(tick_size=DEFAULT_TICK_SIZE, min_order_size=1.0)

    def place_market_order(self, order: MarketOrderIntent) -> OrderResult:
        if order.side != Side.BUY:
            return super().place_market_order(order)

        book = self.get_order_book(order.token_id)
        price_cap = order.price_cap if order.price_cap is not None else 1.0
        if order.reference_price and order.allowed_slippage is not None:
            price_cap = min(price_cap, order.reference_price + order.allowed_slippage)

        budget = min(float(order.amount), self._collateral_balance)
        spent = 0.0
        shares = 0.0
        for level in book.asks:
            if level.price > price_cap + 1e-9 or budget - spent <= 1e-9:
                break
            take_usd = min(level.price * level.size, budget - spent)
            spent += take_usd
            shares += take_usd / level.price

        self._counter += 1
        oid = f"paper-us-{int(time.time())}-{self._counter}"
        if shares <= 0:
            return OrderResult(order_id=oid, status="unmatched", raw={"order": order, "reason": "no_liquidity_within_cap"})

        fill_price = spent / shares
        self._conditional_balances[order.token_id] = self._conditional_balances.get(order.token_id, 0.0) + shares
        self._collateral_balance = max(0.0, self._collateral_balance - spent)
        self._trades.append(
            Trade(
                trade_id=f"{oid}-fill",
                order_id=oid,
                token_id=order.token_id,
                side=order.side,
                price=fill_price,
                size=shares,
                timestamp=int(time.time()),
            )
        )
        self._orders_by_id[oid] = OpenOrder(
            order_id=oid,
            token_id=order.token_id,
            side=order.side,
            price=fill_price,
            size_matched=shares,
            original_size=shares,
            status="matched",
        )
        raw = {
            "order": order,
            "_market_price": fill_price,
            "_buffered_price": fill_price,
            "_fill_price": fill_price,
            "makingAmount": str(spent),
            "takingAmount": str(shares),
        }
        return OrderResult(order_id=oid, status="matched", raw=raw)
