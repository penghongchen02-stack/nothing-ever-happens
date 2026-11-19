"""Paper trading that fills against real order books.

``RealBookPaperExchangeClient`` keeps the simulated cash and positions of
``PaperExchangeClient`` but takes prices from a live order-book source, so a
paper buy only fills at prices and sizes that are actually on the book.
"""

from __future__ import annotations

import time
from collections.abc import Callable

from bot.exchange.paper import PaperExchangeClient
from bot.models import MarketOrderIntent, OpenOrder, OrderBookSnapshot, OrderResult, Side, Trade


class RealBookPaperExchangeClient(PaperExchangeClient):
    order_id_prefix = "paper"

    def __init__(
        self,
        book_source: Callable[[str], OrderBookSnapshot],
        *,
        initial_collateral_balance: float = 100.0,
    ) -> None:
        super().__init__(initial_collateral_balance=initial_collateral_balance)
        self._book_source = book_source

    def get_order_book(self, token_id: str) -> OrderBookSnapshot:
        return self._book_source(token_id)

    def get_mid_price(self, token_id: str) -> float:
        book = self.get_order_book(token_id)
        best_bid = max((lvl.price for lvl in book.bids), default=0.0)
        best_ask = min((lvl.price for lvl in book.asks), default=0.0)
        if best_bid and best_ask:
            return (best_bid + best_ask) / 2
        return best_ask or best_bid

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
        for level in sorted(book.asks, key=lambda lvl: lvl.price):
            if level.price > price_cap + 1e-9 or budget - spent <= 1e-9:
                break
            take_usd = min(level.price * level.size, budget - spent)
            spent += take_usd
            shares += take_usd / level.price

        self._counter += 1
        oid = f"{self.order_id_prefix}-{int(time.time())}-{self._counter}"
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
