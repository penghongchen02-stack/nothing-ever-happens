"""Standalone yes/no market discovery for Polymarket US (polymarket.us).

Polymarket US lists markets on its public gateway. Each market is a single
YES instrument; buying NO is expressed as ORDER_INTENT_BUY_SHORT on the same
market slug. Markets that belong to one multi-outcome question (for example
"Spotify Top Global Artist 2026") share the same ``question`` text, so a market
is treated as standalone when no other open market has the same question.

Returned markets reuse ``StandaloneMarket``. Both token ids are the market slug,
because Polymarket US addresses everything by slug.
"""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
from datetime import datetime, timedelta, timezone

import aiohttp

from bot.standalone_markets import (
    DEFAULT_MAX_END_DATE_MONTHS,
    EXCLUDED_KEYWORDS,
    EXCLUDED_TITLE_PHRASES,
    StandaloneMarket,
    _load_json_list,
    _parse_iso_ts,
)

logger = logging.getLogger(__name__)

US_GATEWAY_API = "https://gateway.polymarket.us"
US_PAGE_LIMIT = 500
# The US API allows 25 requests/s per IP; stay far below that.
US_PAGE_DELAY_SEC = 0.5
US_PAGE_MAX_RETRIES = 6
US_RETRY_BASE_DELAY_SEC = 2.0
US_RETRY_MAX_DELAY_SEC = 30.0
US_MAX_PAGES = 200

# Polymarket US categories that mirror the global bot's keyword exclusions.
EXCLUDED_US_CATEGORIES = {"sports", "crypto", "finance", "macro"}
# Categories fetched from the gateway. Sports alone is tens of thousands of
# markets, so only the categories the strategy can trade are requested.
INCLUDED_US_CATEGORIES = ("politics", "culture", "technology", "geopolitics", "climate", "science")


class USMarketFetchError(RuntimeError):
    pass


def _is_binary_yes_no(market: dict) -> bool:
    outcomes = _load_json_list(market.get("outcomes"))
    if len(outcomes) == 2:
        return {str(o).strip().lower() for o in outcomes} == {"yes", "no"}
    sides = market.get("marketSides") or []
    labels = {str(side.get("description") or "").strip().lower() for side in sides if isinstance(side, dict)}
    return len(sides) == 2 and labels == {"yes", "no"}


def _is_open(market: dict) -> bool:
    if not market.get("active") or market.get("closed") or market.get("archived") or market.get("hidden"):
        return False
    status = str(market.get("status") or "MARKET_STATUS_OPEN")
    return status == "MARKET_STATUS_OPEN"


def _has_excluded_text(market: dict) -> bool:
    for field in ("question", "title", "subtitle", "eventSubcategory"):
        value = str(market.get(field) or "").lower()
        if any(phrase in value for phrase in EXCLUDED_TITLE_PHRASES):
            return True
        if any(keyword in value for keyword in EXCLUDED_KEYWORDS):
            return True
    for tag in market.get("tags") or []:
        label = str((tag.get("label") or tag.get("name") or "") if isinstance(tag, dict) else tag).lower()
        if any(keyword in label for keyword in EXCLUDED_KEYWORDS):
            return True
    return False


def _ends_within_window(market: dict, *, max_end_date_months: int) -> bool:
    end_ts = _parse_iso_ts(str(market.get("endDate") or ""))
    if end_ts <= 0:
        return False
    now = datetime.now(timezone.utc)
    end = datetime.fromtimestamp(end_ts, tz=timezone.utc)
    return now <= end <= now + timedelta(days=max_end_date_months * 30)


def passes_us_candidate_filters(market: dict, *, max_end_date_months: int = DEFAULT_MAX_END_DATE_MONTHS) -> bool:
    if not _is_open(market):
        return False
    if str(market.get("category") or "").lower() in EXCLUDED_US_CATEGORIES:
        return False
    if not _is_binary_yes_no(market):
        return False
    if _has_excluded_text(market):
        return False
    if not _ends_within_window(market, max_end_date_months=max_end_date_months):
        return False
    return True


def _question_key(market: dict) -> str:
    return str(market.get("question") or "").strip().lower()


def build_us_standalone_market(market: dict) -> StandaloneMarket | None:
    slug = str(market.get("slug") or "")
    if not slug:
        return None
    question = str(market.get("question") or "")
    title = str(market.get("title") or "")
    display_question = f"{question} ({title})" if title and title.lower() not in question.lower() else question
    end_date = str(market.get("endDate") or "")
    return StandaloneMarket(
        question=display_question,
        slug=slug,
        condition_id=str(market.get("id") or ""),
        yes_token_id=slug,
        no_token_id=slug,
        yes_price=0.0,
        no_price=0.0,
        volume=0.0,
        liquidity=0.0,
        min_order_size=float(market.get("minimumTradeQty") or 0.0),
        end_date=end_date,
        end_ts=_parse_iso_ts(end_date),
        category=str(market.get("category") or ""),
        event_slug=_question_key(market),
    )


def select_us_standalone_markets(
    raw_markets: list[dict],
    *,
    max_end_date_months: int = DEFAULT_MAX_END_DATE_MONTHS,
) -> list[StandaloneMarket]:
    question_counts = Counter(_question_key(m) for m in raw_markets if _is_open(m))
    selected: list[StandaloneMarket] = []
    for raw in raw_markets:
        if question_counts[_question_key(raw)] != 1:
            continue
        if not passes_us_candidate_filters(raw, max_end_date_months=max_end_date_months):
            continue
        market = build_us_standalone_market(raw)
        if market is not None:
            selected.append(market)
    selected.sort(key=lambda m: m.end_ts)
    return selected


async def fetch_all_open_us_markets(session: aiohttp.ClientSession) -> list[dict]:
    markets: list[dict] = []
    offset = 0
    retries = 0
    pages = 0
    while pages < US_MAX_PAGES:
        params = [
            ("active", "true"),
            ("closed", "false"),
            ("limit", str(US_PAGE_LIMIT)),
            ("offset", str(offset)),
            *(("categories", category) for category in INCLUDED_US_CATEGORIES),
        ]
        try:
            async with session.get(
                f"{US_GATEWAY_API}/v1/markets",
                params=params,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                resp.raise_for_status()
                payload = await resp.json()
        except aiohttp.ClientResponseError as exc:
            if exc.status == 429 and retries < US_PAGE_MAX_RETRIES:
                delay = min(US_RETRY_BASE_DELAY_SEC * (2**retries), US_RETRY_MAX_DELAY_SEC)
                retries += 1
                logger.warning("us_markets_rate_limited offset=%d retry=%d delay=%.1f", offset, retries, delay)
                await asyncio.sleep(delay)
                continue
            raise USMarketFetchError(f"us_markets_fetch_aborted offset={offset} status={exc.status}") from exc
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise USMarketFetchError(f"us_markets_fetch_failed offset={offset} err={exc}") from exc

        retries = 0
        batch = payload.get("markets") if isinstance(payload, dict) else None
        if not isinstance(batch, list) or not batch:
            break
        markets.extend(m for m in batch if isinstance(m, dict))
        pages += 1
        offset += len(batch)
        if len(batch) < US_PAGE_LIMIT:
            break
        await asyncio.sleep(US_PAGE_DELAY_SEC)
    return markets


async def fetch_us_candidate_markets(
    session: aiohttp.ClientSession,
    *,
    max_end_date_months: int = DEFAULT_MAX_END_DATE_MONTHS,
) -> list[StandaloneMarket]:
    raw = await fetch_all_open_us_markets(session)
    markets = select_us_standalone_markets(raw, max_end_date_months=max_end_date_months)
    logger.info("us_markets_selected", extra={"open_markets": len(raw), "candidates": len(markets)})
    return markets
