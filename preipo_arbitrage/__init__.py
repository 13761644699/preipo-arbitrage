from __future__ import annotations

import html
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "data" / "reports" / "preipo_arbitrage.html"
LATEST_JSON = ROOT / "data" / "reports" / "preipo_arbitrage_latest.json"

BINANCE_FAPI = "https://fapi.binance.com"
OKX_API = "https://www.okx.com"
BITGET_API = "https://api.bitget.com"
GATE_API = "https://api.gateio.ws"

# Some venues quote these pre-IPO contracts as a valuation index where 2500 means
# roughly 250.0B USD. Keep the conversion visible instead of hiding it in labels.
VALUATION_INDEX_FACTOR_B = 0.1
IMPLIED_MARKET_CAP_FACTOR_B = {
    "valuation_index_10x": VALUATION_INDEX_FACTOR_B,
    "proxy_share_price": 1.2,
    "proxy_token_price": 1000.0,
}
DEFAULT_ROUND_TRIP_FEE_PCT = 0.16


@dataclass(frozen=True)
class Instrument:
    asset: str
    asset_label: str
    venue: str
    venue_type: str
    source: str
    symbol: str
    value_kind: str
    unit_note: str
    enabled: bool = True


@dataclass
class VenueQuote:
    asset: str
    asset_label: str
    venue: str
    venue_type: str
    source: str
    symbol: str
    value_kind: str
    unit_note: str
    price: float
    bid: float
    ask: float
    index_price: float
    funding_rate: float | None
    funding_interval_hours: float | None
    funding_8h: float | None
    volume_24h_usd: float
    open_interest: float
    updated_at: str
    status: str
    error: str = ""

    @property
    def comparable(self) -> bool:
        return self.value_kind == "valuation_index_10x" and self.price > 0

    @property
    def implied_market_cap_b(self) -> float | None:
        factor = IMPLIED_MARKET_CAP_FACTOR_B.get(self.value_kind)
        if factor is None or self.price <= 0:
            return None
        return self.price * factor

    @property
    def bid_value_b(self) -> float | None:
        factor = IMPLIED_MARKET_CAP_FACTOR_B.get(self.value_kind)
        if factor is None:
            return None
        value = self.bid or self.price
        if value <= 0:
            return None
        return value * factor

    @property
    def ask_value_b(self) -> float | None:
        factor = IMPLIED_MARKET_CAP_FACTOR_B.get(self.value_kind)
        if factor is None:
            return None
        value = self.ask or self.price
        if value <= 0:
            return None
        return value * factor


@dataclass
class SpreadOpportunity:
    asset: str
    asset_label: str
    short_venue: str
    short_symbol: str
    short_value_b: float
    long_venue: str
    long_symbol: str
    long_value_b: float
    spread_b: float
    spread_pct: float
    rough_after_fee_pct: float
    note: str


@dataclass
class FundingOpportunity:
    asset: str
    asset_label: str
    short_venue: str
    short_symbol: str
    short_rate_8h: float
    long_venue: str
    long_symbol: str
    long_rate_8h: float
    gap_8h: float
    annualized_pct: float
    note: str


INSTRUMENTS: tuple[Instrument, ...] = (
    Instrument("SPACEX", "SpaceX 估值合约", "OKX", "perp", "okx_swap", "SPACEX-USDT-SWAP", "valuation_index_10x", "价格 x 0.1 = 隐含市值(B USD)"),
    Instrument("SPACEX", "SpaceX 估值合约", "Gate", "pre-market perp", "gate_futures", "SPACEX_USDT", "valuation_index_10x", "价格 x 0.1 = 隐含市值(B USD)"),
    Instrument("SPACEX_PROXY", "SpaceX SPCX 映射", "Binance", "TradFi perp", "binance_futures", "SPCXUSDT", "proxy_share_price", "SPCX价格 x 1.2 = SpaceX隐含市值(B USD)"),
    Instrument("SPACEX_PROXY", "SpaceX SPCX 映射", "Bitget", "perp", "bitget_mix", "SPCXUSDT", "proxy_share_price", "SPCX价格 x 1.2 = SpaceX隐含市值(B USD)"),
    Instrument("SPACEX_PROXY", "SpaceX SPCX 映射", "Gate", "pre-market perp", "gate_futures", "SPCX_USDT", "proxy_share_price", "SPCX价格 x 1.2 = SpaceX隐含市值(B USD)"),
    Instrument("OPENAI", "OpenAI 估值合约", "OKX", "perp", "okx_swap", "OPENAI-USDT-SWAP", "valuation_index_10x", "价格 x 0.1 = 隐含市值(B USD)"),
    Instrument("OPENAI", "OpenAI 估值合约", "Gate", "pre-market perp", "gate_futures", "OPENAI_USDT", "valuation_index_10x", "价格 x 0.1 = 隐含市值(B USD)"),
    Instrument("OPENAI_PROXY", "OpenAI OPN 映射", "Binance", "perp", "binance_futures", "OPNUSDT", "proxy_token_price", "OPN价格 x 1000 = OpenAI隐含市值(B USD)"),
    Instrument("OPENAI_PROXY", "OpenAI OPN 映射", "Bitget", "perp", "bitget_mix", "OPNUSDT", "proxy_token_price", "OPN价格 x 1000 = OpenAI隐含市值(B USD)"),
    Instrument("OPENAI_PROXY", "OpenAI OPN 映射", "Gate", "perp", "gate_futures", "OPN_USDT", "proxy_token_price", "OPN价格 x 1000 = OpenAI隐含市值(B USD)"),
    Instrument("ANTHROPIC", "Anthropic 估值合约", "OKX", "perp", "okx_swap", "ANTHROPIC-USDT-SWAP", "valuation_index_10x", "价格 x 0.1 = 隐含市值(B USD)"),
    Instrument("ANTHROPIC", "Anthropic 估值合约", "Gate", "pre-market perp", "gate_futures", "ANTHROPIC_USDT", "valuation_index_10x", "价格 x 0.1 = 隐含市值(B USD)"),
)

PLACEHOLDER_SOURCES = (
    ("PreStocks", "private-stock venue", "待接入公开API；如果只有网页/登录数据，先做手工CSV槽位"),
    ("Jarsy", "private-stock venue", "未找到稳定公开行情API，先保留源位"),
    ("MSX", "private-stock venue", "未找到稳定公开行情API，先保留源位"),
    ("Deshare", "private-stock venue", "未找到稳定公开行情API，先保留源位"),
    ("Ondo GM", "tokenized stock/RWA", "需要确认合约地址和价格源，避免把钱包映射标的当交易所合约"),
    ("Republic", "private market", "通常偏一级/私募市场报价，和永续盘前合约不能直接混算"),
    ("Binance Wallet", "wallet/on-chain mapping", "需要明确链、合约地址、池子和份额口径"),
    ("OKX Wallet", "wallet/on-chain mapping", "需要明确链、合约地址、池子和份额口径"),
)

PRIMARY_ASSETS = (
    {
        "asset": "SPACEX",
        "proxy": "SPACEX_PROXY",
        "title": "SpaceX",
        "subtitle": "主看 SPACEX 估值合约；SPCX 只作为映射标的单独观察。",
    },
    {
        "asset": "OPENAI",
        "proxy": "OPENAI_PROXY",
        "title": "OpenAI",
        "subtitle": "主看 OPENAI 估值合约；OPN 只作为映射标的单独观察。",
    },
    {
        "asset": "ANTHROPIC",
        "proxy": "",
        "title": "Anthropic",
        "subtitle": "目前只接入 OKX/Gate 的 ANTHROPIC 估值合约。",
    },
)

PRICE_STRONG_PCT = 1.0
PRICE_WATCH_PCT = 0.35
PRICE_THIN_PCT = 0.0
FUNDING_STRONG_APY = 30.0
FUNDING_WATCH_APY = 12.0
FUNDING_THIN_APY = 5.0


def build() -> dict:
    quotes, errors = fetch_quotes()
    spreads = price_spreads(quotes)
    funding = funding_spreads(quotes)
    payload = {
        "generated_at": now_text(),
        "quotes": [asdict(item) for item in quotes],
        "price_spreads": [asdict(item) for item in spreads],
        "funding_spreads": [asdict(item) for item in funding],
        "errors": errors,
    }
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(render(quotes, spreads, funding, errors), encoding="utf-8")
    LATEST_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def fetch_quotes() -> tuple[list[VenueQuote], list[str]]:
    errors: list[str] = []
    quotes: list[VenueQuote] = []
    lock = threading.Lock()

    def fetch_one(inst: Instrument) -> None:
        try:
            quote = fetch_instrument(inst)
            with lock:
                quotes.append(quote)
        except Exception as exc:
            with lock:
                errors.append(f"{inst.venue} {inst.symbol}: {exc.__class__.__name__} {exc}")
                quotes.append(
                    VenueQuote(
                        asset=inst.asset,
                        asset_label=inst.asset_label,
                        venue=inst.venue,
                        venue_type=inst.venue_type,
                        source=inst.source,
                        symbol=inst.symbol,
                        value_kind=inst.value_kind,
                        unit_note=inst.unit_note,
                        price=0.0,
                        bid=0.0,
                        ask=0.0,
                        index_price=0.0,
                        funding_rate=None,
                        funding_interval_hours=None,
                        funding_8h=None,
                        volume_24h_usd=0.0,
                        open_interest=0.0,
                        updated_at="",
                        status="抓取失败",
                        error=f"{exc.__class__.__name__}: {exc}",
                    )
                )

    with ThreadPoolExecutor(max_workers=12) as pool:
        tasks = [pool.submit(fetch_one, inst) for inst in INSTRUMENTS if inst.enabled]
        for task in as_completed(tasks):
            task.result()

    quotes.sort(key=lambda item: (item.asset, item.venue, item.symbol))
    return quotes, errors


def fetch_instrument(inst: Instrument) -> VenueQuote:
    if inst.source == "binance_futures":
        return fetch_binance(inst)
    if inst.source == "okx_swap":
        return fetch_okx(inst)
    if inst.source == "gate_futures":
        return fetch_gate(inst)
    if inst.source == "bitget_mix":
        return fetch_bitget(inst)
    raise ValueError(f"unknown source {inst.source}")


def fetch_binance(inst: Instrument) -> VenueQuote:
    premium = fetch_json(f"{BINANCE_FAPI}/fapi/v1/premiumIndex", {"symbol": inst.symbol})
    book = fetch_json(f"{BINANCE_FAPI}/fapi/v1/ticker/bookTicker", {"symbol": inst.symbol})
    ticker = fetch_json(f"{BINANCE_FAPI}/fapi/v1/ticker/24hr", {"symbol": inst.symbol})
    funding = to_float_or_none(premium.get("lastFundingRate"))
    return quote_from_values(
        inst,
        price=first_positive(premium.get("markPrice"), ticker.get("lastPrice")),
        bid=to_float(book.get("bidPrice")),
        ask=to_float(book.get("askPrice")),
        index_price=to_float(premium.get("indexPrice")),
        funding_rate=funding,
        funding_interval_hours=8.0 if funding is not None else None,
        volume_24h_usd=to_float(ticker.get("quoteVolume")),
        open_interest=0.0,
        updated_at=ms_to_local(premium.get("time")),
        status="Binance U本位公开接口",
    )


def fetch_okx(inst: Instrument) -> VenueQuote:
    ticker = first_data(fetch_json(f"{OKX_API}/api/v5/market/ticker", {"instId": inst.symbol}))
    funding_data = first_data(fetch_json(f"{OKX_API}/api/v5/public/funding-rate", {"instId": inst.symbol}))
    funding = to_float_or_none(funding_data.get("fundingRate"))
    interval = funding_interval_hours(funding_data.get("prevFundingTime"), funding_data.get("fundingTime"))
    return quote_from_values(
        inst,
        price=first_positive(ticker.get("last"), ticker.get("askPx"), ticker.get("bidPx")),
        bid=to_float(ticker.get("bidPx")),
        ask=to_float(ticker.get("askPx")),
        index_price=0.0,
        funding_rate=funding,
        funding_interval_hours=interval if funding is not None else None,
        volume_24h_usd=to_float(ticker.get("volCcy24h")) * first_positive(ticker.get("last"), 1),
        open_interest=0.0,
        updated_at=ms_to_local(ticker.get("ts")),
        status="OKX公开SWAP接口",
    )


def fetch_gate(inst: Instrument) -> VenueQuote:
    contract = fetch_json(f"{GATE_API}/api/v4/futures/usdt/contracts/{inst.symbol}")
    ticker_list = fetch_json(f"{GATE_API}/api/v4/futures/usdt/tickers", {"contract": inst.symbol})
    ticker = ticker_list[0] if isinstance(ticker_list, list) and ticker_list else {}
    funding = to_float_or_none(contract.get("funding_rate") or ticker.get("funding_rate"))
    interval_seconds = to_float(contract.get("funding_interval"))
    interval = interval_seconds / 3600 if interval_seconds else 8.0
    return quote_from_values(
        inst,
        price=first_positive(contract.get("mark_price"), ticker.get("mark_price"), ticker.get("last")),
        bid=to_float(ticker.get("highest_bid")),
        ask=to_float(ticker.get("lowest_ask")),
        index_price=to_float(contract.get("index_price") or ticker.get("index_price")),
        funding_rate=funding,
        funding_interval_hours=interval if funding is not None else None,
        volume_24h_usd=to_float(ticker.get("volume_24h_quote") or ticker.get("volume_24h_settle")),
        open_interest=to_float(contract.get("position_size") or ticker.get("total_size")),
        updated_at=now_text(),
        status=("Gate盘前合约（接口无行情时间戳）" if contract.get("is_pre_market") else "Gate U本位永续（接口无行情时间戳）"),
    )


def fetch_bitget(inst: Instrument) -> VenueQuote:
    ticker_data = fetch_json(
        f"{BITGET_API}/api/v2/mix/market/ticker",
        {"symbol": inst.symbol, "productType": "USDT-FUTURES"},
    )
    ticker = first_data(ticker_data)
    funding_data = fetch_json(
        f"{BITGET_API}/api/v2/mix/market/current-fund-rate",
        {"symbol": inst.symbol, "productType": "USDT-FUTURES"},
    )
    funding_row = first_data(funding_data)
    funding = to_float_or_none(funding_row.get("fundingRate") or ticker.get("fundingRate"))
    interval = to_float(funding_row.get("fundingRateInterval")) or 8.0
    return quote_from_values(
        inst,
        price=first_positive(ticker.get("markPrice"), ticker.get("lastPr")),
        bid=to_float(ticker.get("bidPr")),
        ask=to_float(ticker.get("askPr")),
        index_price=to_float(ticker.get("indexPrice")),
        funding_rate=funding,
        funding_interval_hours=interval if funding is not None else None,
        volume_24h_usd=to_float(ticker.get("usdtVolume") or ticker.get("quoteVolume")),
        open_interest=to_float(ticker.get("holdingAmount")),
        updated_at=ms_to_local(ticker.get("ts")),
        status="Bitget USDT-FUTURES公开接口",
    )


def quote_from_values(
    inst: Instrument,
    *,
    price: float,
    bid: float,
    ask: float,
    index_price: float,
    funding_rate: float | None,
    funding_interval_hours: float | None,
    volume_24h_usd: float,
    open_interest: float,
    updated_at: str,
    status: str,
) -> VenueQuote:
    funding_8h = None
    if funding_rate is not None and funding_interval_hours and funding_interval_hours > 0:
        funding_8h = funding_rate * (8.0 / funding_interval_hours)
    return VenueQuote(
        asset=inst.asset,
        asset_label=inst.asset_label,
        venue=inst.venue,
        venue_type=inst.venue_type,
        source=inst.source,
        symbol=inst.symbol,
        value_kind=inst.value_kind,
        unit_note=inst.unit_note,
        price=price,
        bid=bid,
        ask=ask,
        index_price=index_price,
        funding_rate=funding_rate,
        funding_interval_hours=funding_interval_hours,
        funding_8h=funding_8h,
        volume_24h_usd=volume_24h_usd,
        open_interest=open_interest,
        updated_at=updated_at,
        status=status,
    )


def price_spreads(quotes: list[VenueQuote]) -> list[SpreadOpportunity]:
    grouped: dict[str, list[VenueQuote]] = {}
    for quote in quotes:
        if quote.value_kind == "valuation_index_10x" and quote.comparable and quote.ask_value_b and quote.bid_value_b:
            grouped.setdefault(quote.asset, []).append(quote)

    opportunities: list[SpreadOpportunity] = []
    for asset, items in grouped.items():
        if len(items) < 2:
            continue
        best: SpreadOpportunity | None = None
        for high in items:
            for low in items:
                if high.venue == low.venue:
                    continue
                high_value = high.bid_value_b or high.implied_market_cap_b or 0.0
                low_value = low.ask_value_b or low.implied_market_cap_b or 0.0
                spread = high_value - low_value
                mid = (high_value + low_value) / 2 if high_value and low_value else 0.0
                spread_pct = spread / mid * 100 if mid else 0.0
                candidate = SpreadOpportunity(
                    asset=asset,
                    asset_label=items[0].asset_label,
                    short_venue=high.venue,
                    short_symbol=high.symbol,
                    short_value_b=high_value,
                    long_venue=low.venue,
                    long_symbol=low.symbol,
                    long_value_b=low_value,
                    spread_b=spread,
                    spread_pct=spread_pct,
                    rough_after_fee_pct=spread_pct - DEFAULT_ROUND_TRIP_FEE_PCT,
                    note="只比较同一估值指数口径；用高bid做空、低ask做多。执行前还要看盘口厚度和限价成交。",
                )
                if best is None or candidate.rough_after_fee_pct > best.rough_after_fee_pct:
                    best = candidate
        if best:
            opportunities.append(best)
    opportunities.sort(key=lambda item: item.rough_after_fee_pct, reverse=True)
    return opportunities


def funding_spreads(quotes: list[VenueQuote]) -> list[FundingOpportunity]:
    grouped: dict[str, list[VenueQuote]] = {}
    for quote in quotes:
        if quote.funding_8h is not None and quote.price > 0:
            grouped.setdefault(quote.asset, []).append(quote)

    rows: list[FundingOpportunity] = []
    for asset, items in grouped.items():
        if len(items) < 2:
            continue
        high = max(items, key=lambda item: item.funding_8h or 0.0)
        low = min(items, key=lambda item: item.funding_8h or 0.0)
        gap = (high.funding_8h or 0.0) - (low.funding_8h or 0.0)
        if high is low or abs(gap) < 0.00000001:
            continue
        rows.append(
            FundingOpportunity(
                asset=asset,
                asset_label=items[0].asset_label,
                short_venue=high.venue,
                short_symbol=high.symbol,
                short_rate_8h=high.funding_8h or 0.0,
                long_venue=low.venue,
                long_symbol=low.symbol,
                long_rate_8h=low.funding_8h or 0.0,
                gap_8h=gap,
                annualized_pct=gap * 3 * 365 * 100,
                note="正资金费通常多头付空头；资金费套利方向是空高费率腿、多低费率腿。",
            )
        )
    rows.sort(key=lambda item: abs(item.gap_8h), reverse=True)
    return rows


def proxy_price_spreads(quotes: list[VenueQuote], asset: str) -> list[SpreadOpportunity]:
    items = [
        item
        for item in quotes
        if item.asset == asset and item.price > 0 and item.bid_value_b is not None and item.ask_value_b is not None
    ]
    rows: list[SpreadOpportunity] = []
    for high in items:
        for low in items:
            if high.venue == low.venue:
                continue
            high_value = high.bid_value_b or 0.0
            low_value = low.ask_value_b or 0.0
            spread = high_value - low_value
            mid = (high_value + low_value) / 2 if high_value and low_value else 0.0
            spread_pct = spread / mid * 100 if mid else 0.0
            rows.append(
                SpreadOpportunity(
                    asset=asset,
                    asset_label=items[0].asset_label if items else asset,
                    short_venue=high.venue,
                    short_symbol=high.symbol,
                    short_value_b=high_value,
                    long_venue=low.venue,
                    long_symbol=low.symbol,
                    long_value_b=low_value,
                    spread_b=spread,
                    spread_pct=spread_pct,
                    rough_after_fee_pct=spread_pct - DEFAULT_ROUND_TRIP_FEE_PCT,
                    note="同一映射标的跨所价差；用映射公式换成隐含市值后比较，不和估值合约混算。",
                )
            )
    rows.sort(key=lambda item: item.rough_after_fee_pct, reverse=True)
    return rows


def spread_grade(row: SpreadOpportunity | None) -> tuple[str, str, str]:
    if row is None:
        return "不可做", "danger", "缺少两家以上同口径估值合约，不能算价差。"
    if row.rough_after_fee_pct >= PRICE_STRONG_PCT:
        return "值得复核", "good", "价差扣粗略双边摩擦后仍较大，下一步查盘口深度、下单限制和两边可成交量。"
    if row.rough_after_fee_pct >= PRICE_WATCH_PCT:
        return "小机会", "watch", "有正价差但不厚，只有在限价成交、低手续费、盘口够深时才可能有意义。"
    if row.rough_after_fee_pct > PRICE_THIN_PCT:
        return "太薄", "muted-box", "扣粗略摩擦后只剩很薄空间，容易被滑点和挂单失败吃掉。"
    return "不做", "danger", "扣粗略双边摩擦后为负，不应当当成价差套利。"


def funding_grade(row: FundingOpportunity | None) -> tuple[str, str, str]:
    if row is None:
        return "不可做", "danger", "缺少两家以上资金费数据，或者资金费没有明显差异。"
    if row.annualized_pct >= FUNDING_STRONG_APY:
        return "值得复核", "good", "资金费差较厚，但必须同时确认价格基差、强平线、持仓上限和资金费是否会快速回落。"
    if row.annualized_pct >= FUNDING_WATCH_APY:
        return "观察", "watch", "资金费差能看，但不足以单独下结论；更适合配合价差或已有仓位。"
    if row.annualized_pct >= FUNDING_THIN_APY:
        return "很薄", "muted-box", "资金费毛值偏低，通常不够覆盖执行和资金占用，除非已经有自然对冲仓位。"
    return "不做", "danger", "资金费差太小，暂时没有资金费套利价值。"


def funding_status_for_quotes(quotes: list[VenueQuote], asset: str, row: FundingOpportunity | None) -> tuple[str, str, str]:
    rows = [item for item in quotes if item.asset == asset and item.price > 0]
    with_funding = [item for item in rows if item.funding_8h is not None]
    nonzero = [item for item in with_funding if abs(item.funding_8h or 0.0) > 0.00000001]
    if row is not None:
        return funding_grade(row)
    if len(with_funding) >= 2 and not nonzero:
        venues = "、".join(f"{item.venue} {item.symbol}" for item in with_funding)
        return "已接到，当前为0", "muted-box", f"已从接口拿到资金费字段，但当前均为 0：{venues}。所以没有资金费套利，不是没接到。"
    if len(with_funding) == 1:
        item = with_funding[0]
        return "只有单边", "muted-box", f"只拿到 {item.venue} {item.symbol} 的资金费，缺另一边，不能构成跨所资金费套利。"
    if rows:
        return "未接到", "danger", "有报价但没有资金费字段，需继续找交易所的专门资金费接口或网页数据。"
    return "无报价", "danger", "没有该标的报价，不能判断资金费。"


def spread_action(row: SpreadOpportunity | None) -> str:
    if row is None:
        return "不动：没有可比双边。"
    return (
        f"价差套法：做空 {row.short_venue} {row.short_symbol}，"
        f"做多 {row.long_venue} {row.long_symbol}；当前按 bid/ask 估算毛价差 "
        f"{row.spread_pct:+.2f}%，粗扣费后 {row.rough_after_fee_pct:+.2f}%。"
    )


def funding_action(row: FundingOpportunity | None) -> str:
    if row is None:
        return "不动：没有可比资金费。"
    return (
        f"资金费套法：做空 {row.short_venue} {row.short_symbol} "
        f"({row.short_rate_8h * 100:+.4f}%/8h)，做多 {row.long_venue} {row.long_symbol} "
        f"({row.long_rate_8h * 100:+.4f}%/8h)；可捕获差 {row.gap_8h * 100:+.4f}%/8h，"
        f"年化毛值 {row.annualized_pct:+.1f}%。"
    )


def funding_action_for_asset(quotes: list[VenueQuote], asset: str, row: FundingOpportunity | None) -> str:
    if row is not None:
        return funding_action(row)
    rows = [item for item in quotes if item.asset == asset and item.funding_8h is not None]
    if rows:
        rates = "；".join(f"{item.venue} {item.symbol} {item.funding_8h * 100:+.4f}%/8h" for item in rows)
        return f"不动：接口已返回资金费，但当前没有可套差值。{rates}。"
    return "不动：当前没有拿到可用资金费。"


def quote_rows_for_asset(quotes: list[VenueQuote], asset: str) -> str:
    rows = [row for row in quotes if row.asset == asset]
    return "".join(render_quote_row(row) for row in rows) or "<tr><td colspan='9'>暂无报价。</td></tr>"


def proxy_spread_rows(rows: list[SpreadOpportunity]) -> str:
    return "".join(
        f"""
        <tr>
          <td>{esc(row.short_venue)}<br><small>{esc(row.short_symbol)}</small></td>
          <td>{row.short_value_b:.2f}B</td>
          <td>{esc(row.long_venue)}<br><small>{esc(row.long_symbol)}</small></td>
          <td>{row.long_value_b:.2f}B</td>
          <td class="{cls(row.spread_pct)}">{row.spread_b:+.2f}B<br><small>{row.spread_pct:+.2f}%</small></td>
          <td class="{cls(row.rough_after_fee_pct)}">{row.rough_after_fee_pct:+.2f}%</td>
          <td>{esc(row.note)}</td>
        </tr>
        """
        for row in rows[:5]
    ) or "<tr><td colspan='7'>暂无两家以上同映射标的报价。</td></tr>"


def asset_section(
    spec: dict,
    quotes: list[VenueQuote],
    spreads: list[SpreadOpportunity],
    funding: list[FundingOpportunity],
) -> str:
    asset = spec["asset"]
    proxy = spec.get("proxy") or ""
    spread = next((item for item in spreads if item.asset == asset), None)
    fund = next((item for item in funding if item.asset == asset), None)
    proxy_fund = next((item for item in funding if item.asset == proxy), None) if proxy else None
    spread_label, spread_class, spread_reason = spread_grade(spread)
    fund_label, fund_class, fund_reason = funding_status_for_quotes(quotes, asset, fund)
    proxy_label, proxy_class, proxy_reason = funding_status_for_quotes(quotes, proxy, proxy_fund) if proxy else ("无映射腿", "muted-box", "当前没有单独映射标的。")

    proxy_rows = quote_rows_for_asset(quotes, proxy) if proxy else "<tr><td colspan='9'>这个标的当前没有映射腿。</td></tr>"
    proxy_action = funding_action_for_asset(quotes, proxy, proxy_fund) if proxy else "映射腿只单独观察，不和估值合约混做价差。"
    proxy_spreads = proxy_price_spreads(quotes, proxy) if proxy else []

    return f"""
    <section class="asset-panel {esc(asset.lower())}">
      <div class="asset-head">
        <div>
          <h2>{esc(spec['title'])}</h2>
          <p>{esc(spec['subtitle'])}</p>
        </div>
        <div class="asset-badges">
          <span class="{esc(spread_class)}">价差：{esc(spread_label)}</span>
          <span class="{esc(fund_class)}">估值合约资金费：{esc(fund_label)}</span>
          <span class="{esc(proxy_class)}">映射腿资金费：{esc(proxy_label)}</span>
        </div>
      </div>
      <div class="decision-grid">
        <article class="decision-card {esc(spread_class)}">
          <strong>价差套利</strong>
          <b>{esc(spread_label)}</b>
          <p>{esc(spread_action(spread))}</p>
          <small>{esc(spread_reason)}</small>
        </article>
        <article class="decision-card {esc(fund_class)}">
          <strong>估值合约资金费</strong>
          <b>{esc(fund_label)}</b>
          <p>{esc(funding_action_for_asset(quotes, asset, fund))}</p>
          <small>{esc(fund_reason)}</small>
        </article>
        <article class="decision-card {esc(proxy_class)}">
          <strong>映射腿资金费</strong>
          <b>{esc(proxy_label)}</b>
          <p>{esc(proxy_action)}</p>
          <small>{esc(proxy_reason)}</small>
        </article>
      </div>
      <h3>估值合约报价</h3>
      <table>
        <thead><tr><th>标的</th><th>交易所</th><th>合约</th><th>交易所价格</th><th>隐含市值</th><th>换算说明</th><th>买一/卖一</th><th>资金费</th><th>24h额</th><th>状态</th></tr></thead>
        <tbody>{quote_rows_for_asset(quotes, asset)}</tbody>
      </table>
      <h3>映射标的，只单独观察</h3>
      <table>
        <thead><tr><th>标的</th><th>交易所</th><th>合约</th><th>交易所价格</th><th>隐含市值</th><th>换算说明</th><th>买一/卖一</th><th>资金费</th><th>24h额</th><th>状态</th></tr></thead>
        <tbody>{proxy_rows}</tbody>
      </table>
      <h3>映射腿跨所价差</h3>
      <table>
        <thead><tr><th>做空腿</th><th>做空隐含市值</th><th>做多腿</th><th>做多隐含市值</th><th>市值价差</th><th>粗扣费后</th><th>说明</th></tr></thead>
        <tbody>{proxy_spread_rows(proxy_spreads)}</tbody>
      </table>
    </section>
    """


def render(
    quotes: list[VenueQuote],
    spreads: list[SpreadOpportunity],
    funding: list[FundingOpportunity],
    errors: list[str],
) -> str:
    comparable_quotes = [item for item in quotes if item.comparable]
    proxy_quotes = [item for item in quotes if not item.comparable]
    best_spread = spreads[0] if spreads else None
    best_funding = funding[0] if funding else None
    best_spread_label, _, best_spread_reason = spread_grade(best_spread)
    best_funding_label, _, best_funding_reason = funding_grade(best_funding)
    cards = [
        ("可比估值标的", str(len(comparable_quotes)), "SPACEX/OPENAI/ANTHROPIC 同估值指数口径"),
        ("映射标的", str(len(proxy_quotes)), "SPCX、OPN 等按固定公式显示隐含市值，但不和估值合约混算"),
        ("最强价差", f"{best_spread.rough_after_fee_pct:+.2f}%" if best_spread else "无", f"{best_spread.asset_label}：{best_spread_label}。{best_spread_reason}" if best_spread else "暂无可比价差"),
        ("最强资金费", f"{best_funding.annualized_pct:+.1f}%" if best_funding else "无", f"{best_funding.asset_label}：{best_funding_label}。{best_funding_reason}" if best_funding else "暂无跨所资金费差"),
    ]
    card_html = "".join(
        f"<section class='card'><span>{esc(title)}</span><b class='{cls_text(value)}'>{esc(value)}</b><p>{esc(note)}</p></section>"
        for title, value, note in cards
    )
    asset_sections = "".join(asset_section(spec, quotes, spreads, funding) for spec in PRIMARY_ASSETS)
    spread_rows = "".join(
        f"""
        <tr>
          <td><b>{esc(row.asset_label)}</b><small>{esc(row.asset)}</small></td>
          <td>{esc(row.short_venue)}<br><small>{esc(row.short_symbol)}</small></td>
          <td>{row.short_value_b:.2f}B</td>
          <td>{esc(row.long_venue)}<br><small>{esc(row.long_symbol)}</small></td>
          <td>{row.long_value_b:.2f}B</td>
          <td class="{cls(row.spread_pct)}">{row.spread_b:+.2f}B<br><small>{row.spread_pct:+.2f}%</small></td>
          <td class="{cls(row.rough_after_fee_pct)}">{row.rough_after_fee_pct:+.2f}%</td>
          <td>{esc(row.note)}</td>
        </tr>
        """
        for row in spreads
    ) or "<tr><td colspan='8'>当前没有两家以上同口径估值合约可比。</td></tr>"
    funding_rows = "".join(
        f"""
        <tr>
          <td><b>{esc(row.asset_label)}</b><small>{esc(row.asset)}</small></td>
          <td>{esc(row.short_venue)}<br><small>{esc(row.short_symbol)}</small></td>
          <td class="{cls(row.short_rate_8h)}">{row.short_rate_8h * 100:+.4f}%</td>
          <td>{esc(row.long_venue)}<br><small>{esc(row.long_symbol)}</small></td>
          <td class="{cls(row.long_rate_8h)}">{row.long_rate_8h * 100:+.4f}%</td>
          <td class="{cls(row.gap_8h)}">{row.gap_8h * 100:+.4f}%</td>
          <td class="{cls(row.annualized_pct)}">{row.annualized_pct:+.1f}%</td>
          <td>{esc(row.note)}</td>
        </tr>
        """
        for row in funding
    ) or "<tr><td colspan='8'>当前没有两家以上资金费数据可比。</td></tr>"
    placeholder_rows = "".join(
        f"<tr><td>{esc(name)}</td><td>{esc(kind)}</td><td>{esc(note)}</td></tr>"
        for name, kind, note in PLACEHOLDER_SOURCES
    )
    error_rows = "".join(f"<li>{esc(item)}</li>" for item in errors) or "<li>暂无抓取错误。</li>"
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>盘前股权合约套利看板</title>
  <style>
    :root {{ --bg:#f6f7f4; --panel:#fff; --ink:#141915; --muted:#67706a; --line:#dfe5dc; --green:#087b4b; --red:#b94343; --blue:#2357b8; --amber:#9a6a05; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; background:var(--bg); color:var(--ink); font-family:"Microsoft YaHei", Arial, sans-serif; }}
    header {{ padding:24px 30px 16px; background:#e8efe5; border-bottom:1px solid var(--line); }}
    h1 {{ margin:0 0 8px; font-size:26px; letter-spacing:0; }}
    .sub {{ color:var(--muted); line-height:1.6; font-size:14px; max-width:1100px; }}
    main {{ padding:20px 30px 42px; display:grid; gap:16px; }}
    .cards {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; }}
    .card,.panel {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; box-shadow:0 8px 22px rgba(30,45,30,.06); }}
    .card {{ padding:14px; min-height:118px; }}
    .card span {{ display:block; color:var(--muted); margin-bottom:8px; }}
    .card b {{ display:block; font-size:30px; margin-bottom:8px; overflow-wrap:anywhere; }}
    .card p {{ margin:0; color:var(--muted); font-size:13px; line-height:1.5; }}
    .panel,.asset-panel {{ padding:17px; overflow:auto; }}
    h2 {{ margin:0 0 12px; font-size:19px; }}
    h3 {{ margin:18px 0 10px; font-size:15px; color:var(--muted); }}
    table {{ width:100%; border-collapse:collapse; font-size:14px; }}
    th,td {{ padding:10px 8px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; }}
    th {{ color:var(--muted); white-space:nowrap; }}
    small {{ display:block; color:var(--muted); margin-top:4px; line-height:1.4; }}
    .pos {{ color:var(--green); font-weight:700; }}
    .neg {{ color:var(--red); font-weight:700; }}
    .muted {{ color:var(--muted); }}
    .warn {{ color:var(--amber); font-weight:700; }}
    .asset-panel {{ background:var(--panel); border:1px solid var(--line); border-radius:10px; box-shadow:0 8px 22px rgba(30,45,30,.06); }}
    .asset-head {{ display:flex; justify-content:space-between; gap:14px; align-items:flex-start; margin-bottom:14px; }}
    .asset-head p {{ margin:0; color:var(--muted); line-height:1.5; }}
    .asset-badges {{ display:flex; flex-wrap:wrap; gap:8px; justify-content:flex-end; }}
    .asset-badges span {{ border:1px solid var(--line); border-radius:999px; padding:7px 10px; font-size:13px; background:#f8faf7; }}
    .decision-grid {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:12px; margin-bottom:8px; }}
    .decision-card {{ border:1px solid var(--line); border-radius:9px; padding:13px; background:#fbfcfa; min-height:148px; }}
    .decision-card strong {{ display:block; color:var(--muted); margin-bottom:7px; }}
    .decision-card b {{ display:block; font-size:22px; margin-bottom:8px; }}
    .decision-card p {{ margin:0 0 8px; line-height:1.5; }}
    .decision-card small {{ display:block; }}
    .good {{ color:var(--green); border-color:#a8d7c2 !important; background:#f0fbf5 !important; font-weight:700; }}
    .watch {{ color:var(--amber); border-color:#ead39a !important; background:#fffaf0 !important; font-weight:700; }}
    .danger {{ color:var(--red); border-color:#e4b0a8 !important; background:#fff5f3 !important; font-weight:700; }}
    .muted-box {{ color:var(--muted); border-color:var(--line) !important; background:#f8faf7 !important; }}
    ul {{ margin:0; padding-left:20px; }}
    li {{ margin:8px 0; line-height:1.5; }}
    @media (max-width:1100px) {{ .cards {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} .decision-grid {{ grid-template-columns:1fr; }} .asset-head {{ display:block; }} .asset-badges {{ justify-content:flex-start; margin-top:10px; }} }}
    @media (max-width:760px) {{ header,main {{ padding-left:14px; padding-right:14px; }} .cards {{ grid-template-columns:1fr; }} table {{ font-size:13px; }} }}
  </style>
</head>
<body>
  <header>
    <h1>盘前股权合约套利看板</h1>
    <div class="sub">生成时间：{esc(now_text())}。所有价格都原样显示交易所报价；同时按公式口径换算隐含市值：估值合约价格 x 0.1，SPCX价格 x 1.2，OPN价格 x 1000。映射标的只和同一映射标的比，不和估值合约硬混。</div>
  </header>
  <main>
    <div class="cards">{card_html}</div>
    {asset_sections}
    <section class="panel">
      <h2>全局同口径估值价差排序</h2>
      <table>
        <thead><tr><th>标的</th><th>做空腿</th><th>做空隐含市值</th><th>做多腿</th><th>做多隐含市值</th><th>毛价差</th><th>粗扣费后</th><th>说明</th></tr></thead>
        <tbody>{spread_rows}</tbody>
      </table>
      <p class="muted">粗扣费后 = 毛价差百分比 - {DEFAULT_ROUND_TRIP_FEE_PCT:.2f}% 双边吃单摩擦估计；实际要按你账户费率、盘口深度和限价成交重新算。</p>
    </section>
    <section class="panel">
      <h2>全局资金费差价排序</h2>
      <table>
        <thead><tr><th>标的</th><th>做空高费率腿</th><th>8h费率</th><th>做多低费率腿</th><th>8h费率</th><th>8h差</th><th>年化毛值</th><th>说明</th></tr></thead>
        <tbody>{funding_rows}</tbody>
      </table>
      <p class="muted">资金费统一换算成 8 小时口径；正资金费通常是多头付空头，所以“空高费率、多低费率”只是资金费方向，还要同时看价差、强平线和持仓上限。</p>
    </section>
    <section class="panel">
      <h2>待接入和不能混算的源</h2>
      <table>
        <thead><tr><th>来源</th><th>类型</th><th>处理方式</th></tr></thead>
        <tbody>{placeholder_rows}</tbody>
      </table>
    </section>
    <section class="panel">
      <h2>执行纪律</h2>
      <ul>
        <li>先确认合约名和口径，再看价差；低价映射标的不能直接和估值指数合约做价差。</li>
        <li>盘前私募股权合约可能有交易时段、指数源、结算规则、涨跌停、强平和地区限制，不能按普通币本位永续理解。</li>
        <li>价差套利要用 bid/ask，不用 last；资金费套利要统一周期，不能把 4h 和 8h 直接相减。</li>
        <li>如果后面接入 PreStocks、Jarsy、MSX、Deshare、Ondo GM、Republic，必须先登记“价格代表什么”，再进入可比表。</li>
        {error_rows}
      </ul>
    </section>
  </main>
</body>
</html>"""


def render_quote_row(row: VenueQuote) -> str:
    if row.implied_market_cap_b is not None:
        implied = f"{row.implied_market_cap_b:.2f}B"
        method = row.unit_note
    else:
        implied = "无明确换算"
        method = row.unit_note
    funding = "无"
    if row.funding_8h is not None:
        funding = f"{row.funding_8h * 100:+.4f}% / 8h"
    status = row.error or row.status
    return f"""
    <tr>
      <td><b>{esc(row.asset_label)}</b><small>{esc(row.asset)}</small></td>
      <td>{esc(row.venue)}<small>{esc(row.venue_type)}</small></td>
      <td>{esc(row.symbol)}<small>{esc(row.source)}</small></td>
      <td>{row.price:.6g}</td>
      <td>{esc(implied)}</td>
      <td>{esc(method)}</td>
      <td>{row.bid:.6g} / {row.ask:.6g}</td>
      <td class="{cls(row.funding_8h or 0.0)}">{esc(funding)}</td>
      <td>{format_usd(row.volume_24h_usd)}</td>
      <td>{esc(status)}<small>{esc(row.updated_at)}</small></td>
    </tr>
    """


def fetch_json(url: str, params: dict | None = None) -> dict | list:
    full_url = url if not params else f"{url}?{urlencode(params)}"
    request = Request(full_url, headers={"User-Agent": "longwang-preipo-arbitrage/0.1"})
    with urlopen(request, timeout=12) as response:
        return json.loads(response.read().decode("utf-8"))


def first_data(payload: dict | list) -> dict:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list) and data:
            return data[0]
        if isinstance(data, dict):
            return data
    if isinstance(payload, list) and payload:
        return payload[0] if isinstance(payload[0], dict) else {}
    return {}


def first_positive(*values) -> float:
    for value in values:
        parsed = to_float(value)
        if parsed > 0:
            return parsed
    return 0.0


def to_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def to_float_or_none(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def funding_interval_hours(prev_ms, next_ms) -> float:
    prev_value = to_float(prev_ms)
    next_value = to_float(next_ms)
    if prev_value > 0 and next_value > prev_value:
        return (next_value - prev_value) / 1000 / 3600
    return 8.0


def ms_to_local(value) -> str:
    raw = to_float(value)
    if raw <= 0:
        return ""
    return datetime.fromtimestamp(raw / 1000, UTC).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def unix_to_local(value) -> str:
    raw = to_float(value)
    if raw <= 0:
        return ""
    return datetime.fromtimestamp(raw, UTC).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def format_usd(value: float) -> str:
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value / 1_000:.1f}K"
    if value > 0:
        return f"${value:.0f}"
    return "无"


def esc(value: object) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def cls(value: float) -> str:
    return "pos" if value >= 0 else "neg"


def cls_text(value: str) -> str:
    return "pos" if str(value).startswith("+") else ("neg" if str(value).startswith("-") else "")


if __name__ == "__main__":
    build()
