"""
Список источников данных для конвейера.

Формирует единый список запросов (ALL_REQUESTS) из трёх источников:
1. Frankfurter API (реальные курсы валют ЕЦБ) — 20 запросов.
2. Binance Public Ticker (криптовалюты) — 10 запросов.
3. Mock-запросы (имитация нестабильных источников) — 20 запросов.

Итого: 20 + 10 + 20 = 50 запросов.

Каждый элемент списка — словарь с одинаковой структурой:
{
    "kind": "frankfurter" | "binance" | "mock",
    "url": str | None,
    "pair": str,
    "base": str,
    "target": str,
    "cid": str,   # correlation id для логирования
}
"""

from __future__ import annotations

# Курсовые пары для Frankfurter API (ЕЦБ).
FX_PAIRS = [
    ("USD", "EUR"), ("USD", "GBP"), ("USD", "JPY"), ("USD", "CHF"),
    ("EUR", "USD"), ("EUR", "GBP"), ("EUR", "JPY"), ("EUR", "CHF"),
    ("GBP", "USD"), ("GBP", "EUR"), ("GBP", "JPY"), ("GBP", "CHF"),
    ("JPY", "USD"), ("JPY", "EUR"), ("JPY", "GBP"), ("JPY", "CHF"),
    ("CHF", "USD"), ("CHF", "EUR"), ("CHF", "GBP"), ("CHF", "JPY"),
]

# Тикеры криптовалют для Binance Public API.
BINANCE_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT",
]

# Пары для мок-запросов (имитация нестабильных источников без сети).
MOCK_PAIRS = [
    ("AUD", "USD"), ("CAD", "USD"), ("NZD", "USD"), ("SEK", "USD"),
    ("NOK", "USD"), ("MXN", "USD"), ("SGD", "USD"), ("HKD", "USD"),
    ("ZAR", "USD"), ("TRY", "USD"), ("PLN", "USD"), ("DKK", "USD"),
    ("ILS", "USD"), ("KRW", "USD"), ("THB", "USD"), ("CZK", "USD"),
    ("HUF", "USD"), ("RON", "USD"), ("BRL", "USD"), ("INR", "USD"),
]


def build_requests() -> list[dict]:
    """Собирает единый список из 50 запросов для конвейера."""
    reqs: list[dict] = []

    # 1. Frankfurter (20 запросов)
    for base, target in FX_PAIRS:
        reqs.append({
            "kind": "frankfurter",
            "url": f"https://api.frankfurter.app/latest?from={base}&to={target}",
            "pair": f"{base}/{target}",
            "base": base,
            "target": target,
            "cid": f"FX-{base}-{target}",
        })

    # 2. Binance (10 запросов)
    for symbol in BINANCE_SYMBOLS:
        reqs.append({
            "kind": "binance",
            "url": f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}",
            "pair": symbol,
            "base": symbol[:-4],   # например BTC из BTCUSDT
            "target": symbol[-4:], # USDT
            "cid": f"BN-{symbol}",
        })

    # 3. Mock (20 запросов)
    for base, target in MOCK_PAIRS:
        reqs.append({
            "kind": "mock",
            "url": None,
            "pair": f"{base}/{target}",
            "base": base,
            "target": target,
            "cid": f"MOCK-{base}-{target}",
        })

    return reqs


ALL_REQUESTS = build_requests()
