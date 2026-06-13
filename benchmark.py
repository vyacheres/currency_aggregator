"""
Последовательный эталон (baseline) для сравнения с гибридным конвейером.

Делает то же самое, что main.py, но полностью последовательно, в одном
потоке/процессе:
    для каждого запроса -> синхронный HTTP-запрос (requests) -> CPU-bound
    расчет (processor.process_rate) -> запись в SQLite (storage.save_rate).

Используется как точка отсчета для расчета ускорения гибридного конвейера.
"""

from __future__ import annotations

import logging
import random
import time

import requests

import processor
import storage
from sources import ALL_REQUESTS

logger = logging.getLogger("currency_aggregator.benchmark")

BASELINE_DB_PATH = "currency_data_baseline.db"
REQUEST_TIMEOUT = 5.0


def _fetch_one_sync(req: dict) -> dict:
    """Синхронный аналог Aggregator._fetch_one (без ретраев - для простоты эталона)."""
    if req["kind"] == "mock":
        time.sleep(random.uniform(0.1, 0.5))
        rate = random.uniform(0.5, 150.0)
        return {**req, "rate": rate}

    try:
        resp = requests.get(req["url"], timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()

        if req["kind"] == "frankfurter":
            rate = float(data["rates"][req["target"]])
        else:  # binance
            rate = float(data["price"])

        return {**req, "rate": rate}
    except Exception as exc:  # noqa: BLE001 - эталон должен продолжать работу
        logger.error("cid=%s ошибка запроса (%s), используем fallback", req["cid"], exc)
        rate = random.uniform(0.5, 150.0)
        return {**req, "rate": rate, "kind": f"{req['kind']}_fallback"}


def run_sequential_baseline(db_path: str = BASELINE_DB_PATH) -> float:
    """Выполняет весь конвейер последовательно и возвращает время в секундах."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(processName)s:%(threadName)s] %(levelname)s %(name)s: %(message)s",
    )

    storage.init_db(db_path)

    start = time.perf_counter()

    for req in ALL_REQUESTS:
        raw = _fetch_one_sync(req)
        record = processor.process_rate(raw)
        storage.save_rate(record, db_path=db_path)

    elapsed = time.perf_counter() - start
    logger.info("Последовательный эталон выполнен за %.2f сек (%d запросов)", elapsed, len(ALL_REQUESTS))
    return elapsed


if __name__ == "__main__":
    run_sequential_baseline()
