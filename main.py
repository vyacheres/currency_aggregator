"""
Точка входа гибридного приложения.

Запускает конвейер: asyncio (сбор данных) -> ProcessPoolExecutor (CPU-bound
обработка) -> ThreadPoolExecutor (запись в SQLite).

Запуск:
    python main.py

Остановка:
    Ctrl+C - graceful shutdown (все процессы и потоки корректно завершаются).
"""

from __future__ import annotations

import asyncio
import logging

from aggregator import Aggregator
from sources import ALL_REQUESTS


def setup_logging() -> None:
    """
    Настраивает логирование так, чтобы в каждой строке были видны:
    - имя процесса (processName) - важно для ProcessPoolExecutor,
    - имя потока (threadName) - важно для ThreadPoolExecutor,
    - сообщение, в котором мы передаем correlation_id (cid).
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(processName)s:%(threadName)s] %(levelname)s %(name)s: %(message)s",
    )


def main() -> None:
    setup_logging()
    logger = logging.getLogger("currency_aggregator.main")

    aggregator = Aggregator(ALL_REQUESTS)

    try:
        elapsed = asyncio.run(aggregator.run())
        logger.info("Готово. Время выполнения гибридного конвейера: %.2f сек", elapsed)
    except KeyboardInterrupt:
        logger.warning("Получен Ctrl+C - выполняется graceful shutdown...")
        aggregator.shutdown()
        logger.info("Все процессы и потоки остановлены.")


if __name__ == "__main__":
    main()
