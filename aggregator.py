"""
Основной класс гибридного конвейера.

Архитектура (линейный трёхэтапный конвейер):

  Этап 1 (asyncio/aiohttp)        Этап 2 (ProcessPoolExecutor)        Этап 3 (ThreadPoolExecutor)
  ------------------------         -----------------------------       ------------------------------
  collect()                         dispatcher()                        store_consumer()
  - асинхронные HTTP-запросы        - забирает "сырые" данные из        - забирает готовые записи из
    (aiohttp + as_completed)          asyncio.Queue                       multiprocessing.Queue (через
  - складывает результаты           - считает numpy.mean/std в           run_in_executor, т.к. mp.Queue.get
    в asyncio.Queue                   отдельном процессе                 блокирующий)
                                       (run_in_executor + ProcessPool)   - пишет в SQLite через
                                     - кладёт результат в                  run_in_executor + ThreadPoolExecutor
                                       multiprocessing.Queue

Завершение всех трёх этапов происходит только через poison pills (None),
передаваемые по очередям.
"""

from __future__ import annotations

import asyncio
import logging
import multiprocessing
import random
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import aiohttp

import processor
import storage

logger = logging.getLogger("currency_aggregator")

REQUEST_TIMEOUT = 5.0       # секунд, согласно ТЗ
MAX_RETRIES = 2             # количество повторных попыток (помимо первой)
BACKOFF_BASE = 1.0          # секунд, база для экспоненциальной задержки


class Aggregator:
    """Связывает три парадигмы конкурентности в единый конвейер."""

    def __init__(
        self,
        requests: list[dict],
        db_path: str = storage.DB_PATH,
        num_processes: int | None = None,
        num_threads: int = 4,
    ) -> None:
        self.requests = requests
        self.db_path = db_path
        self.num_processes = num_processes or min(4, multiprocessing.cpu_count())
        self.num_threads = num_threads

        # Очереди-мосты между этапами
        self.raw_queue: asyncio.Queue = asyncio.Queue()
        self.result_queue: multiprocessing.Queue = multiprocessing.Queue()

        self.process_pool: ProcessPoolExecutor | None = None
        self.thread_pool: ThreadPoolExecutor | None = None

        self.saved_count = 0

    # ------------------------------------------------------------------
    # Этап 1: сбор данных (asyncio + aiohttp)
    # ------------------------------------------------------------------
    async def _fetch_one(self, session: aiohttp.ClientSession, req: dict) -> dict:
        """Выполняет один запрос с таймаутом и повторами (экспоненциальная задержка)."""
        cid = req["cid"]

        # Mock-источник - имитация нестабильного API, без сети
        if req["kind"] == "mock":
            await asyncio.sleep(random.uniform(0.1, 0.5))
            rate = random.uniform(0.5, 150.0)
            logger.info("cid=%s mock-источник, rate=%.4f", cid, rate)
            return {**req, "rate": rate}

        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)

        for attempt in range(MAX_RETRIES + 1):
            try:
                async with session.get(req["url"], timeout=timeout) as resp:
                    if resp.status == 429:
                        delay = BACKOFF_BASE * (attempt + 1)
                        logger.warning(
                            "cid=%s HTTP 429 (Too Many Requests), попытка %d, ждем %.1fс",
                            cid, attempt + 1, delay,
                        )
                        await asyncio.sleep(delay)
                        continue

                    resp.raise_for_status()
                    data = await resp.json()

                    if req["kind"] == "frankfurter":
                        rate = float(data["rates"][req["target"]])
                    elif req["kind"] == "binance":
                        rate = float(data["price"])
                    else:
                        raise ValueError(f"Неизвестный тип источника: {req['kind']}")

                    logger.info("cid=%s успешно получен курс %.6f (попытка %d)", cid, rate, attempt + 1)
                    return {**req, "rate": rate}

            except (aiohttp.ClientError, asyncio.TimeoutError, KeyError, ValueError) as exc:
                delay = BACKOFF_BASE * (2 ** attempt)
                if attempt < MAX_RETRIES:
                    logger.warning(
                        "cid=%s ошибка запроса (%s), попытка %d/%d, retry через %.1fс",
                        cid, exc, attempt + 1, MAX_RETRIES + 1, delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        "cid=%s исчерпаны попытки (%s) - используем fallback-значение",
                        cid, exc,
                    )

        # Fallback: после всех неудачных попыток подставляем синтетическое
        # значение, чтобы конвейер не терял запись и продолжал работу.
        rate = random.uniform(0.5, 150.0)
        return {**req, "rate": rate, "kind": f"{req['kind']}_fallback"}

    async def collect(self) -> None:
        """Запускает все HTTP-запросы параллельно и складывает результаты в asyncio.Queue."""
        async with aiohttp.ClientSession() as session:
            tasks = [self._fetch_one(session, req) for req in self.requests]
            for coro in asyncio.as_completed(tasks):
                record = await coro
                await self.raw_queue.put(record)

        # poison pill для dispatcher
        await self.raw_queue.put(None)
        logger.info("collect: все %d запросов обработаны, poison pill отправлен", len(self.requests))

    # ------------------------------------------------------------------
    # Этап 2: CPU-bound обработка (ProcessPoolExecutor)
    # ------------------------------------------------------------------
    async def dispatcher(self) -> None:
        """
        Мост между asyncio и multiprocessing.

        Забирает "сырые" данные из asyncio.Queue, передает их на расчет
        в ProcessPoolExecutor (numpy.mean/std в processor.process_rate),
        и складывает готовый результат в multiprocessing.Queue.
        """
        loop = asyncio.get_running_loop()

        while True:
            raw = await self.raw_queue.get()
            if raw is None:
                break

            try:
                result = await loop.run_in_executor(self.process_pool, processor.process_rate, raw)
                self.result_queue.put(result)
            except Exception:
                logger.exception("dispatcher: ошибка обработки cid=%s", raw.get("cid"))

        # poison pill для store_consumer
        self.result_queue.put(None)
        logger.info("dispatcher: все элементы обработаны, poison pill отправлен")

    # ------------------------------------------------------------------
    # Этап 3: сохранение в SQLite (ThreadPoolExecutor)
    # ------------------------------------------------------------------
    async def store_consumer(self) -> None:
        """
        Забирает готовые записи из multiprocessing.Queue (блокирующий .get(),
        выполняемый в ThreadPoolExecutor) и пишет их в SQLite, также через
        ThreadPoolExecutor, чтобы не блокировать Event Loop.
        """
        loop = asyncio.get_running_loop()

        while True:
            record = await loop.run_in_executor(self.thread_pool, self.result_queue.get)
            if record is None:
                break

            await loop.run_in_executor(self.thread_pool, storage.save_rate, record, self.db_path)
            self.saved_count += 1

        logger.info("store_consumer: завершено, сохранено %d записей", self.saved_count)

    # ------------------------------------------------------------------
    # Запуск всего конвейера
    # ------------------------------------------------------------------
    async def run(self) -> float:
        """Запускает конвейер целиком. Возвращает время выполнения в секундах."""
        storage.init_db(self.db_path)

        self.process_pool = ProcessPoolExecutor(max_workers=self.num_processes)
        self.thread_pool = ThreadPoolExecutor(
            max_workers=self.num_threads, thread_name_prefix="storage"
        )

        start = time.perf_counter()

        tasks = [
            asyncio.create_task(self.collect(), name="collect"),
            asyncio.create_task(self.dispatcher(), name="dispatcher"),
            asyncio.create_task(self.store_consumer(), name="store_consumer"),
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.warning("run: получен сигнал отмены, останавливаем задачи конвейера...")
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        finally:
            elapsed = time.perf_counter() - start
            logger.info("Завершение executors (ProcessPoolExecutor, ThreadPoolExecutor)...")
            self.process_pool.shutdown(wait=True, cancel_futures=True)
            self.thread_pool.shutdown(wait=True, cancel_futures=True)

        logger.info("Конвейер завершен за %.2f сек, сохранено записей: %d", elapsed, self.saved_count)
        return elapsed

    def shutdown(self) -> None:
        """Принудительное освобождение ресурсов (вызывается при Ctrl+C)."""
        if self.process_pool is not None:
            self.process_pool.shutdown(wait=False, cancel_futures=True)
        if self.thread_pool is not None:
            self.thread_pool.shutdown(wait=False, cancel_futures=True)
        self.result_queue.close()
