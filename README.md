# Currency Aggregator — гибридный конвейер (asyncio + ProcessPoolExecutor + ThreadPoolExecutor)

Итоговая работа: гибридное приложение, в котором одновременно работают три
парадигмы конкурентного программирования:

- **asyncio + aiohttp** — этап 1: асинхронный сбор курсов валют (I/O-bound);
- **ProcessPoolExecutor** — этап 2: CPU-bound обработка (`numpy.mean` / `numpy.std`);
- **ThreadPoolExecutor через `loop.run_in_executor()`** — этап 3: безопасная
  запись в SQLite синхронной библиотекой `sqlite3`.

## Архитектура

```
Этап 1 (asyncio/aiohttp)        Этап 2 (ProcessPoolExecutor)        Этап 3 (ThreadPoolExecutor)
collect()                  -->  dispatcher()                  -->  store_consumer()
- aiohttp + as_completed         - run_in_executor(process_pool,     - run_in_executor(thread_pool, ...)
- результаты -> asyncio.Queue      processor.process_rate)            - mp.Queue.get() (блокирующий)
                                  - результат -> multiprocessing.Queue - запись в SQLite (rates)
```

Все три этапа соединены очередями (`asyncio.Queue` между этапом 1 и
диспетчером, `multiprocessing.Queue` между диспетчером и этапом 3) и
завершаются только через poison pills (`None`).

## Источники данных (50 запросов)

- **Frankfurter API** (реальные курсы валют ЕЦБ) — 20 запросов (`sources.FX_PAIRS`)
- **Binance Public Ticker** (криптовалюты) — 10 запросов (`sources.BINANCE_SYMBOLS`)
- **Mock-запросы** (имитация нестабильных источников) — 20 запросов (`sources.MOCK_PAIRS`)

## Структура репозитория

```
currency_aggregator/
├── README.md
├── requirements.txt
├── main.py          # точка входа
├── aggregator.py     # основной класс конвейера (Aggregator)
├── sources.py        # список из 50 запросов (ALL_REQUESTS)
├── processor.py       # CPU-bound функция process_rate (numpy)
├── storage.py         # синхронная работа с SQLite
├── benchmark.py        # последовательный эталон (baseline)
└── report/
    ├── report.md       # отчёт с бенчмарками и анализом
    ├── run_hybrid.log    # лог гибридного запуска
    ├── run_baseline.log  # лог последовательного эталона
    └── run_cprofile.log   # лог профилирования cProfile
```

## Установка

```bash
pip install -r requirements.txt
```

## Запуск и проверка

```bash
# 1. Последовательный эталон (замер базового времени)
python -c "from benchmark import run_sequential_baseline; run_sequential_baseline()"

# 2. Гибридный режим
python main.py

# 3. Проверка БД
python -c "from storage import fetch_all; print(len(fetch_all())); print(fetch_all()[:5])"

# 4. Профилирование
python -m cProfile -s cumtime main.py
```

## Graceful shutdown

При нажатии `Ctrl+C` `asyncio.run()` отменяет задачи конвейера
(`collect`, `dispatcher`, `store_consumer`), после чего `Aggregator.run()`
в блоке `finally` корректно завершает `ProcessPoolExecutor` и
`ThreadPoolExecutor` (`shutdown(wait=True, cancel_futures=True)`).

## Логирование

Формат лога включает `processName`, `threadName` и `correlation_id` (`cid`)
для каждой записи, что позволяет отследить путь конкретного запроса через
все три этапа конвейера.

## Бенчмарки

См. [report/report.md](report/report.md) — там зафиксировано измеренное
ускорение гибридного конвейера относительно последовательного эталона
(**≥ 3×**, фактически ~15×) и результаты профилирования.
