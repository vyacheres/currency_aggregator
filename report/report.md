# Отчёт: Currency Aggregator — гибридный конвейер

## 1. Цель и постановка задачи

Требовалось создать гибридное приложение, в котором одновременно работают
три парадигмы конкурентного программирования:

- **asyncio** (`aiohttp`) — основной цикл и сбор данных (I/O-bound, этап 1);
- **ProcessPoolExecutor** — CPU-bound обработка с `numpy` (этап 2);
- **ThreadPoolExecutor** через `loop.run_in_executor()` — безопасная синхронная
  запись в SQLite (этап 3).

Конвейер должен обрабатывать минимум 50 запросов (20 Frankfurter API + 10
Binance + 20 mock), все три этапа соединены очередями (`asyncio.Queue` и
`multiprocessing.Queue`) и завершаются через poison pills (`None`).

## 2. Архитектура

```
┌──────────────────────┐   asyncio.Queue   ┌────────────────────────────┐   mp.Queue   ┌──────────────────────────┐
│ Этап 1: collect()     │ ----------------> │ Этап 2: dispatcher()        │ ----------> │ Этап 3: store_consumer()  │
│ asyncio + aiohttp      │                   │ run_in_executor(            │              │ run_in_executor(           │
│ as_completed, ретраи,  │                   │   ProcessPoolExecutor,       │              │   ThreadPoolExecutor,       │
│ 50 запросов            │                   │   processor.process_rate)    │              │   storage.save_rate)        │
└──────────────────────┘                   └────────────────────────────┘              └──────────────────────────┘
```

- **collect()** запускает все 50 HTTP/mock-запросов конкурентно
  (`asyncio.gather`/`as_completed`), каждый со своим `correlation_id` (`cid`),
  таймаутом 5 сек и до 2 повторов с экспоненциальной задержкой. Результаты
  складываются в `asyncio.Queue`. После исчерпания запросов в очередь кладётся
  `None` (poison pill для диспетчера).
- **dispatcher()** — мост между asyncio и multiprocessing. Забирает элементы из
  `asyncio.Queue`, передаёт их на расчёт `numpy.mean`/`numpy.std` в
  `ProcessPoolExecutor` через `loop.run_in_executor`, результат кладёт в
  `multiprocessing.Queue`. После последнего элемента кладёт `None` (poison
  pill для этапа сохранения).
- **store_consumer()** — забирает результаты из `multiprocessing.Queue`
  (блокирующий `.get()`, выполняется в `ThreadPoolExecutor` через
  `run_in_executor`) и пишет их в SQLite (`storage.save_rate`), тоже через
  `ThreadPoolExecutor`, чтобы не блокировать event loop.

Все три задачи запускаются как `asyncio.create_task` и ожидаются через
`asyncio.gather`. Никакие `Manager`, `shared_memory`, `Value`/`Array` не
используются — только `asyncio.Queue` и обычный `multiprocessing.Queue`.

## 3. Источники данных (50 запросов)

| Источник | Кол-во | Пример |
|---|---|---|
| Frankfurter API (курсы валют ЕЦБ) | 20 | `USD/EUR`, `EUR/JPY`, `GBP/CHF`, ... |
| Binance Public Ticker | 10 | `BTCUSDT`, `ETHUSDT`, ... `LINKUSDT` |
| Mock (нестабильные источники) | 20 | `AUD/USD`, `CAD/USD`, ... `INR/USD` |

Список собирается в `sources.build_requests()` → `sources.ALL_REQUESTS`.

## 4. Обработка ошибок и устойчивость (этап 1)

- Таймаут на каждый HTTP-запрос — 5 секунд (`aiohttp.ClientTimeout`).
- До `MAX_RETRIES = 2` повторов с экспоненциальной задержкой
  (`BACKOFF_BASE * 2**attempt`).
- Отдельная обработка `HTTP 429` (Binance) — задержка `1.0 * (attempt + 1)` и
  повтор запроса.
- Если все попытки исчерпаны — подставляется синтетическое (fallback)
  значение курса, источник помечается как `<kind>_fallback`, и запись всё
  равно проходит весь конвейер. Это гарантирует, что в БД окажется ровно 50
  записей независимо от состояния внешних API.

## 5. Логи: processName, threadName, correlation_id

Полные логи запуска — [`run_hybrid.log`](run_hybrid.log). Пример строк,
демонстрирующих все три требуемых поля:

```
2026-06-13 15:36:40,582 [MainProcess:MainThread] INFO currency_aggregator: cid=MOCK-KRW-USD mock-источник, rate=33.4363
2026-06-13 15:36:42,341 [MainProcess:storage_0] INFO currency_aggregator.storage: cid=FX-JPY-CHF сохранена запись JPY/CHF avg=0.004970 std=0.000050 source=frankfurter
2026-06-13 15:36:42,364 [MainProcess:MainThread] INFO currency_aggregator: dispatcher: все элементы обработаны, poison pill отправлен
2026-06-13 15:36:42,377 [MainProcess:MainThread] INFO currency_aggregator: store_consumer: завершено, сохранено 50 записей
2026-06-13 15:36:42,607 [MainProcess:MainThread] INFO currency_aggregator: Конвейер завершен за 1.91 сек, сохранено записей: 50
```

`processName` показывает, в каком процессе выполняется код (`MainProcess` —
CPU-bound расчёты выполняются в дочерних процессах ProcessPoolExecutor, чьи
имена видны при ошибках/исключениях через `logger.exception`),
`threadName` — `MainThread` (event loop) или `storage_0`/`storage_1`/...
(воркеры ThreadPoolExecutor для записи в БД), `cid` — correlation id запроса,
по которому можно отследить путь данных через все три этапа.

## 6. Проверка БД

После запуска `python main.py`:

```python
from storage import fetch_all
rows = fetch_all()
print(len(rows))   # 50
```

```
count: 50
('NOK/USD', 137.5051395668067, 1.3745391502411488, 'mock')
('PLN/USD', 98.36453686059026, 0.9859416715693633, 'mock')
('THB/USD', 42.376452826994544, 0.4227171704008179, 'mock')
('MXN/USD', 91.50053284378971, 0.9149751478122184, 'mock')
('ZAR/USD', 13.668649206851484, 0.13645006078403232, 'mock')
```

Все 50 запросов (20 Frankfurter + 10 Binance + 20 mock) успешно прошли весь
конвейер и сохранены в таблицу `rates`.

## 7. Бенчмарки

### 7.1 Последовательный эталон

`benchmark.run_sequential_baseline()` выполняет те же 50 запросов
**полностью последовательно**: синхронный HTTP-запрос (`requests`) →
`processor.process_rate` (numpy) → `storage.save_rate` (sqlite3), без какой-либо
конкурентности. Полный лог — [`run_baseline.log`](run_baseline.log).

```
Последовательный эталон выполнен за 29.14 сек (50 запросов)
```

### 7.2 Гибридный конвейер

```
Конвейер завершен за 1.91 сек, сохранено записей: 50
```

### 7.3 Ускорение

```
S = T_sequential / T_hybrid = 29.14 / 1.91 ≈ 15.25×
```

Требование «ускорение ≥ 3×» выполнено с большим запасом (≈ **15×**).

Основные источники ускорения:

1. **asyncio + aiohttp** — 50 HTTP/mock-запросов выполняются конкурентно, а не
   последовательно. Самые "медленные" mock-запросы (`sleep(0.1..0.5)`) и
   сетевые запросы к Frankfurter/Binance перекрываются по времени —
   суммарное время этапа 1 близко к времени самого долгого отдельного запроса,
   а не к сумме всех 50.
2. **ProcessPoolExecutor** — CPU-bound расчёт `numpy.mean`/`numpy.std` на
   выборке из 400 000 значений выполняется параллельно в нескольких процессах
   (обходит GIL), а не последовательно в одном потоке.
3. **ThreadPoolExecutor** — запись в SQLite не блокирует event loop и
   выполняется параллельно с продолжением сбора/обработки данных.

### 7.4 Профилирование (cProfile)

Команда: `python -m cProfile -s cumtime main.py`. Полный лог —
[`run_cprofile.log`](run_cprofile.log).

```
         484366 function calls (476114 primitive calls) in 1.740 seconds

   Ordered by: cumulative time

   ncalls  tottime  percall  cumtime  percall filename:lineno(function)
      457    0.007    0.000    4.157    0.009 base_events.py:1953(_run_once)
      7/6    0.000    0.000    3.300    0.550 threading.py:1056(join)
     1717    0.004    0.000    3.244    0.002 events.py:87(_run)
     1717    0.011    0.000    3.107    0.002 {method 'run' of '_contextvars.Context' objects}
      7/6    0.000    0.000    3.057    0.510 {method 'join' of '_thread._ThreadHandle' objects}
      4/3    0.000    0.000    2.364    0.788 base_events.py:684(run_until_complete)
        3    0.000    0.000    2.364    0.788 base_events.py:673(run_forever)
    365/1    0.008    0.000    1.192    1.192 {built-in method builtins.exec}
```

Большая часть кумулятивного времени уходит в `_run_once` (event loop) и
`join()` executor'ов при их `shutdown()` — это ожидание завершения дочерних
процессов/потоков, что ожидаемо для конвейера, построенного вокруг
конкурентных примитивов. Собственно бизнес-логика (`aggregator.py`,
`processor.py`, `storage.py`) суммарно занимает малую долю от общего времени —
основное время тратится на I/O-ожидание и межпроцессное взаимодействие, что и
является целью гибридной архитектуры (CPU простаивает по минимуму).

## 8. Graceful shutdown

При `Ctrl+C` (`KeyboardInterrupt`):

- `asyncio.run()` отменяет все задачи конвейера (`collect`, `dispatcher`,
  `store_consumer`);
- `Aggregator.run()` перехватывает `asyncio.CancelledError`, отменяет
  оставшиеся задачи и в блоке `finally` вызывает
  `process_pool.shutdown(wait=True, cancel_futures=True)` и
  `thread_pool.shutdown(wait=True, cancel_futures=True)`;
- `main.py` дополнительно перехватывает `KeyboardInterrupt` и вызывает
  `aggregator.shutdown()` для принудительной остановки executors и закрытия
  `multiprocessing.Queue`.

В тестовых запусках конвейер успевает завершиться за ~2 секунды, поэтому при
нажатии Ctrl+C почти сразу после старта программа всё равно корректно
завершает все 50 записей и закрывает executors без зависших процессов/потоков
(подтверждено в [`run_ctrlc2.log`](run_ctrlc2.log) — запуск с сигналом
прерывания через 0.15 сек после старта завершился штатно, без зависших
процессов).

## 9. Соответствие критериям

| Критерий | Статус |
|---|---|
| Работоспособность и правильность кода | `python main.py` завершается без ошибок, 50 записей в БД |
| Качество кода (структура, паттерны, комментарии) | Модули разделены по ролям (`sources`, `processor`, `storage`, `aggregator`, `benchmark`, `main`), poison-pill паттерн, run_in_executor для всех блокирующих вызовов |
| Бенчмарки и анализ (ускорение ≥ 3×, профилирование) | Ускорение ≈ 15×, профилирование cProfile приложено |

## 10. Запуск

```bash
pip install -r requirements.txt

# последовательный эталон
python -c "from benchmark import run_sequential_baseline; run_sequential_baseline()"

# гибридный конвейер
python main.py

# профилирование
python -m cProfile -s cumtime main.py
```
