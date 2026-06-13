"""
Слой хранения - синхронная работа с SQLite.

Все функции в этом модуле блокирующие (синхронные) и предназначены для
выполнения в ThreadPoolExecutor через loop.run_in_executor(), чтобы не
блокировать asyncio Event Loop.
"""

from __future__ import annotations

import logging
import sqlite3

DB_PATH = "currency_data.db"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS rates (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp     REAL    NOT NULL,
    currency_pair TEXT    NOT NULL,
    average_rate  REAL    NOT NULL,
    std_dev       REAL    NOT NULL,
    source        TEXT    NOT NULL
);
"""

INSERT_SQL = """
INSERT INTO rates (timestamp, currency_pair, average_rate, std_dev, source)
VALUES (?, ?, ?, ?, ?);
"""

logger = logging.getLogger("currency_aggregator.storage")


def init_db(db_path: str = DB_PATH) -> None:
    """Создает таблицу rates, если она еще не существует."""
    with sqlite3.connect(db_path) as conn:
        conn.execute(CREATE_TABLE_SQL)
        conn.commit()


def save_rate(record: dict, db_path: str = DB_PATH) -> None:
    """
    Сохраняет одну запись о курсе в SQLite.

    Открывает собственное соединение - функция вызывается из разных потоков
    ThreadPoolExecutor, а соединения sqlite3 не должны делиться между потоками.
    """
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            INSERT_SQL,
            (
                record["timestamp"],
                record["currency_pair"],
                record["average_rate"],
                record["std_dev"],
                record["source"],
            ),
        )
        conn.commit()

    logger.info(
        "cid=%s сохранена запись %s avg=%.6f std=%.6f source=%s",
        record.get("cid", "-"),
        record["currency_pair"],
        record["average_rate"],
        record["std_dev"],
        record["source"],
    )


def fetch_all(db_path: str = DB_PATH) -> list[tuple]:
    """Вспомогательная функция для отчёта/проверки: вернуть все записи."""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            "SELECT currency_pair, average_rate, std_dev, source FROM rates"
        )
        return cursor.fetchall()
