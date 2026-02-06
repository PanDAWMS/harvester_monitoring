from __future__ import annotations

from configparser import ConfigParser
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import threading

import psycopg
from psycopg import sql
from psycopg.types.json import Json

from logger import ServiceLogger

_logger = ServiceLogger("timescaledb", __file__, "ERROR").logger


class TimescaleDbBaseClass:
    def __init__(self, path: str, section: str = "timescaledb"):
        self.path = path
        self.connection = self._make_connection(path, section)

    def _make_connection(self, path: str, section: str) -> "TimescaleClient":
        cfg = ConfigParser()
        if not cfg.read(path):
            raise FileNotFoundError(f"Could not read settings file: {path}")

        if section not in cfg and "postgresql" in cfg:
            section = "postgresql"
        if section not in cfg:
            raise KeyError(f"Missing section [{section}] in {path}")

        sec = cfg[section]

        host = sec.get("host")
        port = int(sec.get("port", "6605"))
        dbname = sec.get("dbname")
        user = sec.get("login")
        password = sec.get("password")

        if not all([host, dbname, user, password]):
            raise ValueError(f"Missing required settings in [{section}] (host/dbname/login/password)")

        sslmode = sec.get("sslmode", "require")
        schema = sec.get("schema", "public")
        label_column = sec.get("label_column", "labels")
        page_size = int(sec.get("page_size", "500"))
        autocommit = sec.get("autocommit", "true").strip().lower() in ("true")

        json_mode = sec.get("json_mode", "false").strip().lower() in ("true",)

        strict_columns = sec.get("strict_columns", "false").strip().lower() in ("true")

        return TimescaleClient(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            sslmode=sslmode,
            schema=schema,
            label_column=label_column,
            page_size=page_size,
            autocommit=autocommit,
            json_mode=json_mode,
            strict_columns=strict_columns,
        )


class TimescaleClient:
    def __init__(
        self,
        host: str,
        port: int,
        dbname: str,
        user: str,
        password: str,
        sslmode: str = "require",
        schema: str = "public",
        label_column: str = "labels",
        page_size: int = 500,
        autocommit: bool = True,
        connect_timeout: int = 10,
        json_mode: bool = False,
        strict_columns: bool = False,
    ) -> None:
        self.schema = schema
        self.label_column = label_column
        self.page_size = page_size
        self.json_mode = json_mode
        self.strict_columns = strict_columns

        self._conn_kwargs = dict(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            sslmode=sslmode,
            connect_timeout=connect_timeout,
            autocommit=autocommit,
        )

        self._tls = threading.local()

        self._cols_lock = threading.Lock()
        self._cols_cache: Dict[str, List[str]] = {}

    def _get_conn(self) -> psycopg.Connection:
        conn = getattr(self._tls, "conn", None)

        if conn is None or conn.closed:
            conn = psycopg.connect(**self._conn_kwargs)
            self._tls.conn = conn
            return conn

        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            conn = psycopg.connect(**self._conn_kwargs)
            self._tls.conn = conn

        return conn

    def close(self) -> None:
        conn = getattr(self._tls, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            self._tls.conn = None

    @staticmethod
    def _to_datetime_utc(value: Any) -> datetime:
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)

        if isinstance(value, (int, float)):

            return datetime.fromtimestamp(value, tz=timezone.utc)

        if isinstance(value, str):
            s = value.strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

        raise TypeError(f"Unsupported time type for TIMESTAMPTZ: {type(value)}")

    def _get_table_columns(self, conn: psycopg.Connection, measurement: str) -> List[str]:
        key = f"{self.schema}.{measurement}"

        with self._cols_lock:
            if key in self._cols_cache:
                return self._cols_cache[key]

        q = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        with conn.cursor() as cur:
            cur.execute(q, (self.schema, measurement))
            cols = [r[0] for r in cur.fetchall()]

        if not cols:
            raise RuntimeError(f"Table not found: {key}")
        if "time" not in cols:
            raise RuntimeError(f"Table {key} missing required column: time")

        with self._cols_lock:
            self._cols_cache[key] = cols

        return cols

    def write_points(
        self,
        points: List[Dict[str, Any]],
        time_precision: str = "s",
        retention_policy: Optional[str] = None,
    ) -> None:
        if not points:
            return

        by_measurement: Dict[str, List[Dict[str, Any]]] = {}
        for p in points:
            m = p.get("measurement")
            if not m:
                continue
            by_measurement.setdefault(m, []).append(p)

        for measurement, rows in by_measurement.items():
            self._insert(measurement, rows)

    def _insert(self, measurement: str, rows: List[Dict[str, Any]]) -> None:
        conn = self._get_conn()

        table_ident = sql.SQL("{}.{}").format(
            sql.Identifier(self.schema),
            sql.Identifier(measurement),
        )

        cols = self._get_table_columns(conn, measurement)

        if self.json_mode:
            if self.label_column not in cols:
                raise RuntimeError(
                    f"json_mode=True but table {self.schema}.{measurement} has no column '{self.label_column}'"
                )

            query = sql.SQL("INSERT INTO {} (time, {}) VALUES (%s, %s)").format(
                table_ident,
                sql.Identifier(self.label_column),
            )

            data: List[Tuple[Any, Any]] = []
            for p in rows:
                data.append(
                    (
                        self._to_datetime_utc(p["time"]),
                        Json({"tags": p.get("tags", {}), "fields": p.get("fields", {})}),
                    )
                )

            self._executemany_paged(conn, query, data, measurement)
            return

        insert_cols = [c for c in cols if c != self.label_column]
        # ensure time first
        insert_cols = ["time"] + [c for c in insert_cols if c != "time"]
        insert_set = set(insert_cols)

        cols_sql = sql.SQL(", ").join(sql.Identifier(c) for c in insert_cols)
        placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in insert_cols)

        query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            table_ident, cols_sql, placeholders
        )

        data: List[Tuple[Any, ...]] = []
        for p in rows:
            t = self._to_datetime_utc(p["time"])
            tags = p.get("tags") or {}
            fields = p.get("fields") or {}

            values: Dict[str, Any] = {"time": t}

            dropped: List[str] = []

            for k, v in tags.items():
                if k in insert_set:
                    values[k] = v
                else:
                    dropped.append(k)

            for k, v in fields.items():
                if k in insert_set:
                    values[k] = v
                else:
                    dropped.append(k)

            if self.strict_columns and dropped:
                raise RuntimeError(
                    f"columns_mode: table {self.schema}.{measurement} has no columns for keys: "
                    f"{sorted(set(dropped))[:50]}{' ...' if len(set(dropped)) > 50 else ''}"
                )

            data.append(tuple(values.get(c) for c in insert_cols))

        self._executemany_paged(conn, query, data, measurement)

    def _executemany_paged(self, conn: psycopg.Connection, query, data, measurement: str) -> None:
        if not data:
            return

        with conn.cursor() as cur:
            try:
                inserted_total = 0
                for i in range(0, len(data), self.page_size):
                    batch = data[i:i + self.page_size]
                    cur.executemany(query, batch)
                    inserted_total += len(batch)
                    _logger.info("Inserted batch into %s: %d rows (total=%d)",
                                 measurement, len(batch), inserted_total)

                if not conn.autocommit:
                    conn.commit()
                    _logger.info("Committed transaction for %s", measurement)
            except Exception:
                if not conn.autocommit:
                    conn.rollback()
                    _logger.exception("Rolled back transaction for %s", measurement)
                else:
                    _logger.exception("Insert failed for %s (autocommit=True)", measurement)
                raise
