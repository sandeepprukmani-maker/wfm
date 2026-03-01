"""
FlowForge — MSSQL MCP Connector
Wraps Microsoft SQL Server via pyodbc / pymssql

Supports:
- Execute queries (SELECT, INSERT, UPDATE, DELETE, stored procs)
- Row count assertions
- Export results to CSV / XLSX
- Schema inspection
"""

import logging
import csv
import io
from typing import Any, List, Optional, Tuple
from contextlib import contextmanager

logger = logging.getLogger(__name__)

try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False
    logger.warning("pyodbc not installed. Install with: pip install pyodbc")

try:
    import pymssql
    PYMSSQL_AVAILABLE = True
except ImportError:
    PYMSSQL_AVAILABLE = False

try:
    import openpyxl
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False


class MSSQLMCP:
    """
    MCP connector for Microsoft SQL Server.
    Prefers pyodbc; falls back to pymssql.
    """

    def __init__(
        self,
        server: str = None,
        database: str = None,
        username: str = None,
        password: str = None,
        connection_string: str = None,
        credential_name: str = None,
        driver: str = "ODBC Driver 17 for SQL Server",
        timeout: int = 30,
        autocommit: bool = False,
    ):
        self.credential_name = credential_name
        self.timeout = timeout
        self.autocommit = autocommit
        self._conn = None

        if connection_string:
            self._connection_string = connection_string
        else:
            self._connection_string = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
                f"Timeout={timeout};"
            )
            # Store for pymssql fallback
            self._server   = server
            self._database = database
            self._username = username
            self._password = password

        self.last_column_names: List[str] = []   # set after every execute_query call
        logger.info(f"MSSQLMCP initialized (credential={credential_name})")

    # ── Connection ────────────────────────────────────────────────────────────

    def connect(self):
        if PYODBC_AVAILABLE:
            self._conn = pyodbc.connect(self._connection_string, autocommit=self.autocommit)
        elif PYMSSQL_AVAILABLE:
            self._conn = pymssql.connect(
                server=self._server, user=self._username,
                password=self._password, database=self._database
            )
        else:
            raise RuntimeError("No SQL driver available. Install pyodbc or pymssql.")
        logger.info("MSSQL connection established")
        return self

    def disconnect(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    @contextmanager
    def _cursor(self):
        if not self._conn:
            self.connect()
        cur = self._conn.cursor()
        try:
            yield cur
            if not self.autocommit:
                self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        finally:
            cur.close()

    # ── Query Execution ───────────────────────────────────────────────────────

    def execute_query(self, query: str, params: tuple = None) -> List[Tuple]:
        """Execute SQL and return all rows. Sets self.last_column_names after execution."""
        logger.info(f"Executing query: {query[:120]}…")
        with self._cursor() as cur:
            cur.execute(query, params or ())
            # Capture column names from cursor description before fetchall
            self.last_column_names = [desc[0] for desc in cur.description] if cur.description else []
            rows = cur.fetchall()
        logger.info(f"Query returned {len(rows)} rows, columns: {self.last_column_names}")
        return rows

    def execute_scalar(self, query: str, params: tuple = None) -> Any:
        """Return single value (first column of first row)."""
        rows = self.execute_query(query, params)
        if not rows:
            return None
        return rows[0][0]

    def execute_non_query(self, query: str, params: tuple = None) -> int:
        """Execute DML (INSERT/UPDATE/DELETE). Returns row count."""
        with self._cursor() as cur:
            cur.execute(query, params or ())
            return cur.rowcount

    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """Bulk DML execution. Returns total rows affected."""
        total = 0
        with self._cursor() as cur:
            for params in params_list:
                cur.execute(query, params)
                total += cur.rowcount
        return total

    def execute_stored_proc(self, proc_name: str, params: tuple = None) -> List[Tuple]:
        """Execute stored procedure."""
        placeholders = ",".join(["?" for _ in (params or [])])
        query = f"EXEC {proc_name} {placeholders}"
        return self.execute_query(query, params)

    # ── Assertions ────────────────────────────────────────────────────────────

    def assert_row_count(self, query: str, expected: int, params: tuple = None) -> bool:
        """Assert query returns exactly expected rows. Raises AssertionError on mismatch."""
        actual = self.execute_scalar(query, params)
        if actual != expected:
            raise AssertionError(f"Row count mismatch: expected {expected}, got {actual}\nQuery: {query}")
        return True

    def assert_no_rows(self, query: str, params: tuple = None) -> bool:
        """Assert query returns 0 rows."""
        return self.assert_row_count(query, 0, params)

    def assert_value(self, query: str, expected: Any, params: tuple = None) -> bool:
        """Assert scalar result equals expected."""
        actual = self.execute_scalar(query, params)
        if actual != expected:
            raise AssertionError(f"Value mismatch: expected {expected!r}, got {actual!r}")
        return True

    # ── Export ────────────────────────────────────────────────────────────────

    def export_to_csv(self, query: str, output_path: str, params: tuple = None) -> str:
        """Run query and write CSV to output_path."""
        with self._cursor() as cur:
            cur.execute(query, params or ())
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)

        logger.info(f"Exported {len(rows)} rows to {output_path}")
        return output_path

    def export_to_xlsx(self, query: str, output_path: str, params: tuple = None) -> str:
        """Run query and write Excel to output_path."""
        if not OPENPYXL_AVAILABLE:
            raise RuntimeError("openpyxl not installed. Run: pip install openpyxl")

        with self._cursor() as cur:
            cur.execute(query, params or ())
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Results"
        ws.append(columns)
        for row in rows:
            ws.append(list(row))
        wb.save(output_path)
        logger.info(f"Exported {len(rows)} rows to {output_path}")
        return output_path

    # ── Schema Inspection ─────────────────────────────────────────────────────

    def list_tables(self, schema: str = "dbo") -> List[str]:
        rows = self.execute_query(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_TYPE='BASE TABLE'",
            (schema,)
        )
        return [r[0] for r in rows]

    def describe_table(self, table_name: str) -> List[dict]:
        rows = self.execute_query(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH "
            "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=?",
            (table_name,)
        )
        return [{"column": r[0], "type": r[1], "nullable": r[2], "max_length": r[3]} for r in rows]

    # ── Health Check ─────────────────────────────────────────────────────────

    def health_check(self) -> dict:
        try:
            version = self.execute_scalar("SELECT @@VERSION")
            return {"status": "ok", "version": str(version)[:80]}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    # ── Context Manager ───────────────────────────────────────────────────────

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()
