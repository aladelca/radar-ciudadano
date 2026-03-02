from __future__ import annotations

from typing import Any, Dict, List

import psycopg
from psycopg.rows import dict_row


class CopilotRepository:
    def __init__(self, dsn: str) -> None:
        self.conn = psycopg.connect(dsn, row_factory=dict_row, autocommit=True)

    def close(self) -> None:
        self.conn.close()

    def search_candidates(self, query: str, *, limit: int = 10) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                select *
                from jne.search_candidatos_copilot(%s, %s)
                """,
                (query, limit),
            )
            return list(cur.fetchall())
