#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys

from dotenv import load_dotenv

from _bootstrap import ensure_src_path

ensure_src_path()

from jne_ingest.copilot_repository import CopilotRepository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Consulta base para copilot ciudadano")
    parser.add_argument("query", type=str, help="Texto de consulta (ej. denuncias de candidato X)")
    parser.add_argument("--limit", type=int, default=10, help="Cantidad de resultados (1-100)")
    parser.add_argument("--db-dsn", type=str, default=None, help="DSN Postgres")
    return parser


def main() -> int:
    load_dotenv()
    args = build_parser().parse_args()
    dsn = args.db_dsn or os.getenv(
        "DATABASE_DSN",
        "postgresql://postgres:postgres@127.0.0.1:54322/postgres",
    )

    repo = CopilotRepository(dsn)
    try:
        rows = repo.search_candidates(args.query, limit=args.limit)
    finally:
        repo.close()

    if not rows:
        print("Sin resultados.")
        return 0

    for idx, row in enumerate(rows, start=1):
        print(
            f"{idx}. {row['nombre_completo']} | {row['organizacion_politica']} | "
            f"{row['cargo']} | estado={row['estado']} | score={row['score']}"
        )
        print(
            "   "
            f"sent_penales={row['sentencias_penales_count']}, "
            f"sent_obligaciones={row['sentencias_obligaciones_count']}, "
            f"expedientes={row['expedientes_count']}, "
            f"ingresos={row['ingresos_count']}, "
            f"inmuebles={row['bienes_inmuebles_count']}, "
            f"muebles={row['bienes_muebles_count']}, "
            f"otros_bienes={row['otros_bienes_muebles_count']}, "
            f"titularidades={row['titularidades_count']}, "
            f"anotaciones={row['anotaciones_count']}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
