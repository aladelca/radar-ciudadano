#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path

from dotenv import load_dotenv
import psycopg


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Aplica migraciones SQL locales por DSN")
    parser.add_argument(
        "--db-dsn",
        type=str,
        default=None,
        help="DSN Postgres. Si no se envía usa DATABASE_DSN del .env",
    )
    parser.add_argument(
        "--migrations-dir",
        type=str,
        default="supabase/migrations",
        help="Directorio de migraciones SQL",
    )
    return parser


def main() -> int:
    load_dotenv()
    args = build_parser().parse_args()
    dsn = args.db_dsn or os.getenv(
        "DATABASE_DSN",
        "postgresql://postgres:postgres@127.0.0.1:54322/postgres",
    )
    migrations_dir = Path(args.migrations_dir)
    if not migrations_dir.exists():
        raise FileNotFoundError(f"No existe directorio de migraciones: {migrations_dir}")

    files = sorted([p for p in migrations_dir.iterdir() if p.is_file() and p.suffix == ".sql"])
    if not files:
        print("No hay migraciones SQL para aplicar.")
        return 0

    conn = psycopg.connect(dsn, autocommit=False)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                create table if not exists public.schema_migrations (
                    name text primary key,
                    applied_at timestamptz not null default now()
                )
                """
            )
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("select name from public.schema_migrations")
            applied = {row[0] for row in cur.fetchall()}

        for file in files:
            if file.name in applied:
                print(f"SKIP {file.name}")
                continue

            sql = file.read_text(encoding="utf-8")
            print(f"APPLY {file.name}")
            with conn.cursor() as cur:
                cur.execute(sql)
                cur.execute(
                    "insert into public.schema_migrations (name) values (%s)",
                    (file.name,),
                )
            conn.commit()

        print("Migraciones aplicadas correctamente.")
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
