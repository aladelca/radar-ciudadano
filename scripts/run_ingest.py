#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
import sys

from dotenv import load_dotenv

from _bootstrap import ensure_src_path

ensure_src_path()

from jne_ingest.browser_search_provider import PlaywrightAdvancedSearchProvider
from jne_ingest.config import AppConfig
from jne_ingest.jne_client import JNEClient
from jne_ingest.pipeline import IngestionPipeline
from jne_ingest.repository import PostgresRepository
from jne_ingest.token_provider import (
    EnvTokenProvider,
    PlaywrightTokenProvider,
    StaticTokenProvider,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingesta JNE -> Supabase/Postgres")
    parser.add_argument("--process-id", type=int, default=None, help="Id proceso electoral")
    parser.add_argument("--tipo-eleccion-id", type=int, default=None, help="Id tipo de eleccion")
    parser.add_argument(
        "--tipo-eleccion-nombre",
        type=str,
        default=None,
        help="Nombre tipo de eleccion (ej. PRESIDENCIAL).",
    )
    parser.add_argument("--page-size", type=int, default=None, help="Tamano de pagina")
    parser.add_argument("--max-pages", type=int, default=None, help="Limite de paginas para pruebas")
    parser.add_argument(
        "--partition-mod",
        type=int,
        default=None,
        help="Particion por id_hoja_vida (modulo).",
    )
    parser.add_argument(
        "--partition-rem",
        type=int,
        default=None,
        help="Resto de particion por id_hoja_vida.",
    )
    parser.add_argument(
        "--search-mode",
        type=str,
        default=None,
        choices=["api", "browser"],
        help="Modo de busqueda de candidatos: api (default) | browser.",
    )
    parser.add_argument("--db-dsn", type=str, default=None, help="DSN Postgres")
    parser.add_argument("--token", type=str, default=None, help="Token recaptcha fijo")
    parser.add_argument(
        "--token-provider",
        type=str,
        default="env",
        choices=["env", "playwright", "static"],
        help="Proveedor de token: env | playwright | static.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default=None,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nivel de logs (default: JNE_LOG_LEVEL o INFO).",
    )
    parser.add_argument("--dry-run", action="store_true", help="No persiste en BD")
    return parser


def setup_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        force=True,
    )


def main() -> int:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args()
    resolved_log_level = (args.log_level or os.getenv("JNE_LOG_LEVEL", "INFO")).upper()
    setup_logging(resolved_log_level)
    logger = logging.getLogger("jne_ingest.run")

    config = AppConfig.from_env(
        process_id=args.process_id,
        tipo_eleccion_id=args.tipo_eleccion_id,
        tipo_eleccion_nombre=args.tipo_eleccion_nombre,
        page_size=args.page_size,
        max_pages=args.max_pages,
        search_mode=args.search_mode,
        dry_run=args.dry_run,
        database_dsn=args.db_dsn,
        partition_mod=args.partition_mod,
        partition_rem=args.partition_rem,
    )
    logger.info(
        "Inicio ingesta | process_id=%s tipo_id=%s tipo_nombre=%s page_size=%s max_pages=%s partition_mod=%s partition_rem=%s dry_run=%s token_provider=%s search_mode=%s",
        config.process_id,
        config.tipo_eleccion_id,
        config.tipo_eleccion_nombre,
        config.page_size,
        config.max_pages,
        config.partition_mod,
        config.partition_rem,
        config.dry_run,
        args.token_provider,
        config.search_mode,
    )

    if args.token_provider == "static":
        if not args.token:
            raise ValueError("Si usas --token-provider static, debes enviar --token.")
        token_provider = StaticTokenProvider(args.token)
    elif args.token_provider == "playwright":
        token_provider = PlaywrightTokenProvider()
    else:
        token_provider = StaticTokenProvider(args.token) if args.token else EnvTokenProvider()
    client = JNEClient(config)
    browser_search_provider = None
    repo = None

    if not config.dry_run:
        repo = PostgresRepository(config.database_dsn)
    if config.search_mode == "browser":
        browser_search_provider = PlaywrightAdvancedSearchProvider(config)

    try:
        pipeline = IngestionPipeline(
            config=config,
            client=client,
            token_provider=token_provider,
            browser_search_provider=browser_search_provider,
            repository=repo,
        )
        metrics = pipeline.run()
    except Exception:
        logger.exception("Ingesta fallida por excepcion no controlada.")
        raise
    finally:
        client.close()
        if browser_search_provider:
            try:
                browser_search_provider.close()
            except Exception:
                logger.warning("Fallo al cerrar browser_search_provider; ignorando cierre forzado.")
        if repo:
            repo.close()

    print("Ingesta finalizada")
    print(f"  candidatos_leidos: {metrics.candidates_read}")
    print(f"  candidatos_persistidos: {metrics.candidates_persisted}")
    print(f"  paginas_leidas: {metrics.pages_read}")
    print(f"  errores: {metrics.errors_count}")
    print(f"  tipos: {metrics.tipos_procesados}")
    logger.info(
        "Fin ingesta | candidatos_leidos=%s candidatos_persistidos=%s paginas_leidas=%s errores=%s tipos=%s",
        metrics.candidates_read,
        metrics.candidates_persisted,
        metrics.pages_read,
        metrics.errors_count,
        metrics.tipos_procesados,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
