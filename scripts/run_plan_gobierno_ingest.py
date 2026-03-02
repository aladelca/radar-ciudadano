#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import replace
import logging
import os
import sys

from dotenv import load_dotenv

from _bootstrap import ensure_src_path

ensure_src_path()

from jne_ingest.config import AppConfig
from jne_ingest.jne_client import JNEClient
from jne_ingest.plan_gobierno_pipeline import PlanGobiernoPipeline
from jne_ingest.repository import PostgresRepository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingesta independiente de planes de gobierno desde candidatos ya persistidos."
    )
    parser.add_argument("--process-id", type=int, default=None, help="Id proceso electoral")
    parser.add_argument("--tipo-eleccion-id", type=int, default=None, help="Id tipo de eleccion")
    parser.add_argument(
        "--tipo-eleccion-nombre",
        type=str,
        default=None,
        help="Nombre tipo de eleccion (ej. PRESIDENCIAL).",
    )
    parser.add_argument(
        "--max-candidates",
        type=int,
        default=None,
        help="Limite de candidatos para corrida parcial.",
    )
    parser.add_argument(
        "--skip-pdf-text",
        action="store_true",
        help="No descarga ni extrae texto de PDF de plan.",
    )
    parser.add_argument("--db-dsn", type=str, default=None, help="DSN Postgres")
    parser.add_argument(
        "--log-level",
        type=str,
        default=None,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nivel de logs (default: JNE_LOG_LEVEL o INFO).",
    )
    return parser


def setup_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        force=True,
    )


def resolve_tipo_eleccion(config: AppConfig, client: JNEClient) -> AppConfig:
    if config.tipo_eleccion_id is not None:
        return config
    if not config.tipo_eleccion_nombre:
        return config

    objetivo = config.tipo_eleccion_nombre.strip().upper()
    tipos = client.get_tipos_eleccion(config.process_id)
    for tipo in tipos:
        tipo_id = tipo.get("idTipoEleccion")
        nombre = str(tipo.get("tipoEleccion", "")).strip().upper()
        if tipo_id is not None and nombre == objetivo:
            return replace(config, tipo_eleccion_id=int(tipo_id))

    raise ValueError(
        f"No se encontro tipo eleccion '{config.tipo_eleccion_nombre}' en proceso {config.process_id}."
    )


def main() -> int:
    load_dotenv()
    args = build_parser().parse_args()
    resolved_log_level = (args.log_level or os.getenv("JNE_LOG_LEVEL", "INFO")).upper()
    setup_logging(resolved_log_level)
    logger = logging.getLogger("jne_ingest.run_plan_gobierno")

    config = AppConfig.from_env(
        process_id=args.process_id,
        tipo_eleccion_id=args.tipo_eleccion_id,
        tipo_eleccion_nombre=args.tipo_eleccion_nombre,
        database_dsn=args.db_dsn,
    )
    logger.info(
        "Inicio ingesta plan gobierno | process_id=%s tipo_id=%s tipo_nombre=%s max_candidates=%s extract_pdf_text=%s",
        config.process_id,
        config.tipo_eleccion_id,
        config.tipo_eleccion_nombre,
        args.max_candidates,
        not args.skip_pdf_text,
    )

    client = JNEClient(config)
    resolved_config = resolve_tipo_eleccion(config, client)
    if resolved_config.tipo_eleccion_id != config.tipo_eleccion_id:
        logger.info(
            "Tipo de eleccion resuelto por nombre | tipo_nombre=%s tipo_id=%s",
            resolved_config.tipo_eleccion_nombre,
            resolved_config.tipo_eleccion_id,
        )

    repo = PostgresRepository(resolved_config.database_dsn)
    pipeline = PlanGobiernoPipeline(
        config=resolved_config,
        client=client,
        repository=repo,
        extract_pdf_text=not args.skip_pdf_text,
        max_candidates=args.max_candidates,
    )

    try:
        metrics = pipeline.run()
    except Exception:
        logger.exception("Ingesta de planes de gobierno fallida.")
        raise
    finally:
        pipeline.close()
        client.close()
        repo.close()

    print("Ingesta de planes de gobierno finalizada")
    print(f"  candidates_read: {metrics.candidates_read}")
    print(f"  candidates_persisted: {metrics.candidates_persisted}")
    print(f"  plans_resolved: {metrics.plans_resolved}")
    print(f"  pdf_texts_extracted: {metrics.pdf_texts_extracted}")
    print(f"  missing_inputs: {metrics.missing_inputs}")
    print(f"  errors: {metrics.errors_count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
