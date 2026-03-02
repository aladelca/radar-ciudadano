#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os

from dotenv import load_dotenv
import uvicorn

from _bootstrap import ensure_src_path

ensure_src_path()

from jne_ingest.api_app import create_app


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Servidor API para consulta de candidatos JNE 2026")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host bind")
    parser.add_argument("--port", type=int, default=8010, help="Puerto")
    parser.add_argument("--reload", action="store_true", help="Modo autoreload para desarrollo")
    parser.add_argument(
        "--log-level",
        type=str,
        default=os.getenv("JNE_LOG_LEVEL", "INFO"),
        help="Nivel de logging (DEBUG, INFO, WARNING, ERROR)",
    )
    return parser


def setup_logging(log_level: str) -> None:
    resolved = log_level.strip().upper()
    logging.basicConfig(
        level=getattr(logging, resolved, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )


def main() -> None:
    load_dotenv()
    args = build_parser().parse_args()
    setup_logging(args.log_level)
    logger = logging.getLogger("jne_ingest.api_run")
    logger.info(
        "Iniciando API | host=%s port=%s reload=%s log_level=%s",
        args.host,
        args.port,
        args.reload,
        args.log_level.upper(),
    )

    uvicorn.run(
        "jne_ingest.api_app:create_app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        factory=True,
        log_level=args.log_level.lower(),
    )


if __name__ == "__main__":
    main()
