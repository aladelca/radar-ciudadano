#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Optional
from uuid import UUID

from dotenv import load_dotenv

from _bootstrap import ensure_src_path

ensure_src_path()

from jne_ingest.config import AppConfig
from jne_ingest.instagram_discovery_client import InstagramDiscoveryClient
from jne_ingest.repository import PostgresRepository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingesta inicial de Instagram Business Discovery para un candidato."
    )
    parser.add_argument("--id-hoja-vida", type=int, required=True, help="ID de hoja de vida del candidato")
    parser.add_argument("--username", type=str, required=True, help="Username de Instagram del candidato")
    parser.add_argument(
        "--app-user-ig-id",
        type=str,
        required=True,
        help="IG User ID profesional (de tu app user) usado para business_discovery",
    )
    parser.add_argument(
        "--access-token",
        type=str,
        default=None,
        help="Token de Graph API (si no se envia, usa INSTAGRAM_GRAPH_ACCESS_TOKEN)",
    )
    parser.add_argument("--media-limit", type=int, default=25, help="Cantidad maxima de media por candidato")
    parser.add_argument(
        "--source",
        type=str,
        default="auto_discovery",
        choices=["manual", "auto_discovery", "api_onboarded"],
        help="Fuente de vinculacion de cuenta",
    )
    parser.add_argument("--is-oficial", action="store_true", help="Marca la cuenta como oficial")
    parser.add_argument("--is-public", action="store_true", help="Marca la cuenta como publica")
    parser.add_argument("--is-private", action="store_true", help="Marca la cuenta como privada")
    parser.add_argument("--db-dsn", type=str, default=None, help="DSN Postgres")
    parser.add_argument("--dry-run", action="store_true", help="No persistir en BD")
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


def resolve_is_public(is_public: bool, is_private: bool) -> Optional[bool]:
    if is_public and is_private:
        raise ValueError("No puedes combinar --is-public y --is-private.")
    if is_public:
        return True
    if is_private:
        return False
    return None


def main() -> int:
    load_dotenv()
    args = build_parser().parse_args()
    resolved_log_level = (args.log_level or os.getenv("JNE_LOG_LEVEL", "INFO")).upper()
    setup_logging(resolved_log_level)
    logger = logging.getLogger("instagram_discovery.run")

    token = (args.access_token or os.getenv("INSTAGRAM_GRAPH_ACCESS_TOKEN", "")).strip()
    if not token:
        raise ValueError("Falta token. Usa --access-token o INSTAGRAM_GRAPH_ACCESS_TOKEN.")
    is_public = resolve_is_public(args.is_public, args.is_private)
    config = AppConfig.from_env(database_dsn=args.db_dsn)

    repo: Optional[PostgresRepository] = None
    client = InstagramDiscoveryClient(access_token=token)
    run_id: Optional[UUID] = None
    media_inserted = 0

    try:
        if not args.dry_run:
            repo = PostgresRepository(config.database_dsn)
            repo.upsert_instagram_account(
                id_hoja_vida=args.id_hoja_vida,
                username=args.username,
                source=args.source,
                is_oficial=args.is_oficial,
                is_public=is_public,
            )
            run_id = repo.create_instagram_run(
                mode="discovery",
                id_hoja_vida=args.id_hoja_vida,
                username=args.username,
            )
            logger.info("Instagram run creado | run_id=%s", run_id)

        discovery = client.fetch_business_discovery(
            app_user_ig_id=args.app_user_ig_id,
            target_username=args.username,
            media_limit=args.media_limit,
        )
        media_payload = discovery.get("media", {})
        media_items = media_payload.get("data", []) if isinstance(media_payload, dict) else []
        if not isinstance(media_items, list):
            media_items = []

        if repo and run_id:
            repo.insert_instagram_profile_snapshot(
                run_id=run_id,
                id_hoja_vida=args.id_hoja_vida,
                username=args.username,
                payload=discovery,
            )
            media_inserted = repo.insert_instagram_media_snapshots(
                run_id=run_id,
                id_hoja_vida=args.id_hoja_vida,
                username=args.username,
                items=media_items,
            )
            repo.finish_instagram_run(
                run_id,
                status="completed",
                metrics={
                    "username": args.username,
                    "followers_count": discovery.get("followers_count"),
                    "media_count": discovery.get("media_count"),
                    "media_items_persisted": media_inserted,
                },
            )
        else:
            media_inserted = len(media_items)

        print("Instagram discovery finalizado")
        print(f"  id_hoja_vida: {args.id_hoja_vida}")
        print(f"  username: {args.username}")
        print(f"  followers_count: {discovery.get('followers_count')}")
        print(f"  media_count (profile): {discovery.get('media_count')}")
        print(f"  media_items_procesados: {media_inserted}")
        return 0
    except Exception as exc:
        if repo and run_id:
            repo.finish_instagram_run(
                run_id,
                status="failed",
                metrics={
                    "username": args.username,
                    "id_hoja_vida": args.id_hoja_vida,
                },
                error_message=str(exc),
            )
        logger.exception("Instagram discovery fallo.")
        raise
    finally:
        client.close()
        if repo:
            repo.close()


if __name__ == "__main__":
    sys.exit(main())
