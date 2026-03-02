#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from typing import Dict, Optional

import httpx


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Smoke test para beta publica de API.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8010", help="URL base de API")
    parser.add_argument("--api-key", default="", help="API key para header X-API-Key")
    parser.add_argument("--include-ai", action="store_true", help="Incluir prueba de /copilot/ask-ai")
    parser.add_argument("--timeout", type=float, default=15.0, help="Timeout HTTP por request")
    return parser


def _headers(api_key: str) -> Dict[str, str]:
    headers = {"accept": "application/json"}
    key = str(api_key or "").strip()
    if key:
        headers["X-API-Key"] = key
    return headers


def _request(
    client: httpx.Client,
    method: str,
    path: str,
    *,
    headers: Dict[str, str],
    json_payload: Optional[dict] = None,
    params: Optional[dict] = None,
) -> httpx.Response:
    response = client.request(
        method,
        path,
        headers=headers,
        json=json_payload,
        params=params,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"{method} {path} fallo: {response.status_code} body={response.text[:300]}")
    return response


def main() -> int:
    args = _build_parser().parse_args()
    base_url = str(args.base_url).rstrip("/")
    headers = _headers(args.api_key)

    with httpx.Client(base_url=base_url, timeout=args.timeout) as client:
        health = _request(client, "GET", "/health", headers=headers).json()
        search = _request(
            client,
            "GET",
            "/api/v1/candidatos/search",
            headers=headers,
            params={"q": "acuna", "limit": 3},
        ).json()
        dashboard = _request(
            client,
            "GET",
            "/api/v1/dashboard/insights",
            headers=headers,
            params={"top_universidades": 10},
        ).json()
        ask = _request(
            client,
            "POST",
            "/api/v1/copilot/ask",
            headers={**headers, "content-type": "application/json"},
            json_payload={"query": "candidatos con sentencias", "limit": 3},
        ).json()

        search_data = search.get("data") or []
        if search_data:
            candidate_id = search_data[0].get("id_hoja_vida")
            if candidate_id is not None:
                _request(
                    client,
                    "GET",
                    f"/api/v1/candidatos/{int(candidate_id)}",
                    headers=headers,
                    params={"include_raw": "false"},
                )

        ask_ai_mode = "-"
        if args.include_ai:
            ask_ai = _request(
                client,
                "POST",
                "/api/v1/copilot/ask-ai",
                headers={**headers, "content-type": "application/json"},
                json_payload={"query": "candidatos con expedientes", "limit": 3},
            ).json()
            ask_ai_mode = str(ask_ai.get("mode") or "-")

    print("Smoke beta OK")
    print(f"  base_url: {base_url}")
    print(f"  health.status: {health.get('status')}")
    print(f"  search.count: {search.get('count')}")
    print(f"  dashboard.generated_at: {dashboard.get('generated_at')}")
    print(f"  ask.count: {ask.get('count')}")
    print(f"  ask-ai.mode: {ask_ai_mode}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"Smoke beta FAILED: {exc}", file=sys.stderr)
        raise SystemExit(1)
