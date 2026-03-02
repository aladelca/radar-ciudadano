from __future__ import annotations

from typing import Any, Dict

import httpx


class InstagramDiscoveryClient:
    def __init__(
        self,
        *,
        access_token: str,
        api_version: str = "v25.0",
        timeout_seconds: int = 30,
    ) -> None:
        token = access_token.strip()
        if not token:
            raise ValueError("access_token de Instagram no puede ser vacio.")
        self.access_token = token
        self.api_version = api_version.strip().lstrip("v")
        self.client = httpx.Client(
            timeout=timeout_seconds,
            headers={
                "Accept": "application/json",
                "User-Agent": "congreso-votaciones/instagram-discovery",
            },
        )

    def close(self) -> None:
        self.client.close()

    def fetch_business_discovery(
        self,
        *,
        app_user_ig_id: str,
        target_username: str,
        media_limit: int = 25,
    ) -> Dict[str, Any]:
        ig_id = app_user_ig_id.strip()
        username = self._normalize_username(target_username)
        if not ig_id:
            raise ValueError("app_user_ig_id no puede ser vacio.")
        if media_limit < 1:
            raise ValueError("media_limit debe ser >= 1.")

        fields = self._build_business_discovery_fields(username=username, media_limit=media_limit)
        url = f"https://graph.facebook.com/v{self.api_version}/{ig_id}"
        response = self.client.get(
            url,
            params={
                "fields": fields,
                "access_token": self.access_token,
            },
        )
        if response.status_code >= 400:
            detail = response.text
            raise RuntimeError(
                f"Business Discovery fallo status={response.status_code} para username={username}: {detail}"
            )

        payload = response.json()
        discovery = payload.get("business_discovery")
        if not isinstance(discovery, dict):
            raise RuntimeError("Respuesta de Business Discovery sin objeto business_discovery.")
        return discovery

    @staticmethod
    def _normalize_username(value: str) -> str:
        username = value.strip()
        if username.startswith("@"):
            username = username[1:]
        username = username.strip().lower()
        if not username:
            raise ValueError("username de Instagram no puede ser vacio.")
        return username

    @staticmethod
    def _build_business_discovery_fields(*, username: str, media_limit: int) -> str:
        media_fields = ",".join(
            [
                "id",
                "caption",
                "comments_count",
                "like_count",
                "view_count",
                "media_type",
                "media_product_type",
                "media_url",
                "thumbnail_url",
                "permalink",
                "timestamp",
            ]
        )
        account_fields = ",".join(
            [
                "id",
                "username",
                "name",
                "biography",
                "website",
                "profile_picture_url",
                "followers_count",
                "follows_count",
                "media_count",
                "media.limit(" + str(media_limit) + "){" + media_fields + "}",
            ]
        )
        return f"business_discovery.username({username}){{{account_fields}}}"
