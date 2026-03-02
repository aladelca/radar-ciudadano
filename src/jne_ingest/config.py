from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Tuple


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def _env_csv(name: str) -> Tuple[str, ...]:
    raw = os.getenv(name, "")
    if not raw:
        return tuple()
    items = [value.strip() for value in raw.split(",")]
    return tuple(value for value in items if value)


@dataclass(frozen=True)
class AppConfig:
    database_dsn: str
    process_id: int
    tipo_eleccion_id: Optional[int]
    tipo_eleccion_nombre: Optional[str]
    page_size: int
    max_pages: Optional[int]
    request_timeout_seconds: int
    request_retries: int
    request_backoff_seconds: float
    captcha_retries: int
    verify_ssl: bool
    search_mode: str
    dry_run: bool
    openai_api_key: Optional[str]
    openai_model: str
    openai_timeout_seconds: int
    copilot_session_history_limit: int
    copilot_session_max_turns: int
    copilot_session_max_sessions: int
    copilot_session_signing_key: Optional[str]
    dashboard_cache_ttl_seconds: int
    api_cors_allow_origins: Tuple[str, ...]
    beta_api_keys: Tuple[str, ...]
    beta_ai_api_keys: Tuple[str, ...]
    beta_enforce_api_key_read: bool
    beta_enforce_api_key_ai: bool
    beta_allow_anon_health: bool
    beta_rate_limit_read_per_minute: int
    beta_rate_limit_copilot_per_minute: int
    beta_rate_limit_ai_per_minute: int
    beta_rate_limit_window_seconds: int
    beta_trust_proxy_headers: bool
    readonly_sql_timeout_ms: int
    readonly_sql_max_rows: int
    partition_mod: Optional[int]
    partition_rem: Optional[int]

    # API bases detectadas en config.js del sitio
    api_path: str = "https://apiplataformaelectoral2.jne.gob.pe"
    api_path2: str = "https://apiplataformaelectoral3.jne.gob.pe"
    api_path3: str = "https://apiplataformaelectoral4.jne.gob.pe"
    api_path4: str = "https://apiplataformaelectoral5.jne.gob.pe"
    api_path5: str = "https://apiplataformaelectoral6.jne.gob.pe"
    api_path6: str = "https://apiplataformaelectoral7.jne.gob.pe"
    api_path7: str = "https://apiplataformaelectoral8.jne.gob.pe"
    api_path8: str = "https://apiplataformaelectoral9.jne.gob.pe"
    api_path9: str = "https://apiplataformaelectoral10.jne.gob.pe"

    @staticmethod
    def from_env(
        *,
        process_id: Optional[int] = None,
        tipo_eleccion_id: Optional[int] = None,
        tipo_eleccion_nombre: Optional[str] = None,
        page_size: Optional[int] = None,
        max_pages: Optional[int] = None,
        search_mode: Optional[str] = None,
        dry_run: bool = False,
        database_dsn: Optional[str] = None,
        partition_mod: Optional[int] = None,
        partition_rem: Optional[int] = None,
    ) -> "AppConfig":
        resolved_process_id = process_id or int(os.getenv("JNE_PROCESS_ID", "124"))
        resolved_tipo_eleccion = tipo_eleccion_id
        if resolved_tipo_eleccion is None and os.getenv("JNE_TIPO_ELECCION_ID"):
            resolved_tipo_eleccion = int(os.getenv("JNE_TIPO_ELECCION_ID", "0"))
        resolved_tipo_eleccion_nombre = tipo_eleccion_nombre
        if resolved_tipo_eleccion_nombre is None:
            resolved_tipo_eleccion_nombre = os.getenv("JNE_TIPO_ELECCION_NOMBRE", "").strip() or None
        if resolved_tipo_eleccion is None and resolved_tipo_eleccion_nombre is None:
            resolved_tipo_eleccion_nombre = "PRESIDENCIAL"
        resolved_page_size = page_size or int(os.getenv("JNE_PAGE_SIZE", "20"))
        resolved_max_pages = max_pages
        if resolved_max_pages is None and os.getenv("JNE_MAX_PAGES"):
            resolved_max_pages = int(os.getenv("JNE_MAX_PAGES", "0"))
        resolved_dsn = database_dsn or os.getenv(
            "DATABASE_DSN",
            "postgresql://postgres:postgres@127.0.0.1:54322/postgres",
        )
        resolved_search_mode = (search_mode or os.getenv("JNE_SEARCH_MODE", "api")).strip().lower()
        if resolved_search_mode not in {"api", "browser"}:
            raise ValueError("JNE_SEARCH_MODE debe ser 'api' o 'browser'.")
        resolved_openai_api_key = os.getenv("OPENAI_API_KEY", "").strip() or None
        resolved_cors_allow_origins = _env_csv("API_CORS_ALLOW_ORIGINS")
        resolved_beta_api_keys = _env_csv("BETA_API_KEYS")
        resolved_beta_ai_api_keys = _env_csv("BETA_AI_API_KEYS")
        if resolved_beta_ai_api_keys and not resolved_beta_api_keys:
            resolved_beta_api_keys = resolved_beta_ai_api_keys
        if resolved_beta_ai_api_keys:
            invalid_ai_keys = sorted(set(resolved_beta_ai_api_keys).difference(set(resolved_beta_api_keys)))
            if invalid_ai_keys:
                raise ValueError("BETA_AI_API_KEYS debe ser subconjunto de BETA_API_KEYS.")
        resolved_partition_mod = partition_mod
        if resolved_partition_mod is None and os.getenv("JNE_PARTITION_MOD"):
            resolved_partition_mod = int(os.getenv("JNE_PARTITION_MOD", "0"))
        resolved_partition_rem = partition_rem
        if resolved_partition_rem is None and os.getenv("JNE_PARTITION_REM"):
            resolved_partition_rem = int(os.getenv("JNE_PARTITION_REM", "0"))

        if resolved_partition_mod is not None or resolved_partition_rem is not None:
            if resolved_partition_mod is None or resolved_partition_rem is None:
                raise ValueError("JNE_PARTITION_MOD y JNE_PARTITION_REM deben enviarse juntos.")
            if resolved_partition_mod <= 1:
                raise ValueError("JNE_PARTITION_MOD debe ser > 1.")
            if resolved_partition_rem < 0 or resolved_partition_rem >= resolved_partition_mod:
                raise ValueError("JNE_PARTITION_REM debe estar entre 0 y JNE_PARTITION_MOD-1.")

        return AppConfig(
            database_dsn=resolved_dsn,
            process_id=resolved_process_id,
            tipo_eleccion_id=resolved_tipo_eleccion,
            tipo_eleccion_nombre=resolved_tipo_eleccion_nombre,
            page_size=resolved_page_size,
            max_pages=resolved_max_pages if resolved_max_pages and resolved_max_pages > 0 else None,
            request_timeout_seconds=int(os.getenv("JNE_TIMEOUT_SECONDS", "45")),
            request_retries=max(1, int(os.getenv("JNE_REQUEST_RETRIES", "3"))),
            request_backoff_seconds=max(0.0, float(os.getenv("JNE_BACKOFF_SECONDS", "1.0"))),
            captcha_retries=max(1, int(os.getenv("JNE_CAPTCHA_RETRIES", "4"))),
            verify_ssl=_env_bool("JNE_VERIFY_SSL", False),
            search_mode=resolved_search_mode,
            dry_run=dry_run,
            openai_api_key=resolved_openai_api_key,
            openai_model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip() or "gpt-4.1-mini",
            openai_timeout_seconds=max(5, int(os.getenv("OPENAI_TIMEOUT_SECONDS", "30"))),
            copilot_session_history_limit=max(1, int(os.getenv("COPILOT_SESSION_HISTORY_LIMIT", "3"))),
            copilot_session_max_turns=max(3, int(os.getenv("COPILOT_SESSION_MAX_TURNS", "20"))),
            copilot_session_max_sessions=max(50, int(os.getenv("COPILOT_SESSION_MAX_SESSIONS", "500"))),
            copilot_session_signing_key=os.getenv("COPILOT_SESSION_SIGNING_KEY", "").strip() or None,
            dashboard_cache_ttl_seconds=max(0, int(os.getenv("DASHBOARD_CACHE_TTL_SECONDS", "60"))),
            api_cors_allow_origins=resolved_cors_allow_origins,
            beta_api_keys=resolved_beta_api_keys,
            beta_ai_api_keys=resolved_beta_ai_api_keys,
            beta_enforce_api_key_read=_env_bool("BETA_ENFORCE_API_KEY_READ", False),
            beta_enforce_api_key_ai=_env_bool("BETA_ENFORCE_API_KEY_AI", True),
            beta_allow_anon_health=_env_bool("BETA_ALLOW_ANON_HEALTH", True),
            beta_rate_limit_read_per_minute=max(0, int(os.getenv("BETA_RATE_LIMIT_READ_PER_MINUTE", "120"))),
            beta_rate_limit_copilot_per_minute=max(0, int(os.getenv("BETA_RATE_LIMIT_COPILOT_PER_MINUTE", "40"))),
            beta_rate_limit_ai_per_minute=max(0, int(os.getenv("BETA_RATE_LIMIT_AI_PER_MINUTE", "20"))),
            beta_rate_limit_window_seconds=max(1, int(os.getenv("BETA_RATE_LIMIT_WINDOW_SECONDS", "60"))),
            beta_trust_proxy_headers=_env_bool("BETA_TRUST_PROXY_HEADERS", True),
            readonly_sql_timeout_ms=max(100, int(os.getenv("READONLY_SQL_TIMEOUT_MS", "2500"))),
            readonly_sql_max_rows=max(1, min(200, int(os.getenv("READONLY_SQL_MAX_ROWS", "50")))),
            partition_mod=resolved_partition_mod,
            partition_rem=resolved_partition_rem,
        )
