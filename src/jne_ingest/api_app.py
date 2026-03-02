from __future__ import annotations

from collections import deque
import logging
from pathlib import Path
import re
from threading import Lock
import time
from typing import Any, Deque, Dict, List, Optional
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from jne_ingest.config import AppConfig
from jne_ingest.conversation_memory import ConversationMemoryStore
from jne_ingest.openai_copilot import OpenAICopilotError, OpenAICopilotService
from jne_ingest.query_planner import plan_query
from jne_ingest.query_repository import CandidateReadRepository

logger = logging.getLogger(__name__)

_STATE_INFER_PATTERNS = [
    (re.compile(r"\binscrit", flags=re.IGNORECASE), "INSCRITO"),
    (re.compile(r"\brenunci", flags=re.IGNORECASE), "RENUNCIA"),
    (re.compile(r"\bexcluid", flags=re.IGNORECASE), "EXCLUIDO"),
    (re.compile(r"\bimproced", flags=re.IGNORECASE), "IMPROCEDENTE"),
    (re.compile(r"\btach", flags=re.IGNORECASE), "TACHA"),
]
_SEGMENT_SENADO_PATTERN = re.compile(r"\b(senad|senador|senadores|senado)\b", flags=re.IGNORECASE)
_SEGMENT_PRESIDENCIAL_PATTERN = re.compile(
    r"\b(presidencial|presidente|presidencia)\b",
    flags=re.IGNORECASE,
)


def _new_trace_id() -> str:
    return uuid4().hex[:10]


def _query_preview(query: str, *, max_chars: int = 140) -> str:
    return (str(query or "").strip().replace("\n", " "))[:max_chars]


def _infer_estado_from_query(query: str) -> Optional[str]:
    normalized_query = str(query or "").strip().lower()
    if not normalized_query:
        return None
    for pattern, normalized_state in _STATE_INFER_PATTERNS:
        if pattern.search(normalized_query):
            return normalized_state
    return None


class CopilotAskRequest(BaseModel):
    query: str = Field(min_length=1, max_length=400)
    limit: int = Field(default=5, ge=1, le=20)
    estado: Optional[str] = Field(default=None, max_length=80)
    organizacion: Optional[str] = Field(default=None, max_length=200)
    session_id: Optional[str] = Field(default=None, max_length=64)


class CopilotAskResponse(BaseModel):
    query: str
    summary: str
    count: int
    candidates: List[Dict[str, Any]]
    evidence: List[Dict[str, Any]]


class CopilotAskAIResponse(BaseModel):
    query: str
    summary: str
    count: int
    candidates: List[Dict[str, Any]]
    evidence: List[Dict[str, Any]]
    mode: str
    model: Optional[str] = None
    warning: Optional[str] = None
    session_id: str
    history_used: int
    citations: List[str]


class InMemoryRateLimiter:
    def __init__(self, *, max_requests: int, window_seconds: int) -> None:
        self._max_requests = max(1, int(max_requests))
        self._window_seconds = max(1, int(window_seconds))
        self._lock = Lock()
        self._events: Dict[str, Deque[float]] = {}

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        window_start = now - float(self._window_seconds)
        with self._lock:
            bucket = self._events.get(key)
            if bucket is None:
                bucket = deque()
                self._events[key] = bucket
            while bucket and bucket[0] < window_start:
                bucket.popleft()
            if len(bucket) >= self._max_requests:
                return False
            bucket.append(now)
            return True


def create_app() -> FastAPI:
    config = AppConfig.from_env()
    package_dir = Path(__file__).resolve().parent
    ui_dir = package_dir / "ui"
    static_dir = ui_dir / "static"

    app = FastAPI(
        title="Congreso Votaciones API",
        version="0.1.0",
        description="API de consulta para candidatos JNE 2026 y copilot ciudadano.",
    )
    app.state.config = config
    app.state.beta_api_key_set = set(config.beta_api_keys)
    app.state.beta_ai_api_key_set = set(config.beta_ai_api_keys)
    app.state.read_rate_limiter = (
        InMemoryRateLimiter(
            max_requests=config.beta_rate_limit_read_per_minute,
            window_seconds=config.beta_rate_limit_window_seconds,
        )
        if config.beta_rate_limit_read_per_minute > 0
        else None
    )
    app.state.ai_rate_limiter = (
        InMemoryRateLimiter(
            max_requests=config.beta_rate_limit_ai_per_minute,
            window_seconds=config.beta_rate_limit_window_seconds,
        )
        if config.beta_rate_limit_ai_per_minute > 0
        else None
    )

    if config.api_cors_allow_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(config.api_cors_allow_origins),
            allow_credentials=False,
            allow_methods=["GET", "POST", "OPTIONS"],
            allow_headers=["*"],
        )

    if static_dir.exists():
        app.mount("/ui/static", StaticFiles(directory=str(static_dir)), name="ui-static")

    def _request_api_key(request: Request) -> str:
        return str(request.headers.get("x-api-key") or "").strip()

    def _client_identity(request: Request) -> str:
        if app.state.config.beta_trust_proxy_headers:
            forwarded_for = str(request.headers.get("x-forwarded-for") or "").strip()
            if forwarded_for:
                first_hop = forwarded_for.split(",")[0].strip()
                if first_hop:
                    return f"ip:{first_hop}"
        host = request.client.host if request.client else "unknown"
        return f"ip:{host}"

    def _rate_limit_identity(request: Request) -> str:
        api_key = _request_api_key(request)
        configured_keys = app.state.beta_api_key_set
        if api_key and (not configured_keys or api_key in configured_keys):
            return f"key:{api_key}"
        return _client_identity(request)

    def _require_api_key(request: Request, *, scope: str) -> None:
        configured_keys = app.state.beta_api_key_set
        if not configured_keys:
            return
        if scope == "ai":
            enforce = app.state.config.beta_enforce_api_key_ai
        else:
            enforce = app.state.config.beta_enforce_api_key_read
        if not enforce:
            return

        api_key = _request_api_key(request)
        if not api_key:
            raise HTTPException(status_code=401, detail="API key requerida en header X-API-Key.")
        if api_key not in configured_keys:
            raise HTTPException(status_code=401, detail="API key invalida.")
        if scope == "ai":
            ai_keys = app.state.beta_ai_api_key_set
            if ai_keys and api_key not in ai_keys:
                raise HTTPException(status_code=403, detail="API key sin permiso para /copilot/ask-ai.")

    def _require_known_api_key(request: Request) -> None:
        configured_keys = app.state.beta_api_key_set
        if not configured_keys:
            return
        api_key = _request_api_key(request)
        if not api_key:
            raise HTTPException(status_code=401, detail="API key requerida en header X-API-Key.")
        if api_key not in configured_keys:
            raise HTTPException(status_code=401, detail="API key invalida.")

    @app.middleware("http")
    async def _rate_limit_middleware(request: Request, call_next):
        path = request.url.path
        if not path.startswith("/api/"):
            return await call_next(request)

        limiter = app.state.read_rate_limiter
        if path == "/api/v1/copilot/ask-ai":
            limiter = app.state.ai_rate_limiter

        identity = _rate_limit_identity(request)
        if limiter and not limiter.allow(identity):
            window_seconds = app.state.config.beta_rate_limit_window_seconds
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit excedido. Intenta nuevamente en breve."},
                headers={"Retry-After": str(window_seconds)},
            )
        return await call_next(request)

    @app.on_event("startup")
    def _startup() -> None:
        app.state.repo = CandidateReadRepository(
            config.database_dsn,
            dashboard_cache_ttl_seconds=config.dashboard_cache_ttl_seconds,
            readonly_sql_timeout_ms=config.readonly_sql_timeout_ms,
            readonly_sql_max_rows=config.readonly_sql_max_rows,
        )
        app.state.ai_service = OpenAICopilotService(config)
        app.state.memory_store = ConversationMemoryStore(
            max_sessions=config.copilot_session_max_sessions,
            max_turns_per_session=config.copilot_session_max_turns,
            signing_key=config.copilot_session_signing_key,
        )
        logger.info(
            "API startup complete | process_id=%s openai_configured=%s model=%s dashboard_cache_ttl=%s api_keys=%s enforce_read=%s enforce_ai=%s cors=%s",
            config.process_id,
            bool(config.openai_api_key),
            config.openai_model,
            config.dashboard_cache_ttl_seconds,
            len(config.beta_api_keys),
            config.beta_enforce_api_key_read,
            config.beta_enforce_api_key_ai,
            len(config.api_cors_allow_origins),
        )

    @app.on_event("shutdown")
    def _shutdown() -> None:
        repo: CandidateReadRepository = app.state.repo
        repo.close()
        logger.info("API shutdown complete")

    def _repo() -> CandidateReadRepository:
        return app.state.repo

    @app.get("/", include_in_schema=False)
    def home() -> FileResponse:
        index_path = ui_dir / "index.html"
        if not index_path.exists():
            raise HTTPException(status_code=404, detail="UI no disponible")
        return FileResponse(index_path)

    @app.get("/ui", include_in_schema=False)
    def home_alias() -> FileResponse:
        return home()

    @app.get("/health")
    def health(request: Request) -> Dict[str, Any]:
        if app.state.beta_api_key_set and not app.state.config.beta_allow_anon_health:
            _require_api_key(request, scope="read")
        repo = _repo()
        ok = repo.ping()
        return {
            "status": "ok" if ok else "degraded",
            "database": "ok" if ok else "error",
            "process_id_default": app.state.config.process_id,
            "openai_configured": bool(app.state.config.openai_api_key),
            "openai_model": app.state.config.openai_model,
            "dashboard_cache_ttl_seconds": app.state.config.dashboard_cache_ttl_seconds,
            "copilot_session_mode": "signed_id_in_memory",
            "beta_api_keys_configured": bool(app.state.beta_api_key_set),
            "beta_enforce_api_key_read": app.state.config.beta_enforce_api_key_read,
            "beta_enforce_api_key_ai": app.state.config.beta_enforce_api_key_ai,
            "rate_limit_read_per_minute": app.state.config.beta_rate_limit_read_per_minute,
            "rate_limit_ai_per_minute": app.state.config.beta_rate_limit_ai_per_minute,
        }

    @app.get("/api/v1/candidatos/search")
    def search_candidatos(
        request: Request,
        q: str = Query(default="", max_length=400),
        limit: int = Query(default=20, ge=1, le=100),
        estado: Optional[str] = Query(default=None, max_length=80),
        organizacion: Optional[str] = Query(default=None, max_length=200),
    ) -> Dict[str, Any]:
        _require_api_key(request, scope="read")
        trace_id = _new_trace_id()
        start = time.perf_counter()
        logger.info(
            "search.start | trace=%s query=%s limit=%s estado=%s organizacion=%s",
            trace_id,
            _query_preview(q),
            limit,
            estado,
            organizacion,
        )
        repo = _repo()
        rows = repo.search_candidates(
            q,
            limit=limit,
            estado=estado,
            organizacion=organizacion,
        )
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "search.done | trace=%s rows=%s elapsed_ms=%.1f",
            trace_id,
            len(rows),
            elapsed_ms,
        )
        return {
            "query": q,
            "count": len(rows),
            "data": rows,
        }

    @app.get("/api/v1/dashboard/insights")
    def get_dashboard_insights(
        request: Request,
        top_universidades: int = Query(default=12, ge=5, le=30),
        tipo_eleccion_id: Optional[int] = Query(default=None, ge=1),
    ) -> Dict[str, Any]:
        _require_api_key(request, scope="read")
        trace_id = _new_trace_id()
        start = time.perf_counter()
        logger.info(
            "dashboard.start | trace=%s top_universidades=%s tipo_eleccion_id=%s",
            trace_id,
            top_universidades,
            tipo_eleccion_id,
        )
        repo = _repo()
        payload = repo.get_dashboard_insights(
            top_universities=top_universidades,
            tipo_eleccion_id=tipo_eleccion_id,
        )
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info("dashboard.done | trace=%s elapsed_ms=%.1f", trace_id, elapsed_ms)
        return payload

    @app.get("/api/v1/candidatos/{id_hoja_vida}")
    def get_candidato(
        request: Request,
        id_hoja_vida: int,
        include_raw: bool = Query(default=False),
    ) -> Dict[str, Any]:
        _require_api_key(request, scope="read")
        if include_raw:
            _require_known_api_key(request)
        repo = _repo()
        detail = repo.get_candidate_detail(id_hoja_vida, include_raw=include_raw)
        if not detail:
            raise HTTPException(status_code=404, detail="Candidato no encontrado")
        return detail

    @app.get("/api/v1/candidatos/{id_hoja_vida}/instagram")
    def get_candidato_instagram(
        request: Request,
        id_hoja_vida: int,
        media_limit: int = Query(default=25, ge=1, le=100),
    ) -> Dict[str, Any]:
        _require_api_key(request, scope="read")
        repo = _repo()
        if not repo.candidate_exists(id_hoja_vida):
            raise HTTPException(status_code=404, detail="Candidato no encontrado")
        return repo.get_candidate_instagram(id_hoja_vida, media_limit=media_limit)

    @app.post("/api/v1/copilot/ask", response_model=CopilotAskResponse)
    def copilot_ask(request: Request, payload: CopilotAskRequest) -> CopilotAskResponse:
        _require_api_key(request, scope="read")
        trace_id = _new_trace_id()
        start = time.perf_counter()
        effective_estado = payload.estado or _infer_estado_from_query(payload.query)
        logger.info(
            "copilot.ask.start | trace=%s query=%s limit=%s estado=%s estado_effective=%s organizacion=%s",
            trace_id,
            _query_preview(payload.query),
            payload.limit,
            payload.estado,
            effective_estado,
            payload.organizacion,
        )
        repo = _repo()
        query_plan = plan_query(payload.query)
        logger.info("copilot.ask.plan | trace=%s operation=%s", trace_id, query_plan.operation)
        if query_plan.operation == "aggregate_count":
            aggregate = _resolve_aggregate_query(
                repo,
                payload.query,
                payload.limit,
                estado=effective_estado,
                organizacion=payload.organizacion,
            )
            rows = aggregate["rows"]
            summary = aggregate["summary"]
            evidence = _build_evidence(rows)
            response = CopilotAskResponse(
                query=payload.query,
                summary=_ensure_summary_citations(summary, _build_citation_hints(evidence)),
                count=aggregate["count"],
                candidates=rows,
                evidence=evidence,
            )
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.info(
                "copilot.ask.done | trace=%s mode=aggregate count=%s elapsed_ms=%.1f",
                trace_id,
                response.count,
                elapsed_ms,
            )
            return response

        rows = repo.search_candidates(
            payload.query,
            limit=payload.limit,
            estado=effective_estado,
            organizacion=payload.organizacion,
        )
        evidence = _build_evidence(rows)
        summary = _build_summary(payload.query, rows, effective_estado, payload.organizacion)
        response = CopilotAskResponse(
            query=payload.query,
            summary=summary,
            count=len(rows),
            candidates=rows,
            evidence=evidence,
        )
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "copilot.ask.done | trace=%s mode=search count=%s elapsed_ms=%.1f",
            trace_id,
            response.count,
            elapsed_ms,
        )
        return response

    @app.post("/api/v1/copilot/ask-ai", response_model=CopilotAskAIResponse)
    def copilot_ask_ai(request: Request, payload: CopilotAskRequest) -> CopilotAskAIResponse:
        _require_api_key(request, scope="ai")
        trace_id = _new_trace_id()
        start = time.perf_counter()
        effective_estado = payload.estado or _infer_estado_from_query(payload.query)
        repo = _repo()
        ai_service: OpenAICopilotService = app.state.ai_service
        memory_store: ConversationMemoryStore = app.state.memory_store
        local_query_plan = plan_query(payload.query)
        logger.info(
            "copilot.ask_ai.start | trace=%s query=%s limit=%s estado=%s estado_effective=%s organizacion=%s ai_enabled=%s local_plan=%s",
            trace_id,
            _query_preview(payload.query),
            payload.limit,
            payload.estado,
            effective_estado,
            payload.organizacion,
            ai_service.enabled,
            local_query_plan.operation,
        )
        session_id = memory_store.resolve_session_id(payload.session_id)
        history = memory_store.get_recent_turns(
            session_id,
            limit=app.state.config.copilot_session_history_limit,
        )
        planner_warning: Optional[str] = None
        if ai_service.enabled:
            try:
                sql_plan = ai_service.generate_sql_plan(
                    query=payload.query,
                    schema_context=repo.get_planner_context(),
                    limit=payload.limit,
                    estado=effective_estado,
                    organizacion=payload.organizacion,
                    conversation_history=history,
                )
                required_tables = []
                required_data = sql_plan.get("required_data")
                if isinstance(required_data, list):
                    required_tables = [
                        str(item.get("table"))
                        for item in required_data
                        if isinstance(item, dict) and item.get("table")
                    ]
                logger.info(
                    "copilot.ask_ai.plan | trace=%s intent=%s result_type=%s answer_level=%s execution_mode=%s derived_resolver=%s can_answer=%s objective=%s required_tables=%s missing_info=%s",
                    trace_id,
                    sql_plan.get("intent"),
                    sql_plan.get("result_type"),
                    sql_plan.get("answer_level"),
                    sql_plan.get("execution_mode"),
                    sql_plan.get("derived_resolver"),
                    sql_plan.get("can_answer"),
                    str(sql_plan.get("objective") or "")[:140],
                    ",".join(required_tables) if required_tables else "-",
                    len(sql_plan.get("missing_info") or []),
                )
                if (
                    local_query_plan.operation != "aggregate_count"
                    and str(sql_plan.get("intent") or "") == "aggregate_count"
                    and str(sql_plan.get("result_type") or "") == "aggregate"
                ):
                    raise OpenAICopilotError(
                        "Planner IA propuso agregado para consulta no agregada; se usa busqueda local."
                    )
                if (
                    local_query_plan.operation == "aggregate_count"
                    and str(sql_plan.get("result_type") or "") != "aggregate"
                ):
                    raise OpenAICopilotError(
                        "Planner IA devolvio filas para una consulta agregada; se usa planner local."
                    )
                if sql_plan.get("can_answer"):
                    execution_mode = str(sql_plan.get("execution_mode") or "sql")
                    if execution_mode == "derived":
                        derived_resolver = str(sql_plan.get("derived_resolver") or "")
                        derived_result = _run_derived_resolver(
                            repo,
                            resolver_name=derived_resolver,
                            limit=payload.limit,
                            estado=effective_estado,
                            organizacion=payload.organizacion,
                        )
                        rows = derived_result.get("rows", [])
                        _validate_derived_rows_consistency(
                            query=payload.query,
                            rows=rows,
                            answer_level=str(sql_plan.get("answer_level") or "general"),
                            organizacion=payload.organizacion,
                        )
                        source_tables = derived_result.get("source_tables", [])
                        if isinstance(source_tables, list):
                            for table_name in source_tables:
                                table_text = str(table_name or "").strip()
                                if table_text and table_text not in required_tables:
                                    required_tables.append(table_text)
                        logger.info(
                            "copilot.ask_ai.derived | trace=%s resolver=%s rows=%s",
                            trace_id,
                            derived_resolver,
                            len(rows),
                        )
                    else:
                        rows = repo.execute_readonly_sql(
                            str(sql_plan.get("sql")),
                            limit=max(payload.limit, 20),
                        )
                    if (
                        str(sql_plan.get("result_type") or "") == "rows"
                        and str(sql_plan.get("answer_level") or "general") == "candidate"
                    ):
                        has_candidate_ids = any(
                            isinstance(row, dict) and row.get("id_hoja_vida") is not None
                            for row in rows
                        )
                        if rows and not has_candidate_ids:
                            raise OpenAICopilotError(
                                "Planner IA devolvio filas sin id_hoja_vida para consulta de candidatos."
                            )
                    result_count = _extract_query_count(rows, str(sql_plan.get("result_type") or "rows"))
                    evidence = _build_evidence(rows, source_tables=required_tables)
                    citations = _build_citation_hints(evidence)
                    fallback_summary = _build_sql_plan_summary(
                        query=payload.query,
                        sql_plan=sql_plan,
                        count=result_count,
                        rows=rows,
                    )

                    if str(sql_plan.get("result_type") or "") == "aggregate":
                        summary_with_citations = _ensure_summary_citations(fallback_summary, citations)
                        memory_store.append_turn(
                            session_id=session_id,
                            query=payload.query,
                            summary=summary_with_citations,
                            mode="fallback",
                        )
                        response = CopilotAskAIResponse(
                            query=payload.query,
                            summary=summary_with_citations,
                            count=result_count,
                            candidates=rows,
                            evidence=evidence,
                            mode="fallback",
                            model=ai_service.model,
                            warning="Resultado agregado resuelto por planner SQL.",
                            session_id=session_id,
                            history_used=len(history),
                            citations=citations,
                        )
                        elapsed_ms = (time.perf_counter() - start) * 1000
                        logger.info(
                            "copilot.ask_ai.done | trace=%s mode=fallback reason=planner_aggregate count=%s elapsed_ms=%.1f",
                            trace_id,
                            response.count,
                            elapsed_ms,
                        )
                        return response

                    try:
                        ai_summary = ai_service.generate_summary(
                            query=payload.query,
                            rows=rows,
                            evidence=evidence,
                            estado=effective_estado,
                            organizacion=payload.organizacion,
                            conversation_history=history,
                        )
                        ai_summary = _ensure_summary_result_consistency(
                            ai_summary=ai_summary,
                            fallback_summary=fallback_summary,
                            result_count=result_count,
                        )
                        ai_summary_with_citations = _ensure_summary_citations(ai_summary, citations)
                        memory_store.append_turn(
                            session_id=session_id,
                            query=payload.query,
                            summary=ai_summary_with_citations,
                            mode="ai",
                        )
                        response = CopilotAskAIResponse(
                            query=payload.query,
                            summary=ai_summary_with_citations,
                            count=result_count,
                            candidates=rows,
                            evidence=evidence,
                            mode="ai",
                            model=ai_service.model,
                            warning=planner_warning,
                            session_id=session_id,
                            history_used=len(history),
                            citations=citations,
                        )
                        elapsed_ms = (time.perf_counter() - start) * 1000
                        source_label = (
                            "planner_derived"
                            if str(sql_plan.get("execution_mode") or "sql") == "derived"
                            else "planner_sql"
                        )
                        logger.info(
                            "copilot.ask_ai.done | trace=%s mode=ai source=%s count=%s elapsed_ms=%.1f",
                            trace_id,
                            source_label,
                            response.count,
                            elapsed_ms,
                        )
                        return response
                    except OpenAICopilotError as exc:
                        logger.warning("Fallo resumen IA tras planner SQL; se usa fallback SQL: %s", exc)
                        summary_with_citations = _ensure_summary_citations(fallback_summary, citations)
                        memory_store.append_turn(
                            session_id=session_id,
                            query=payload.query,
                            summary=summary_with_citations,
                            mode="fallback",
                        )
                        response = CopilotAskAIResponse(
                            query=payload.query,
                            summary=summary_with_citations,
                            count=result_count,
                            candidates=rows,
                            evidence=evidence,
                            mode="fallback",
                            model=ai_service.model,
                            warning="No se pudo generar narracion IA; se mantiene resultado SQL del planner.",
                            session_id=session_id,
                            history_used=len(history),
                            citations=citations,
                        )
                        elapsed_ms = (time.perf_counter() - start) * 1000
                        logger.info(
                            "copilot.ask_ai.done | trace=%s mode=fallback reason=summary_llm_error count=%s elapsed_ms=%.1f",
                            trace_id,
                            response.count,
                            elapsed_ms,
                        )
                        return response

                planner_warning = (
                    "Planner IA no encontro suficiente informacion estructurada para responder con SQL directo."
                )
            except (OpenAICopilotError, ValueError, Exception) as exc:  # noqa: BLE001
                logger.warning("Fallo planner SQL IA; se usa planner local: %s", exc)
                planner_warning = "No se pudo ejecutar planner SQL IA; se usa planner SQL local."
                logger.info("copilot.ask_ai.fallback_local | trace=%s reason=planner_error", trace_id)

        aggregate: Optional[Dict[str, Any]] = None
        if local_query_plan.operation == "aggregate_count":
            aggregate = _resolve_aggregate_query(
                repo,
                payload.query,
                payload.limit,
                estado=effective_estado,
                organizacion=payload.organizacion,
            )
            rows = aggregate["rows"]
            result_count = int(aggregate["count"])
        else:
            rows = repo.search_candidates(
                payload.query,
                limit=payload.limit,
                estado=effective_estado,
                organizacion=payload.organizacion,
            )
            result_count = len(rows)

        evidence = _build_evidence(rows)
        citations = _build_citation_hints(evidence)
        fallback_summary = (
            aggregate["summary"]
            if aggregate is not None
            else _build_summary(payload.query, rows, effective_estado, payload.organizacion)
        )

        if not ai_service.enabled or aggregate is not None:
            summary_with_citations = _ensure_summary_citations(fallback_summary, citations)
            memory_store.append_turn(
                session_id=session_id,
                query=payload.query,
                summary=summary_with_citations,
                mode="fallback",
            )
            response = CopilotAskAIResponse(
                query=payload.query,
                summary=summary_with_citations,
                count=result_count,
                candidates=rows,
                evidence=evidence,
                mode="fallback",
                model=ai_service.model if ai_service.enabled else None,
                warning=planner_warning
                or ("OPENAI_API_KEY no configurada; usando resumen SQL." if not ai_service.enabled else None),
                session_id=session_id,
                history_used=len(history),
                citations=citations,
            )
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.info(
                "copilot.ask_ai.done | trace=%s mode=fallback reason=%s count=%s elapsed_ms=%.1f",
                trace_id,
                "no_openai_key" if not ai_service.enabled else "aggregate_local",
                response.count,
                elapsed_ms,
            )
            return response

        try:
            ai_summary = ai_service.generate_summary(
                query=payload.query,
                rows=rows,
                evidence=evidence,
                estado=effective_estado,
                organizacion=payload.organizacion,
                conversation_history=history,
            )
            ai_summary = _ensure_summary_result_consistency(
                ai_summary=ai_summary,
                fallback_summary=fallback_summary,
                result_count=result_count,
            )
        except OpenAICopilotError as exc:
            logger.warning("Fallo OpenAI en ask-ai; se usa fallback SQL: %s", exc)
            summary_with_citations = _ensure_summary_citations(fallback_summary, citations)
            memory_store.append_turn(
                session_id=session_id,
                query=payload.query,
                summary=summary_with_citations,
                mode="fallback",
            )
            response = CopilotAskAIResponse(
                query=payload.query,
                summary=summary_with_citations,
                count=result_count,
                candidates=rows,
                evidence=evidence,
                mode="fallback",
                model=ai_service.model,
                warning=planner_warning or "No se pudo generar respuesta IA; usando resumen SQL.",
                session_id=session_id,
                history_used=len(history),
                citations=citations,
            )
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.info(
                "copilot.ask_ai.done | trace=%s mode=fallback reason=summary_llm_error_local count=%s elapsed_ms=%.1f",
                trace_id,
                response.count,
                elapsed_ms,
            )
            return response

        ai_summary_with_citations = _ensure_summary_citations(ai_summary, citations)
        memory_store.append_turn(
            session_id=session_id,
            query=payload.query,
            summary=ai_summary_with_citations,
            mode="ai",
        )
        response = CopilotAskAIResponse(
            query=payload.query,
            summary=ai_summary_with_citations,
            count=result_count,
            candidates=rows,
            evidence=evidence,
            mode="ai",
            model=ai_service.model,
            warning=planner_warning,
            session_id=session_id,
            history_used=len(history),
            citations=citations,
        )
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "copilot.ask_ai.done | trace=%s mode=ai source=local_search count=%s elapsed_ms=%.1f",
            trace_id,
            response.count,
            elapsed_ms,
        )
        return response

    return app


def _build_evidence(
    rows: List[Dict[str, Any]],
    *,
    source_tables: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    return [
        _build_evidence_item(
            row,
            row_index=index + 1,
            source_tables=source_tables,
        )
        for index, row in enumerate(rows[:5])
    ]


def _build_evidence_item(
    row: Dict[str, Any],
    *,
    row_index: int,
    source_tables: Optional[List[str]] = None,
) -> Dict[str, Any]:
    findings: List[str] = []
    score_value = row.get("score")
    if score_value is None:
        score_value = row.get("metric_value")

    dynamic_counts: List[tuple[str, int]] = []
    for key, value in row.items():
        if not isinstance(key, str) or not key.endswith("_count"):
            continue
        try:
            numeric = int(value or 0)
        except (TypeError, ValueError):
            continue
        if numeric > 0:
            dynamic_counts.append((key, numeric))

    dynamic_counts.sort(key=lambda item: (-item[1], item[0]))
    for field_name, numeric in dynamic_counts[:6]:
        label = field_name[: -len("_count")].replace("_", " ")
        findings.append(f"Registra {numeric} en {label}.")

    if not findings:
        candidate_name = str(row.get("nombre_completo") or "").strip()
        party_name = str(row.get("organizacion_politica") or "").strip()
        if candidate_name:
            findings.append("No se encontraron contadores estructurados > 0 para este candidato.")
        elif party_name:
            findings.append("Fila agregada por organizacion politica sin contadores estructurados > 0.")
        else:
            findings.append("Fila sin contadores estructurados > 0; revisar columnas agregadas del resultado.")

    sources: List[str] = []
    if isinstance(source_tables, list):
        seen = set()
        for table_name in source_tables:
            cleaned = str(table_name or "").strip()
            if cleaned and cleaned not in seen:
                seen.add(cleaned)
                sources.append(cleaned)
    if not sources:
        sources = [
            "jne.v_copilot_context",
            "jne.candidatos",
        ]

    return {
        "row_ref": row_index,
        "id_hoja_vida": row.get("id_hoja_vida"),
        "nombre_completo": row.get("nombre_completo"),
        "organizacion_politica": row.get("organizacion_politica"),
        "cargo": row.get("cargo"),
        "segmento_postulacion": row.get("segmento_postulacion"),
        "score": score_value,
        "findings": findings,
        "sources": sources,
    }


def _build_summary(
    query: str,
    rows: List[Dict[str, Any]],
    estado: Optional[str],
    organizacion: Optional[str],
) -> str:
    if not rows:
        return (
            f"No se encontraron candidatos para '{query}'. "
            "Prueba con nombre, organizacion, cargo o un tema mas especifico."
        )

    top_names = ", ".join([str(r.get("nombre_completo", "")) for r in rows[:3] if r.get("nombre_completo")])
    filtros = []
    if estado:
        filtros.append(f"estado={estado}")
    if organizacion:
        filtros.append(f"organizacion~{organizacion}")
    filtro_txt = f" con filtros ({', '.join(filtros)})" if filtros else ""

    return (
        f"Se encontraron {len(rows)} candidato(s){filtro_txt} para '{query}'. "
        f"Mayor relevancia inicial: {top_names}."
    )


def _build_citation_hints(evidence: List[Dict[str, Any]]) -> List[str]:
    hints: List[str] = []
    for item in evidence[:5]:
        candidate_id = item.get("id_hoja_vida")
        row_ref = item.get("row_ref")
        sources = item.get("sources", [])
        if isinstance(sources, list):
            src = ", ".join([str(s) for s in sources[:4]])
        else:
            src = ""
        if candidate_id is not None:
            hints.append(f"[ID:{candidate_id}] {src}".strip())
        elif row_ref is not None:
            hints.append(f"[ROW:{row_ref}] {src}".strip())
    return hints


def _extract_query_count(rows: List[Dict[str, Any]], result_type: str) -> int:
    if not rows:
        return 0
    if result_type == "aggregate":
        first_row = rows[0]
        for key in ("total", "count", "total_candidates", "cantidad"):
            value = first_row.get(key)
            if isinstance(value, (int, float)):
                return int(value)
        if len(first_row) == 1:
            only_value = next(iter(first_row.values()))
            if isinstance(only_value, (int, float)):
                return int(only_value)
    return len(rows)


def _build_sql_plan_summary(
    *,
    query: str,
    sql_plan: Dict[str, Any],
    count: int,
    rows: List[Dict[str, Any]],
) -> str:
    objective = str(sql_plan.get("objective") or "").strip() or "resolver la consulta"
    reasoning = str(sql_plan.get("reasoning") or "").strip()
    result_type = str(sql_plan.get("result_type") or "rows")

    if result_type == "aggregate":
        summary = f"Objetivo detectado: {objective}. Resultado agregado: {count}."
    else:
        summary = f"Objetivo detectado: {objective}. Filas obtenidas: {count}."

    if rows:
        sample_values: List[str] = []
        for row in rows[:3]:
            name = str(row.get("nombre_completo") or "").strip()
            party = str(row.get("organizacion_politica") or "").strip()
            segment = str(row.get("segmento_postulacion") or row.get("tipo_eleccion") or "").strip()
            if name:
                sample_values.append(name)
            elif party:
                sample_values.append(party)
            elif segment:
                sample_values.append(segment)
        if sample_values:
            summary = f"{summary} Muestra: {', '.join(sample_values)}."
    if reasoning:
        summary = f"{summary} Criterio planner: {reasoning}"
    return f"{summary} Consulta original: '{query}'."


def _resolve_aggregate_query(
    repo: CandidateReadRepository,
    query: str,
    limit: int,
    *,
    metric_key_override: Optional[str] = None,
    estado: Optional[str] = None,
    organizacion: Optional[str] = None,
) -> Dict[str, Any]:
    catalog = repo.get_metric_catalog()
    metric_key = metric_key_override if metric_key_override in catalog else repo.infer_metric_key(query)
    topic_spec = catalog.get(metric_key) if metric_key else None
    logger.info(
        "aggregate.resolve | query=%s metric_override=%s metric_key=%s",
        _query_preview(query),
        metric_key_override,
        metric_key,
    )

    if topic_spec is None:
        text_overview = repo.get_text_match_overview(
            query,
            limit=limit,
            estado=estado,
            organizacion=organizacion,
        )
        top_rows = text_overview.get("top_candidates", [])
        rows = [dict(r) for r in top_rows if isinstance(r, dict)] if isinstance(top_rows, list) else []
        for row in rows:
            if row.get("score") is None:
                row["score"] = row.get("metric_value")
        total = int(text_overview.get("total_candidates") or 0)
        summary = _build_aggregate_count_summary(
            query=query,
            total=total,
            rows=rows,
            label=str(text_overview.get("label") or "coincidencias"),
            definition=str(text_overview.get("definition") or "conteo por coincidencia textual en SQL"),
        )
        logger.info(
            "aggregate.resolve.done | mode=text_fallback total=%s rows=%s",
            total,
            len(rows),
        )
        return {
            "count": total,
            "rows": rows,
            "summary": summary,
        }

    metrics = repo.get_aggregate_metrics()
    metric_field = str(topic_spec.get("metric_field") or f"candidates_with_{metric_key}")
    total = int(metrics.get(metric_field) or 0)

    rows: List[Dict[str, Any]] = []
    if metric_key and metric_key != "total_candidates":
        overview = repo.get_metric_overview(metric_key, limit=limit)
        top_rows = overview.get("top_candidates", [])
        if isinstance(top_rows, list):
            rows = [dict(r) for r in top_rows if isinstance(r, dict)]
            for row in rows:
                if row.get("score") is None:
                    row["score"] = row.get("metric_value")

    summary = _build_aggregate_count_summary(
        query=query,
        total=total,
        rows=rows,
        label=str(topic_spec.get("label") or "resultado(s)"),
        definition=str(topic_spec.get("definition") or "conteo directo en base SQL"),
    )
    logger.info(
        "aggregate.resolve.done | mode=metric total=%s rows=%s metric_field=%s",
        total,
        len(rows),
        metric_field,
    )
    return {
        "count": total,
        "rows": rows,
        "summary": summary,
    }


def _build_aggregate_count_summary(
    *,
    query: str,
    total: int,
    rows: List[Dict[str, Any]],
    label: str,
    definition: str,
) -> str:
    quoted_query = str(query or "").strip()

    if total <= 0:
        return (
            f"No se encontraron {label} para la consulta '{quoted_query}'. "
            f"Definicion usada: {definition}."
        )

    base = (
        f"Hay {total} {label} para la consulta '{quoted_query}'. "
        f"Definicion usada: {definition}."
    )
    top_names = ", ".join([str(r.get("nombre_completo", "")) for r in rows[:3] if r.get("nombre_completo")])
    if top_names:
        return f"{base} Ejemplos con mayor carga: {top_names}."
    return base


def _ensure_summary_citations(summary: str, citations: List[str]) -> str:
    text = (summary or "").strip()
    if not citations:
        return text
    if re.search(r"\[(?:ID:\d+|ROW:\d+)\]", text):
        return text
    citation_line = "Citas sugeridas: " + " | ".join(citations[:3])
    if "Fuentes:" in text:
        return f"{text}\n{citation_line}"
    return f"{text}\n\nFuentes: {citation_line}"


def _ensure_summary_result_consistency(
    *,
    ai_summary: str,
    fallback_summary: str,
    result_count: int,
) -> str:
    text = (ai_summary or "").strip()
    if not text:
        return (fallback_summary or "").strip()
    if result_count > 0 and re.search(r"\bno se encontraron\b", text, flags=re.IGNORECASE):
        return (fallback_summary or text).strip()
    return text


def _validate_derived_rows_consistency(
    *,
    query: str,
    rows: Any,
    answer_level: str,
    organizacion: Optional[str],
) -> None:
    if not isinstance(rows, list):
        raise OpenAICopilotError("Resolver derivado devolvio estructura invalida de filas.")
    if any(not isinstance(row, dict) for row in rows):
        raise OpenAICopilotError("Resolver derivado devolvio filas no estructuradas.")
    if not rows:
        return

    normalized_answer_level = str(answer_level or "").strip().lower()
    if normalized_answer_level == "candidate":
        missing_ids = [row for row in rows if row.get("id_hoja_vida") is None]
        if missing_ids:
            raise OpenAICopilotError(
                "Resolver derivado devolvio filas de candidato sin id_hoja_vida."
            )

    if organizacion:
        target = str(organizacion).strip().upper()
        if target:
            mismatched = [
                row
                for row in rows
                if target not in str(row.get("organizacion_politica") or "").upper()
            ]
            if mismatched:
                raise OpenAICopilotError(
                    "Resolver derivado no respeto filtro de organizacion en todas las filas."
                )

    normalized_query = str(query or "").strip().lower()
    if _SEGMENT_SENADO_PATTERN.search(normalized_query):
        if any(not _row_matches_segment(row, expected="SENADO") for row in rows):
            raise OpenAICopilotError(
                "Resolver derivado no mantuvo consistencia de segmento SENADO."
            )
    if _SEGMENT_PRESIDENCIAL_PATTERN.search(normalized_query):
        if any(not _row_matches_segment(row, expected="PRESIDENCIAL") for row in rows):
            raise OpenAICopilotError(
                "Resolver derivado no mantuvo consistencia de segmento PRESIDENCIAL."
            )


def _row_matches_segment(row: Dict[str, Any], *, expected: str) -> bool:
    segment_text = str(row.get("segmento_postulacion") or row.get("tipo_eleccion") or "").upper()
    cargo_text = str(row.get("cargo") or "").upper()
    if expected == "SENADO":
        return "SENAD" in segment_text or "SENAD" in cargo_text
    if expected == "PRESIDENCIAL":
        return "PRESID" in segment_text or "PRESIDENT" in cargo_text
    return True


def _run_derived_resolver(
    repo: CandidateReadRepository,
    *,
    resolver_name: str,
    limit: int,
    estado: Optional[str],
    organizacion: Optional[str],
) -> Dict[str, Any]:
    normalized = str(resolver_name or "").strip().lower()
    if normalized == "income_amount_ranking":
        payload = repo.get_income_amount_ranking(
            limit=limit,
            estado=estado,
            organizacion=organizacion,
        )
        return {
            "rows": payload.get("rows", []),
            "source_tables": ["declaracion_ingresos", "candidatos"],
        }
    raise OpenAICopilotError(f"Resolver derivado no soportado: {resolver_name}")
