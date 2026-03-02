from __future__ import annotations

from collections import Counter
from copy import deepcopy
import logging
import re
from threading import Lock
import time
from typing import Any, Dict, List, Optional, Set
import unicodedata

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

logger = logging.getLogger("jne_ingest.query_repository")


_MONETARY_KEYWORDS = (
    "ingreso",
    "monto",
    "importe",
    "renta",
    "sueldo",
    "remuneracion",
    "total",
)

_UNIVERSITY_KEYWORDS = (
    "universidad",
    "centroestudio",
    "centro_estudio",
    "institucion",
    "casaestudio",
)

_METRIC_CACHE_TTL_SECONDS = 300
_COUNT_INTENT_MARKERS = frozenset(
    {
        "cuanto",
        "cuantos",
        "cuanta",
        "cuantas",
        "cantidad",
        "numero",
        "total",
    }
)
_TOTAL_QUERY_TOKENS = frozenset({"total", "totales", "registrados", "base", "global"})
_GENERIC_COUNT_QUERY_TOKENS = frozenset(
    {
        "cuanto",
        "cuantos",
        "cuanta",
        "cuantas",
        "cantidad",
        "numero",
        "total",
        "candidato",
        "candidatos",
        "hay",
        "existen",
        "tienen",
        "tiene",
        "con",
        "de",
        "del",
        "la",
        "el",
        "los",
        "las",
        "en",
        "por",
        "para",
    }
)
_SQL_WRITE_KEYWORDS = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|call|do|copy|vacuum|analyze|refresh)\b",
    flags=re.IGNORECASE,
)
_SQL_READ_BLOCKLIST = re.compile(
    r"\b(pg_catalog|information_schema|pg_sleep|dblink|pg_read_file|pg_ls_dir|set_config|current_setting)\b",
    flags=re.IGNORECASE,
)
_SQL_JNE_OBJECT_PATTERN = re.compile(r"\bjne\.[a-z_][a-z0-9_]*\b", flags=re.IGNORECASE)


def _normalize_query(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or "").strip().lower())
    without_marks = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", without_marks)


def _tokenize_query(value: str) -> List[str]:
    return [token for token in re.findall(r"[a-z0-9_]+", _normalize_query(value)) if len(token) >= 3]


def _safe_identifier(name: str) -> bool:
    return bool(re.fullmatch(r"[a-z_][a-z0-9_]*", name))


def _coalesce_expr(column_name: str) -> str:
    return f'coalesce("{column_name}", 0)'


def _humanize_metric(metric_key: str) -> str:
    return metric_key.replace("_", " ")


def _expand_alias_variants(value: str) -> Set[str]:
    normalized = _normalize_query(value).replace(" ", "_")
    if not normalized:
        return set()
    aliases = {normalized}
    if normalized.endswith("es") and len(normalized) > 4:
        aliases.add(normalized[:-2])
    if normalized.endswith("s") and len(normalized) > 3:
        aliases.add(normalized[:-1])
    if not normalized.endswith("s") and len(normalized) > 3:
        aliases.add(f"{normalized}s")
    return {alias for alias in aliases if alias}


def _build_metric_aliases(metric_key: str) -> Set[str]:
    aliases = set(_expand_alias_variants(metric_key))
    for token in metric_key.split("_"):
        aliases.update(_expand_alias_variants(token))
    return aliases


def _build_count_projection_sql(
    columns: List[str],
    *,
    table_alias: str,
    indent: str = "                        ",
) -> str:
    if not columns:
        return ""
    projection = ",\n".join(
        f'{indent}coalesce({table_alias}."{column_name}", 0)::int as "{column_name}"'
        for column_name in columns
    )
    return f",\n{projection}"


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip())


def _parse_decimal(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric <= 0:
            return None
        return numeric

    text = str(value).strip()
    if not text:
        return None

    text = (
        text.upper()
        .replace("S/", "")
        .replace("PEN", "")
        .replace("SOLES", "")
        .replace(" ", "")
    )
    match = re.search(r"[-+]?\d[\d.,]*", text)
    if not match:
        return None

    raw = match.group(0)
    if "," in raw and "." in raw:
        if raw.rfind(",") > raw.rfind("."):
            normalized = raw.replace(".", "").replace(",", ".")
        else:
            normalized = raw.replace(",", "")
    elif "," in raw:
        comma_parts = raw.split(",")
        if len(comma_parts[-1]) in {1, 2}:
            normalized = "".join(comma_parts[:-1]).replace(".", "") + "." + comma_parts[-1]
        else:
            normalized = "".join(comma_parts)
    else:
        normalized = raw

    try:
        numeric = float(normalized)
    except ValueError:
        return None

    if numeric <= 0:
        return None
    return numeric


def _collect_monetary_values(payload: Any, key_hint: str = "") -> List[float]:
    values: List[float] = []
    key_lower = key_hint.lower()
    contextual = any(keyword in key_lower for keyword in _MONETARY_KEYWORDS)

    if isinstance(payload, dict):
        for key, value in payload.items():
            child_hint = f"{key_hint}.{key}" if key_hint else str(key)
            values.extend(_collect_monetary_values(value, child_hint))
        return values

    if isinstance(payload, list):
        for item in payload:
            values.extend(_collect_monetary_values(item, key_hint))
        return values

    if contextual:
        numeric = _parse_decimal(payload)
        if numeric is not None and numeric < 100_000_000:
            values.append(numeric)

    return values


def _normalize_university_name(value: str) -> Optional[str]:
    normalized = _normalize_text(value).upper()
    normalized = re.sub(r"\s+", " ", normalized)
    if not normalized or len(normalized) < 4:
        return None
    if normalized in {"NO", "NO APLICA", "NINGUNO", "NINGUNA", "-"}:
        return None
    if normalized.startswith("UNIV."):
        normalized = "UNIVERSIDAD " + normalized[5:].strip()
    return normalized[:180]


def _collect_universities(payload: Any, key_hint: str = "") -> List[str]:
    results: List[str] = []
    key_lower = key_hint.lower().replace(" ", "")
    key_match = any(keyword in key_lower for keyword in _UNIVERSITY_KEYWORDS)

    if isinstance(payload, dict):
        for key, value in payload.items():
            child_hint = f"{key_hint}.{key}" if key_hint else str(key)
            results.extend(_collect_universities(value, child_hint))
        return results

    if isinstance(payload, list):
        for item in payload:
            results.extend(_collect_universities(item, key_hint))
        return results

    if not isinstance(payload, str):
        return results

    normalized = _normalize_university_name(payload)
    if not normalized:
        return results

    if key_match or "UNIVERSIDAD" in normalized:
        results.append(normalized)
    return results


class CandidateReadRepository:
    def __init__(
        self,
        dsn: str,
        *,
        dashboard_cache_ttl_seconds: int = 60,
        readonly_sql_timeout_ms: int = 2500,
        readonly_sql_max_rows: int = 50,
    ) -> None:
        self.conn = psycopg.connect(dsn, row_factory=dict_row, autocommit=True)
        self._dashboard_cache_ttl_seconds = max(0, dashboard_cache_ttl_seconds)
        self._readonly_sql_timeout_ms = max(100, int(readonly_sql_timeout_ms))
        self._readonly_sql_max_rows = max(1, min(200, int(readonly_sql_max_rows)))
        self._dashboard_cache_lock = Lock()
        self._dashboard_cache: Dict[str, Dict[str, Any]] = {}
        self._dashboard_cache_expiry: Dict[str, float] = {}
        self._metric_catalog_lock = Lock()
        self._metric_catalog_cache: Dict[str, Dict[str, Any]] = {}
        self._metric_catalog_loaded_at = 0.0
        self._planner_context_lock = Lock()
        self._planner_context_cache: Dict[str, Any] = {}
        self._planner_context_loaded_at = 0.0

    def close(self) -> None:
        self.conn.close()

    def ping(self) -> bool:
        with self.conn.cursor() as cur:
            cur.execute("select 1 as ok")
            row = cur.fetchone()
            return bool(row and row.get("ok") == 1)

    def get_schema_context(self) -> Dict[str, Any]:
        start = time.perf_counter()
        with self.conn.cursor() as cur:
            cur.execute(
                """
                select
                    c.table_name,
                    c.column_name,
                    c.data_type
                from information_schema.columns c
                join information_schema.tables t
                  on t.table_schema = c.table_schema
                 and t.table_name = c.table_name
                where c.table_schema = 'jne'
                  and t.table_type in ('BASE TABLE', 'VIEW')
                order by c.table_name, c.ordinal_position
                """
            )
            rows = cur.fetchall()

        grouped: Dict[str, List[Dict[str, str]]] = {}
        for row in rows:
            table_name = str(row.get("table_name") or "").strip()
            column_name = str(row.get("column_name") or "").strip()
            data_type = str(row.get("data_type") or "").strip()
            if not table_name or not column_name:
                continue
            grouped.setdefault(table_name, []).append(
                {
                    "column": column_name,
                    "type": data_type,
                }
            )

        tables = [{"table": name, "columns": columns} for name, columns in sorted(grouped.items())]
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "Schema context loaded | tables=%s elapsed_ms=%.1f",
            len(tables),
            elapsed_ms,
        )
        return {"schema": "jne", "tables": tables}

    def get_planner_context(self, *, refresh: bool = False) -> Dict[str, Any]:
        with self._planner_context_lock:
            if (
                not refresh
                and self._planner_context_cache
                and (time.monotonic() - self._planner_context_loaded_at) <= _METRIC_CACHE_TTL_SECONDS
            ):
                return deepcopy(self._planner_context_cache)

            start = time.perf_counter()
            schema_context = self.get_schema_context()
            election_types = self._get_election_types_catalog(limit=50)
            top_organizations = self._get_top_organizations(limit=40)

            planner_context = {
                **schema_context,
                "domain_guide": {
                    "election_segments": [
                        {
                            "segment": "PRESIDENCIAL",
                            "description": "Postulaciones presidenciales.",
                            "usual_filters": [
                                "segmento_postulacion = 'PRESIDENCIAL'",
                                "id_tipo_eleccion = 1",
                                "tipo_eleccion ilike '%PRESIDENCIAL%'",
                            ],
                        },
                        {
                            "segment": "SENADO",
                            "description": "Postulaciones al senado (distrito unico o multiple).",
                            "usual_filters": [
                                "segmento_postulacion = 'SENADO'",
                                "id_tipo_eleccion in (20, 21)",
                                "tipo_eleccion ilike '%SENADOR%'",
                            ],
                        },
                    ],
                    "recommended_objects": [
                        {
                            "table": "v_candidatos_segmento_postulacion",
                            "use_for": "filtrar por segmento electoral (presidencial/senado).",
                        },
                        {
                            "table": "candidatos_postulaciones",
                            "use_for": "consultas por tipo de eleccion, cargo y organizacion politica.",
                        },
                        {
                            "table": "v_postulaciones_resumen_persona",
                            "use_for": "historial de postulaciones por persona.",
                        },
                        {
                            "table": "v_copilot_context",
                            "use_for": "señales de sentencias, expedientes, ingresos, bienes y ranking contextual.",
                        },
                        {
                            "table": "v_candidato_educacion",
                            "use_for": "consultas de educacion (con/sin estudios).",
                        },
                    ],
                    "party_field_hint": "organizacion_politica",
                },
                "catalogs": {
                    "election_types": election_types,
                    "top_organizations": top_organizations,
                },
            }

            self._planner_context_cache = planner_context
            self._planner_context_loaded_at = time.monotonic()
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.info(
                "Planner context loaded | tables=%s election_types=%s top_organizations=%s elapsed_ms=%.1f",
                len(schema_context.get("tables", [])),
                len(election_types),
                len(top_organizations),
                elapsed_ms,
            )
            return deepcopy(planner_context)

    def _get_election_types_catalog(self, *, limit: int = 50) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                select distinct
                    id_tipo_eleccion,
                    tipo_eleccion
                from jne.catalog_tipos_eleccion
                where id_tipo_eleccion is not null
                order by id_tipo_eleccion
                limit %s
                """,
                (max(1, min(limit, 200)),),
            )
            rows = cur.fetchall()
        return [
            {
                "id_tipo_eleccion": int(row.get("id_tipo_eleccion")),
                "tipo_eleccion": str(row.get("tipo_eleccion") or ""),
            }
            for row in rows
            if row.get("id_tipo_eleccion") is not None
        ]

    def _get_top_organizations(self, *, limit: int = 40) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                select
                    organizacion_politica,
                    count(*)::int as candidatos_count
                from jne.candidatos_postulaciones
                where coalesce(trim(organizacion_politica), '') <> ''
                group by organizacion_politica
                order by count(*) desc, organizacion_politica asc
                limit %s
                """,
                (max(1, min(limit, 200)),),
            )
            rows = cur.fetchall()
        return [
            {
                "organizacion_politica": str(row.get("organizacion_politica") or ""),
                "candidatos_count": int(row.get("candidatos_count") or 0),
            }
            for row in rows
            if row.get("organizacion_politica")
        ]

    def execute_readonly_sql(self, sql_query: str, *, limit: int = 20) -> List[Dict[str, Any]]:
        start = time.perf_counter()
        cleaned = self._validate_readonly_sql(sql_query)
        safe_limit = max(1, min(limit, self._readonly_sql_max_rows))
        wrapped = f"select * from ({cleaned}) as planned_query limit {safe_limit}"
        with self.conn.transaction():
            with self.conn.cursor() as cur:
                cur.execute(
                    "select set_config('statement_timeout', %s, true)",
                    (str(self._readonly_sql_timeout_ms),),
                )
                cur.execute(wrapped)
                rows = list(cur.fetchall())
        elapsed_ms = (time.perf_counter() - start) * 1000
        sql_preview = cleaned.replace("\n", " ")[:220]
        logger.info(
            "Readonly SQL executed | limit=%s rows=%s timeout_ms=%s elapsed_ms=%.1f sql=%s",
            safe_limit,
            len(rows),
            self._readonly_sql_timeout_ms,
            elapsed_ms,
            sql_preview,
        )
        return rows

    @staticmethod
    def _validate_readonly_sql(sql_query: str) -> str:
        cleaned = str(sql_query or "").strip().rstrip(";").strip()
        if not cleaned:
            raise ValueError("SQL vacio.")

        lower = cleaned.lower()
        if not (lower.startswith("select") or lower.startswith("with")):
            raise ValueError("Solo se permiten consultas SELECT/CTE.")
        if ";" in cleaned:
            raise ValueError("Solo se permite una sentencia SQL.")
        if "--" in cleaned or "/*" in cleaned:
            raise ValueError("SQL con comentarios no permitido.")
        if _SQL_WRITE_KEYWORDS.search(cleaned):
            raise ValueError("SQL con palabras reservadas de escritura/DDL no permitido.")
        if _SQL_READ_BLOCKLIST.search(cleaned):
            raise ValueError("SQL con objetos/funciones restringidas no permitido.")
        if not _SQL_JNE_OBJECT_PATTERN.search(cleaned):
            raise ValueError("SQL debe consultar objetos del schema jne.")
        return cleaned

    def candidate_exists(self, id_hoja_vida: int) -> bool:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                select 1 as ok
                from jne.candidatos
                where id_hoja_vida = %s
                limit 1
                """,
                (id_hoja_vida,),
            )
            row = cur.fetchone()
            return bool(row and row.get("ok") == 1)

    def search_candidates(
        self,
        query: str,
        *,
        limit: int = 20,
        estado: Optional[str] = None,
        organizacion: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        start = time.perf_counter()
        metric_key = self.infer_metric_key(query)
        metric_catalog = self.get_metric_catalog()
        metric_spec = metric_catalog.get(metric_key) if metric_key else None
        metric_expression = str((metric_spec or {}).get("expression") or "").strip()
        count_columns = self._base_count_columns(metric_catalog)
        count_projection = _build_count_projection_sql(count_columns, table_alias="v")
        metric_boost_expr = (
            f"(case when ({metric_expression}) > 0 then 180 else 0 end)"
            if metric_expression and metric_key != "total_candidates"
            else "0"
        )

        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                with normalized as (
                    select nullif(trim(coalesce(%s::text, '')), '') as q
                ),
                clean as (
                    select
                        q,
                        upper(regexp_replace(coalesce(q, ''), '[^[:alnum:][:space:]_]', ' ', 'g')) as q_clean
                    from normalized
                ),
                tokens as (
                    select distinct token
                    from clean c
                    cross join lateral regexp_split_to_table(c.q_clean, '[[:space:]]+') token
                    where c.q is not null
                      and token <> ''
                      and length(token) >= 3
                      and token not in (
                          'QUE', 'CUANTO', 'CUANTOS', 'CUANTA', 'CUANTAS',
                          'CUAL', 'CUALES', 'TIENE', 'TIENEN',
                          'CON', 'PARA', 'LOS', 'LAS', 'DEL', 'DE', 'EL', 'LA',
                          'Y', 'O', 'EN', 'UN', 'UNA',
                          'CANDIDATO', 'CANDIDATOS'
                      )
                ),
                token_context as (
                    select coalesce((select string_agg(token, ' ') from tokens), '') as compact_terms
                ),
                ranked as (
                    select
                        v.id_hoja_vida,
                        v.nombre_completo,
                        v.organizacion_politica,
                        v.cargo,
                        v.estado{count_projection},
                        case
                            when c.q is null then 1
                            else
                                (case
                                    when tc.compact_terms <> ''
                                     and upper(coalesce(v.nombre_completo, '')) like '%%' || tc.compact_terms || '%%'
                                    then 200 else 0
                                end) +
                                coalesce((
                                    select sum(
                                        (case when upper(coalesce(v.nombre_completo, '')) like '%%' || t.token || '%%' then 90 else 0 end) +
                                        (case when upper(coalesce(v.organizacion_politica, '')) like '%%' || t.token || '%%' then 60 else 0 end) +
                                        (case when upper(coalesce(v.cargo, '')) like '%%' || t.token || '%%' then 35 else 0 end) +
                                        (case when upper(v.context_text) like '%%' || t.token || '%%' then 20 else 0 end)
                                    )::int
                                    from tokens t
                                ), 0) + {metric_boost_expr}
                        end as score,
                        c.q
                    from jne.v_copilot_context v
                    cross join clean c
                    cross join token_context tc
                )
                select *
                from ranked
                where (q is null or score > 0)
                  and (%s::text is null or upper(coalesce(estado, '')) = upper(%s::text))
                  and (
                    %s::text is null
                    or upper(coalesce(organizacion_politica, '')) like '%%' || upper(%s::text) || '%%'
                  )
                order by score desc, nombre_completo asc
                limit %s
                """,
                (
                    query,
                    estado,
                    estado,
                    organizacion,
                    organizacion,
                    max(1, min(limit, 100)),
                ),
            )
            rows = list(cur.fetchall())
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "search_candidates | query=%s limit=%s estado=%s organizacion=%s metric_key=%s rows=%s elapsed_ms=%.1f",
            (query or "").strip()[:140],
            max(1, min(limit, 100)),
            estado,
            organizacion,
            metric_key,
            len(rows),
            elapsed_ms,
        )
        return rows

    def get_metric_catalog(self, *, refresh: bool = False) -> Dict[str, Dict[str, Any]]:
        with self._metric_catalog_lock:
            if (
                not refresh
                and self._metric_catalog_cache
                and (time.monotonic() - self._metric_catalog_loaded_at) <= _METRIC_CACHE_TTL_SECONDS
            ):
                return deepcopy(self._metric_catalog_cache)

            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    select column_name
                    from information_schema.columns
                    where table_schema = 'jne'
                      and table_name = 'v_copilot_context'
                    order by ordinal_position
                    """
                )
                rows = cur.fetchall()

            count_columns = [
                str(row.get("column_name"))
                for row in rows
                if isinstance(row.get("column_name"), str)
                and str(row.get("column_name")).endswith("_count")
                and _safe_identifier(str(row.get("column_name")))
            ]

            catalog: Dict[str, Dict[str, Any]] = {}
            token_groups: Dict[str, List[str]] = {}
            for column_name in count_columns:
                metric_key = column_name[: -len("_count")]
                aliases = _build_metric_aliases(metric_key)
                catalog[metric_key] = {
                    "metric": metric_key,
                    "metric_field": f"candidates_with_{metric_key}",
                    "label": f"candidato(s) con {_humanize_metric(metric_key)}",
                    "definition": f"al menos un registro en {_humanize_metric(metric_key)}",
                    "expression": _coalesce_expr(column_name),
                    "aliases": aliases,
                    "kind": "base",
                    "source_column": column_name,
                }
                for token in set(metric_key.split("_")):
                    token_norm = _normalize_query(token).replace(" ", "_")
                    if len(token_norm) >= 4:
                        token_groups.setdefault(token_norm, []).append(metric_key)

            for token, members in token_groups.items():
                unique_members = sorted(set(members))
                if len(unique_members) < 2 or token in catalog:
                    continue
                expressions: List[str] = []
                family_aliases = set(_build_metric_aliases(token))
                for member in unique_members:
                    member_spec = catalog.get(member) or {}
                    member_expr = str(member_spec.get("expression") or "").strip()
                    if member_expr:
                        expressions.append(member_expr)
                    for alias in member_spec.get("aliases", set()):
                        family_aliases.update(_expand_alias_variants(str(alias)))
                if not expressions:
                    continue
                catalog[token] = {
                    "metric": token,
                    "metric_field": f"candidates_with_{token}",
                    "label": f"candidato(s) con {_humanize_metric(token)}",
                    "definition": f"suma de metrica(s) de la familia {_humanize_metric(token)}",
                    "expression": " + ".join(expressions),
                    "aliases": {alias for alias in family_aliases if alias},
                    "kind": "family",
                    "members": unique_members,
                }

            # Semantic metric for "denuncias" used in natural language queries.
            if (
                "denuncias" not in catalog
                and "expedientes_count" in count_columns
                and "anotaciones_count" in count_columns
            ):
                catalog["denuncias"] = {
                    "metric": "denuncias",
                    "metric_field": "candidates_with_denuncias",
                    "label": "candidato(s) con denuncias",
                    "definition": "al menos una anotacion o expediente registrado",
                    "expression": f"{_coalesce_expr('expedientes_count')} + {_coalesce_expr('anotaciones_count')}",
                    "aliases": {
                        "denuncia",
                        "denuncias",
                        "acusacion",
                        "acusaciones",
                        "expediente",
                        "expedientes",
                        "anotacion",
                        "anotaciones",
                    },
                    "kind": "derived",
                    "members": ["expedientes", "anotaciones"],
                }

            catalog["total_candidates"] = {
                "metric": "total_candidates",
                "metric_field": "total_candidates",
                "label": "candidato(s) en la base",
                "definition": "registros de candidatos en la vista consolidada",
                "expression": "1",
                "aliases": set(_TOTAL_QUERY_TOKENS),
                "kind": "system",
            }

            self._metric_catalog_cache = catalog
            self._metric_catalog_loaded_at = time.monotonic()
            return deepcopy(catalog)

    def _base_count_columns(self, catalog: Optional[Dict[str, Dict[str, Any]]] = None) -> List[str]:
        active_catalog = catalog if catalog is not None else self.get_metric_catalog()
        columns = {
            str(spec.get("source_column"))
            for spec in active_catalog.values()
            if str(spec.get("kind") or "") == "base"
            and isinstance(spec.get("source_column"), str)
            and _safe_identifier(str(spec.get("source_column")))
        }
        return sorted(columns)

    def infer_metric_key(self, query: str) -> Optional[str]:
        tokens = set(_tokenize_query(query))
        if not tokens:
            return None

        normalized_query = _normalize_query(query)
        catalog = self.get_metric_catalog()
        best_key: Optional[str] = None
        best_score = 0

        for key, spec in catalog.items():
            if key == "total_candidates":
                continue
            aliases = {str(a) for a in spec.get("aliases", set())}
            alias_tokens = set()
            for alias in aliases:
                alias_tokens.update(alias.split("_"))
                alias_tokens.add(alias)

            overlap = tokens.intersection(alias_tokens)
            score = len(overlap) * 12
            if key in normalized_query:
                score += 6
            for alias in aliases:
                if alias and alias in normalized_query:
                    score += 4

            if score > best_score:
                best_key = key
                best_score = score

        if best_score > 0:
            return best_key

        if self._is_total_candidates_query(normalized_query, tokens):
            return "total_candidates"

        return None

    def _is_total_candidates_query(self, normalized_query: str, tokens: Set[str]) -> bool:
        if not tokens:
            return False
        count_intent = bool(tokens.intersection(_COUNT_INTENT_MARKERS)) or normalized_query.startswith(
            ("cuanto", "cantidad", "numero", "total")
        )
        if not count_intent:
            return False
        informative_tokens = {token for token in tokens if token not in _GENERIC_COUNT_QUERY_TOKENS}
        if informative_tokens:
            return False
        has_total_hint = bool(tokens.intersection(_TOTAL_QUERY_TOKENS))
        has_candidate_hint = "candidato" in normalized_query
        return has_total_hint or has_candidate_hint

    def get_total_candidates_count(self) -> int:
        with self.conn.cursor() as cur:
            cur.execute("select count(*)::int as total_candidates from jne.v_copilot_context")
            row = cur.fetchone() or {}
            return int(row.get("total_candidates") or 0)

    def get_metric_overview(self, metric: str, *, limit: int = 5) -> Dict[str, Any]:
        catalog = self.get_metric_catalog()
        spec = catalog.get(metric)
        if not spec:
            return {
                "metric": metric,
                "total_candidates": 0,
                "top_candidates": [],
                "label": "",
                "definition": "",
            }

        expression = str(spec.get("expression") or "").strip()
        if not expression:
            return {
                "metric": metric,
                "total_candidates": 0,
                "top_candidates": [],
                "label": str(spec.get("label") or ""),
                "definition": str(spec.get("definition") or ""),
            }

        count_columns = self._base_count_columns(catalog)
        count_projection = _build_count_projection_sql(count_columns, table_alias="v")

        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                with joined as (
                    select
                        v.id_hoja_vida,
                        v.nombre_completo,
                        v.organizacion_politica,
                        v.cargo,
                        v.estado{count_projection},
                        ({expression})::int as metric_value
                    from jne.v_copilot_context v
                ),
                ranked as (
                    select
                        *,
                        row_number() over (
                            order by metric_value desc, nombre_completo asc
                        ) as rn
                    from joined
                    where metric_value > 0
                )
                select
                    (select count(*)::int from ranked) as total_candidates,
                    coalesce(
                        jsonb_agg(to_jsonb(r) - 'rn' order by r.rn)
                        filter (where r.rn <= %s),
                        '[]'::jsonb
                    ) as top_rows
                from ranked r
                """,
                (max(1, min(limit, 100)),),
            )
            row = cur.fetchone() or {}
            top_rows = row.get("top_rows") or []
            if not isinstance(top_rows, list):
                top_rows = []
            return {
                "metric": metric,
                "total_candidates": int(row.get("total_candidates") or 0),
                "top_candidates": top_rows,
                "label": str(spec.get("label") or ""),
                "definition": str(spec.get("definition") or ""),
            }

    def get_income_amount_ranking(
        self,
        *,
        limit: int = 20,
        estado: Optional[str] = None,
        organizacion: Optional[str] = None,
    ) -> Dict[str, Any]:
        start = time.perf_counter()
        safe_limit = max(1, min(limit, 100))

        with self.conn.cursor() as cur:
            cur.execute(
                """
                select
                    c.id_hoja_vida,
                    c.nombre_completo,
                    c.organizacion_politica,
                    c.cargo,
                    c.estado
                from jne.candidatos c
                where c.id_hoja_vida is not null
                  and (%s::text is null or upper(coalesce(c.estado, '')) = upper(%s::text))
                  and (
                    %s::text is null
                    or upper(coalesce(c.organizacion_politica, '')) like '%%' || upper(%s::text) || '%%'
                  )
                """,
                (estado, estado, organizacion, organizacion),
            )
            candidates = cur.fetchall()

        candidates_by_id: Dict[int, Dict[str, Any]] = {}
        for row in candidates:
            candidate_id = row.get("id_hoja_vida")
            if candidate_id is None:
                continue
            candidates_by_id[int(candidate_id)] = {
                "id_hoja_vida": int(candidate_id),
                "nombre_completo": row.get("nombre_completo"),
                "organizacion_politica": row.get("organizacion_politica"),
                "cargo": row.get("cargo"),
                "estado": row.get("estado"),
            }

        all_candidate_ids = sorted(candidates_by_id.keys())
        if not all_candidate_ids:
            return {
                "rows": [],
                "candidates_with_data": 0,
                "total_candidates_considered": 0,
            }

        with self.conn.cursor() as cur:
            cur.execute(
                """
                select id_hoja_vida, payload
                from jne.declaracion_ingresos
                where id_hoja_vida = any(%s::bigint[])
                order by id_hoja_vida, item_index
                """,
                (all_candidate_ids,),
            )
            income_rows = cur.fetchall()

        totals_by_candidate: Dict[int, float] = {}
        entries_by_candidate: Dict[int, int] = {}
        for row in income_rows:
            candidate_id_raw = row.get("id_hoja_vida")
            if candidate_id_raw is None:
                continue
            candidate_id = int(candidate_id_raw)
            payload = row.get("payload")
            if not isinstance(payload, dict):
                continue
            amounts = _collect_monetary_values(payload)
            if not amounts:
                continue
            totals_by_candidate[candidate_id] = totals_by_candidate.get(candidate_id, 0.0) + max(amounts)
            entries_by_candidate[candidate_id] = entries_by_candidate.get(candidate_id, 0) + 1

        ranked_candidate_ids = sorted(
            totals_by_candidate.keys(),
            key=lambda candidate_id: (
                -totals_by_candidate[candidate_id],
                str(candidates_by_id.get(candidate_id, {}).get("nombre_completo") or ""),
            ),
        )
        rows: List[Dict[str, Any]] = []
        for candidate_id in ranked_candidate_ids[:safe_limit]:
            candidate = dict(candidates_by_id.get(candidate_id) or {})
            total_amount = round(float(totals_by_candidate.get(candidate_id) or 0.0), 2)
            candidate["ingresos_count"] = int(entries_by_candidate.get(candidate_id) or 0)
            candidate["total_ingresos_aprox"] = total_amount
            candidate["score"] = total_amount
            rows.append(candidate)

        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "income_amount_ranking | limit=%s estado=%s organizacion=%s candidates=%s with_data=%s rows=%s elapsed_ms=%.1f",
            safe_limit,
            estado,
            organizacion,
            len(all_candidate_ids),
            len(totals_by_candidate),
            len(rows),
            elapsed_ms,
        )
        return {
            "rows": rows,
            "candidates_with_data": len(totals_by_candidate),
            "total_candidates_considered": len(all_candidate_ids),
        }

    def get_text_match_overview(
        self,
        query: str,
        *,
        limit: int = 5,
        estado: Optional[str] = None,
        organizacion: Optional[str] = None,
    ) -> Dict[str, Any]:
        count_columns = self._base_count_columns()
        count_projection = _build_count_projection_sql(count_columns, table_alias="v")
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                with normalized as (
                    select nullif(trim(coalesce(%s::text, '')), '') as q
                ),
                clean as (
                    select
                        q,
                        upper(regexp_replace(coalesce(q, ''), '[^[:alnum:][:space:]_]', ' ', 'g')) as q_clean
                    from normalized
                ),
                tokens as (
                    select distinct token
                    from clean c
                    cross join lateral regexp_split_to_table(c.q_clean, '[[:space:]]+') token
                    where c.q is not null
                      and token <> ''
                      and length(token) >= 3
                      and token not in (
                          'QUE', 'CUANTO', 'CUANTOS', 'CUANTA', 'CUANTAS',
                          'CUAL', 'CUALES', 'TIENE', 'TIENEN',
                          'CON', 'PARA', 'LOS', 'LAS', 'DEL', 'DE', 'EL', 'LA',
                          'Y', 'O', 'EN', 'UN', 'UNA',
                          'CANDIDATO', 'CANDIDATOS'
                      )
                ),
                token_context as (
                    select coalesce((select string_agg(token, ' ') from tokens), '') as compact_terms
                ),
                ranked as (
                    select
                        v.id_hoja_vida,
                        v.nombre_completo,
                        v.organizacion_politica,
                        v.cargo,
                        v.estado{count_projection},
                        case
                            when c.q is null then 0
                            else
                                (case
                                    when tc.compact_terms <> ''
                                     and upper(coalesce(v.nombre_completo, '')) like '%%' || tc.compact_terms || '%%'
                                    then 200 else 0
                                end) +
                                coalesce((
                                    select sum(
                                        (case when upper(coalesce(v.nombre_completo, '')) like '%%' || t.token || '%%' then 90 else 0 end) +
                                        (case when upper(coalesce(v.organizacion_politica, '')) like '%%' || t.token || '%%' then 60 else 0 end) +
                                        (case when upper(coalesce(v.cargo, '')) like '%%' || t.token || '%%' then 35 else 0 end) +
                                        (case when upper(v.context_text) like '%%' || t.token || '%%' then 20 else 0 end)
                                    )::int
                                    from tokens t
                                ), 0)
                        end as score,
                        c.q
                    from jne.v_copilot_context v
                    cross join clean c
                    cross join token_context tc
                ),
                filtered as (
                    select *
                    from ranked
                    where q is not null
                      and score > 0
                      and (%s::text is null or upper(coalesce(estado, '')) = upper(%s::text))
                      and (
                        %s::text is null
                        or upper(coalesce(organizacion_politica, '')) like '%%' || upper(%s::text) || '%%'
                      )
                ),
                ordered as (
                    select
                        *,
                        row_number() over (order by score desc, nombre_completo asc) as rn
                    from filtered
                )
                select
                    (select count(*)::int from filtered) as total_candidates,
                    coalesce(
                        jsonb_agg(to_jsonb(o) - 'q' - 'rn' order by o.rn)
                        filter (where o.rn <= %s),
                        '[]'::jsonb
                    ) as top_rows
                from ordered o
                """,
                (
                    query,
                    estado,
                    estado,
                    organizacion,
                    organizacion,
                    max(1, min(limit, 100)),
                ),
            )
            row = cur.fetchone() or {}
            top_rows = row.get("top_rows") or []
            if not isinstance(top_rows, list):
                top_rows = []
            return {
                "metric": "text_search",
                "total_candidates": int(row.get("total_candidates") or 0),
                "top_candidates": top_rows,
                "label": "candidato(s) con coincidencia textual",
                "definition": (
                    "coincidencia de terminos en nombre, organizacion, cargo y contexto consolidado"
                ),
            }

    def get_aggregate_metrics(self) -> Dict[str, int]:
        catalog = self.get_metric_catalog()
        projections = [sql.SQL("count(distinct id_hoja_vida)::int as total_candidates")]
        for spec in catalog.values():
            metric_key = str(spec.get("metric") or "")
            field_name = str(spec.get("metric_field") or f"candidates_with_{metric_key}")
            expression = str(spec.get("expression") or "").strip()
            if metric_key == "total_candidates":
                continue
            if not metric_key or not expression or not _safe_identifier(field_name):
                continue
            projections.append(
                sql.SQL(
                    "count(distinct case when ({expr}) > 0 then id_hoja_vida end)::int as {alias}"
                ).format(
                    expr=sql.SQL(expression),
                    alias=sql.Identifier(field_name),
                )
            )

        with self.conn.cursor() as cur:
            query = sql.SQL("select {projections} from jne.v_copilot_context").format(
                projections=sql.SQL(", ").join(projections),
            )
            cur.execute(query)
            row = cur.fetchone() or {}
            return {k: int(v or 0) for k, v in row.items()}

    def get_candidate_detail(self, id_hoja_vida: int, *, include_raw: bool = True) -> Optional[Dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                select
                    c.id_hoja_vida,
                    c.id_proceso_electoral,
                    c.id_tipo_eleccion,
                    c.id_organizacion_politica,
                    c.organizacion_politica,
                    c.numero_documento,
                    c.nombre_completo,
                    c.cargo,
                    c.estado,
                    c.numero_candidato,
                    c.postula_departamento,
                    c.postula_provincia,
                    c.postula_distrito,
                    c.updated_at,
                    coalesce(v.sentencias_penales_count, 0) as sentencias_penales_count,
                    coalesce(v.sentencias_obligaciones_count, 0) as sentencias_obligaciones_count,
                    coalesce(v.expedientes_count, 0) as expedientes_count,
                    coalesce(v.ingresos_count, 0) as ingresos_count,
                    coalesce(v.bienes_inmuebles_count, 0) as bienes_inmuebles_count,
                    coalesce(v.bienes_muebles_count, 0) as bienes_muebles_count,
                    coalesce(v.otros_bienes_muebles_count, 0) as otros_bienes_muebles_count,
                    coalesce(v.titularidades_count, 0) as titularidades_count,
                    coalesce(v.anotaciones_count, 0) as anotaciones_count
                from jne.candidatos c
                left join jne.v_copilot_context v
                    on v.id_hoja_vida = c.id_hoja_vida
                where c.id_hoja_vida = %s
                """,
                (id_hoja_vida,),
            )
            candidate = cur.fetchone()

        if not candidate:
            return None

        detail = {
            "candidate": candidate,
            "postulaciones": self._get_candidate_postulaciones(id_hoja_vida),
            "postulacion_resumen_persona": self._get_postulacion_resumen_persona(
                candidate.get("numero_documento"),
                id_hoja_vida,
            ),
            "sentencias_penales": self._get_payload_items("jne.sentencias_penales", id_hoja_vida),
            "sentencias_obligaciones": self._get_payload_items("jne.sentencias_obligaciones", id_hoja_vida),
            "declaracion_ingresos": self._get_payload_items("jne.declaracion_ingresos", id_hoja_vida),
            "bienes_inmuebles": self._get_payload_items("jne.bienes_inmuebles", id_hoja_vida),
            "bienes_muebles": self._get_payload_items("jne.bienes_muebles", id_hoja_vida),
            "otros_bienes_muebles": self._get_payload_items("jne.otros_bienes_muebles", id_hoja_vida),
            "titularidad_acciones": self._get_payload_items("jne.titularidad_acciones", id_hoja_vida),
            "anotaciones_marginales": self._get_payload_items("jne.anotaciones_marginales", id_hoja_vida),
            "expedientes": self._get_payload_items("jne.expedientes_candidato", id_hoja_vida),
            "instagram": self.get_candidate_instagram(id_hoja_vida),
        }
        if include_raw:
            detail["hoja_vida_raw"] = self._get_hoja_vida_raw(id_hoja_vida)
            detail["hoja_vida_secciones_raw"] = self._get_hoja_vida_secciones_raw(id_hoja_vida)
            detail["anotaciones_raw"] = self._get_payload_single("jne.anotaciones_raw", id_hoja_vida)
            detail["expedientes_raw"] = self._get_payload_single("jne.expedientes_raw", id_hoja_vida)
        return detail

    def get_candidate_instagram(self, id_hoja_vida: int, *, media_limit: int = 25) -> Dict[str, Any]:
        accounts = self._get_instagram_accounts(id_hoja_vida)
        profiles = self._get_instagram_profiles_latest(id_hoja_vida)
        media = self._get_instagram_media_latest(id_hoja_vida, media_limit=media_limit)

        return {
            "id_hoja_vida": id_hoja_vida,
            "accounts": accounts,
            "latest_profiles": profiles,
            "latest_media": media,
        }

    def get_dashboard_insights(
        self,
        *,
        top_universities: int = 12,
        tipo_eleccion_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        normalized_tipo_eleccion_id = (
            int(tipo_eleccion_id) if tipo_eleccion_id is not None and int(tipo_eleccion_id) > 0 else None
        )
        top_limit = max(1, top_universities)
        cache_key = f"{top_limit}:{normalized_tipo_eleccion_id or 0}"
        cached = self._get_dashboard_cache(cache_key)
        if cached is not None:
            return cached

        candidate_ids = self._get_dashboard_candidate_ids(normalized_tipo_eleccion_id)
        election_type_options = self._get_dashboard_tipo_eleccion_options()
        selected_election_type = next(
            (
                option
                for option in election_type_options
                if int(option.get("id_tipo_eleccion") or 0) == (normalized_tipo_eleccion_id or 0)
            ),
            None,
        )
        selected_election_type_label = (
            str(selected_election_type.get("tipo_eleccion") or "").strip()
            if selected_election_type
            else "Todos"
        )

        totals = self._get_dashboard_totals(candidate_ids)
        income = self._get_income_distribution(candidate_ids)
        denuncias = self._get_denuncias_distribution(candidate_ids)
        universities = self._get_universities_distribution(candidate_ids, top_limit=top_limit)
        totals["candidates_with_income_amount"] = income.get("candidates_with_data", 0)
        totals["candidates_with_denuncias"] = denuncias.get("candidates_with_denuncias", 0)
        totals["candidates_with_university"] = universities.get("candidates_with_data", 0)

        payload = {
            "generated_at": self._get_generated_at(),
            "totals": totals,
            "charts": {
                "ingresos": income.get("series", []),
                "denuncias": denuncias.get("series", []),
                "universidades": universities.get("series", []),
            },
            "notes": {
                "ingresos": "Distribucion aproximada por candidato usando montos detectados en declaracion_ingresos.",
                "denuncias": "Denuncias aproximadas = expedientes + sentencias penales + sentencias por obligaciones.",
                "universidades": "Universidades detectadas desde hoja_vida_raw; una misma persona puede aportar a mas de una universidad.",
            },
            "filters": {
                "selected_tipo_eleccion_id": normalized_tipo_eleccion_id,
                "selected_tipo_eleccion_label": selected_election_type_label,
                "tipo_eleccion_options": election_type_options,
            },
        }
        self._set_dashboard_cache(cache_key, payload)
        return payload

    def _get_dashboard_cache(self, cache_key: str) -> Optional[Dict[str, Any]]:
        if self._dashboard_cache_ttl_seconds <= 0:
            return None
        now = time.monotonic()
        with self._dashboard_cache_lock:
            expiry = self._dashboard_cache_expiry.get(cache_key, 0.0)
            cached = self._dashboard_cache.get(cache_key)
            if cached is None or now > expiry:
                return None
            return deepcopy(cached)

    def _set_dashboard_cache(self, cache_key: str, payload: Dict[str, Any]) -> None:
        if self._dashboard_cache_ttl_seconds <= 0:
            return
        with self._dashboard_cache_lock:
            self._dashboard_cache[cache_key] = deepcopy(payload)
            self._dashboard_cache_expiry[cache_key] = (
                time.monotonic() + float(self._dashboard_cache_ttl_seconds)
            )

    def _get_dashboard_candidate_ids(self, tipo_eleccion_id: Optional[int]) -> List[int]:
        with self.conn.cursor() as cur:
            if tipo_eleccion_id is None:
                cur.execute(
                    """
                    select id_hoja_vida
                    from jne.candidatos
                    where id_hoja_vida is not null
                    """
                )
            else:
                cur.execute(
                    """
                    select distinct id_hoja_vida
                    from jne.candidatos_postulaciones
                    where id_hoja_vida is not null
                      and id_tipo_eleccion = %s
                    """,
                    (tipo_eleccion_id,),
                )
            rows = cur.fetchall()
        return [int(row.get("id_hoja_vida")) for row in rows if row.get("id_hoja_vida") is not None]

    def _get_dashboard_tipo_eleccion_options(self) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                select
                    id_tipo_eleccion,
                    coalesce(
                        nullif(trim(max(tipo_eleccion)), ''),
                        'Tipo ' || id_tipo_eleccion::text
                    ) as tipo_eleccion,
                    count(distinct id_hoja_vida)::int as candidates_count
                from jne.candidatos_postulaciones
                where id_tipo_eleccion is not null
                group by id_tipo_eleccion
                order by count(distinct id_hoja_vida) desc, id_tipo_eleccion asc
                """
            )
            rows = cur.fetchall()
        return [
            {
                "id_tipo_eleccion": int(row.get("id_tipo_eleccion") or 0),
                "tipo_eleccion": str(row.get("tipo_eleccion") or "").strip(),
                "candidates_count": int(row.get("candidates_count") or 0),
            }
            for row in rows
            if row.get("id_tipo_eleccion") is not None
        ]

    def _get_generated_at(self) -> str:
        with self.conn.cursor() as cur:
            cur.execute("select to_char(now() at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') as ts")
            row = cur.fetchone()
            return str((row or {}).get("ts") or "")

    def _get_dashboard_totals(self, candidate_ids: List[int]) -> Dict[str, int]:
        if not candidate_ids:
            return {
                "total_candidates": 0,
                "candidates_with_income_rows": 0,
                "candidates_with_expedientes": 0,
                "candidates_with_sentencias_penales": 0,
                "candidates_with_sentencias_obligaciones": 0,
            }

        with self.conn.cursor() as cur:
            cur.execute(
                """
                select
                    (select count(distinct id_hoja_vida)
                     from jne.declaracion_ingresos
                     where id_hoja_vida = any(%s::bigint[])) as candidates_with_income_rows,
                    (select count(distinct id_hoja_vida)
                     from jne.expedientes_candidato
                     where id_hoja_vida = any(%s::bigint[])) as candidates_with_expedientes,
                    (select count(distinct id_hoja_vida)
                     from jne.sentencias_penales
                     where id_hoja_vida = any(%s::bigint[])) as candidates_with_sentencias_penales,
                    (select count(distinct id_hoja_vida)
                     from jne.sentencias_obligaciones
                     where id_hoja_vida = any(%s::bigint[])) as candidates_with_sentencias_obligaciones
                """,
                (candidate_ids, candidate_ids, candidate_ids, candidate_ids),
            )
            row = cur.fetchone() or {}
            return {
                "total_candidates": len(candidate_ids),
                "candidates_with_income_rows": int(row.get("candidates_with_income_rows") or 0),
                "candidates_with_expedientes": int(row.get("candidates_with_expedientes") or 0),
                "candidates_with_sentencias_penales": int(row.get("candidates_with_sentencias_penales") or 0),
                "candidates_with_sentencias_obligaciones": int(
                    row.get("candidates_with_sentencias_obligaciones") or 0
                ),
            }

    def _get_income_distribution(self, candidate_ids: List[int]) -> Dict[str, Any]:
        all_candidates = [int(candidate_id) for candidate_id in candidate_ids]
        if not all_candidates:
            return {
                "series": [
                    {"label": "Sin datos", "count": 0},
                    {"label": "0 - 30k", "count": 0},
                    {"label": "30k - 100k", "count": 0},
                    {"label": "100k - 250k", "count": 0},
                    {"label": "250k - 500k", "count": 0},
                    {"label": "500k+", "count": 0},
                ],
                "candidates_with_data": 0,
            }

        with self.conn.cursor() as cur:
            cur.execute(
                """
                select id_hoja_vida, payload
                from jne.declaracion_ingresos
                where id_hoja_vida = any(%s::bigint[])
                order by id_hoja_vida, item_index
                """,
                (all_candidates,),
            )
            rows = cur.fetchall()

        totals_by_candidate: Dict[int, float] = {}
        for row in rows:
            id_hoja_vida = row.get("id_hoja_vida")
            if id_hoja_vida is None:
                continue
            payload = row.get("payload")
            if not isinstance(payload, dict):
                continue
            amounts = _collect_monetary_values(payload)
            if not amounts:
                continue
            totals_by_candidate[int(id_hoja_vida)] = totals_by_candidate.get(int(id_hoja_vida), 0.0) + max(amounts)

        buckets = {
            "Sin datos": 0,
            "0 - 30k": 0,
            "30k - 100k": 0,
            "100k - 250k": 0,
            "250k - 500k": 0,
            "500k+": 0,
        }

        for candidate_id in all_candidates:
            amount = totals_by_candidate.get(candidate_id)
            if amount is None:
                buckets["Sin datos"] += 1
            elif amount <= 30_000:
                buckets["0 - 30k"] += 1
            elif amount <= 100_000:
                buckets["30k - 100k"] += 1
            elif amount <= 250_000:
                buckets["100k - 250k"] += 1
            elif amount <= 500_000:
                buckets["250k - 500k"] += 1
            else:
                buckets["500k+"] += 1

        return {
            "series": [{"label": label, "count": count} for label, count in buckets.items()],
            "candidates_with_data": len(totals_by_candidate),
        }

    def _get_denuncias_distribution(self, candidate_ids: List[int]) -> Dict[str, Any]:
        if not candidate_ids:
            return {
                "series": [
                    {"label": "0", "count": 0},
                    {"label": "1", "count": 0},
                    {"label": "2 - 3", "count": 0},
                    {"label": "4 - 6", "count": 0},
                    {"label": "7+", "count": 0},
                ],
                "candidates_with_data": 0,
                "candidates_with_denuncias": 0,
            }

        with self.conn.cursor() as cur:
            cur.execute(
                """
                with base as (
                    select unnest(%s::bigint[])::bigint as id_hoja_vida
                ),
                exp as (
                    select id_hoja_vida, count(*)::int as cnt
                    from jne.expedientes_candidato
                    where id_hoja_vida = any(%s::bigint[])
                    group by id_hoja_vida
                ),
                sp as (
                    select id_hoja_vida, count(*)::int as cnt
                    from jne.sentencias_penales
                    where id_hoja_vida = any(%s::bigint[])
                    group by id_hoja_vida
                ),
                so as (
                    select id_hoja_vida, count(*)::int as cnt
                    from jne.sentencias_obligaciones
                    where id_hoja_vida = any(%s::bigint[])
                    group by id_hoja_vida
                )
                select
                    b.id_hoja_vida,
                    coalesce(exp.cnt, 0) + coalesce(sp.cnt, 0) + coalesce(so.cnt, 0) as denuncias_total
                from base b
                left join exp on exp.id_hoja_vida = b.id_hoja_vida
                left join sp on sp.id_hoja_vida = b.id_hoja_vida
                left join so on so.id_hoja_vida = b.id_hoja_vida
                """,
                (candidate_ids, candidate_ids, candidate_ids, candidate_ids),
            )
            rows = cur.fetchall()

        buckets = {
            "0": 0,
            "1": 0,
            "2 - 3": 0,
            "4 - 6": 0,
            "7+": 0,
        }

        for row in rows:
            total = int(row.get("denuncias_total") or 0)
            if total == 0:
                buckets["0"] += 1
            elif total == 1:
                buckets["1"] += 1
            elif total <= 3:
                buckets["2 - 3"] += 1
            elif total <= 6:
                buckets["4 - 6"] += 1
            else:
                buckets["7+"] += 1

        candidates_with_denuncias = sum(1 for row in rows if int(row.get("denuncias_total") or 0) > 0)
        return {
            "series": [{"label": label, "count": count} for label, count in buckets.items()],
            "candidates_with_data": len(rows),
            "candidates_with_denuncias": candidates_with_denuncias,
        }

    def _get_universities_distribution(self, candidate_ids: List[int], *, top_limit: int = 12) -> Dict[str, Any]:
        if not candidate_ids:
            return {"series": [], "candidates_with_data": 0}

        with self.conn.cursor() as cur:
            cur.execute(
                """
                select id_hoja_vida, payload
                from jne.hoja_vida_raw
                where id_hoja_vida = any(%s::bigint[])
                """,
                (candidate_ids,),
            )
            rows = cur.fetchall()

        counter: Counter[str] = Counter()
        candidates_with_data = 0
        for row in rows:
            payload = row.get("payload")
            if not isinstance(payload, dict):
                continue

            universities = set(_collect_universities(payload))
            if not universities:
                continue

            candidates_with_data += 1
            for university in universities:
                counter[university] += 1

        if not counter:
            return {"series": [], "candidates_with_data": 0}

        top_items = counter.most_common(max(1, top_limit))
        remaining = sum(counter.values()) - sum(count for _, count in top_items)

        series = [{"label": name, "count": count} for name, count in top_items]
        if remaining > 0:
            series.append({"label": "OTRAS", "count": remaining})

        return {
            "series": series,
            "candidates_with_data": candidates_with_data,
        }

    def _get_hoja_vida_raw(self, id_hoja_vida: int) -> Dict[str, Any]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                select payload
                from jne.hoja_vida_raw
                where id_hoja_vida = %s
                """,
                (id_hoja_vida,),
            )
            row = cur.fetchone()
            if not row:
                return {}
            payload = row.get("payload")
            if isinstance(payload, dict):
                return payload
            return {}

    def _get_hoja_vida_secciones_raw(self, id_hoja_vida: int) -> Dict[str, Any]:
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    select section_name, payload
                    from jne.hoja_vida_secciones_raw
                    where id_hoja_vida = %s
                    """,
                    (id_hoja_vida,),
                )
                rows = cur.fetchall()
        except Exception:  # noqa: BLE001
            return {}

        sections: Dict[str, Any] = {}
        for row in rows:
            section_name = str(row.get("section_name"))
            sections[section_name] = row.get("payload")
        return sections

    def _get_payload_single(self, table_name: str, id_hoja_vida: int) -> Dict[str, Any]:
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    f"""
                    select payload
                    from {table_name}
                    where id_hoja_vida = %s
                    """,
                    (id_hoja_vida,),
                )
                row = cur.fetchone()
        except Exception:  # noqa: BLE001
            return {}

        if not row:
            return {}
        payload = row.get("payload")
        if isinstance(payload, dict):
            return payload
        return {}

    def _get_payload_items(self, table_name: str, id_hoja_vida: int) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                select payload
                from {table_name}
                where id_hoja_vida = %s
                order by item_index asc
                """,
                (id_hoja_vida,),
            )
            rows = cur.fetchall()
            items: List[Dict[str, Any]] = []
            for row in rows:
                payload = row.get("payload")
                if isinstance(payload, dict):
                    items.append(payload)
            return items

    def _get_candidate_postulaciones(self, id_hoja_vida: int) -> List[Dict[str, Any]]:
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    select
                        id_proceso_electoral,
                        id_tipo_eleccion,
                        tipo_eleccion,
                        segmento_postulacion,
                        id_organizacion_politica,
                        organizacion_politica,
                        cargo,
                        estado,
                        numero_candidato,
                        updated_at
                    from jne.candidatos_postulaciones
                    where id_hoja_vida = %s
                    order by id_proceso_electoral, id_tipo_eleccion, updated_at desc
                    """,
                    (id_hoja_vida,),
                )
                return list(cur.fetchall())
        except Exception:  # noqa: BLE001
            return []

    def _get_postulacion_resumen_persona(
        self,
        numero_documento: Optional[str],
        id_hoja_vida: int,
    ) -> Optional[Dict[str, Any]]:
        try:
            with self.conn.cursor() as cur:
                if numero_documento and str(numero_documento).strip():
                    cur.execute(
                        """
                        select *
                        from jne.v_postulaciones_resumen_persona
                        where numero_documento = %s
                        limit 1
                        """,
                        (str(numero_documento).strip(),),
                    )
                else:
                    persona_key = f"IDHV:{id_hoja_vida}"
                    cur.execute(
                        """
                        select *
                        from jne.v_postulaciones_resumen_persona
                        where persona_key = %s
                        limit 1
                        """,
                        (persona_key,),
                    )
                return cur.fetchone()
        except Exception:  # noqa: BLE001
            return None

    def _get_instagram_accounts(self, id_hoja_vida: int) -> List[Dict[str, Any]]:
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    select
                        id,
                        plataforma,
                        username,
                        profile_url,
                        source,
                        is_oficial,
                        is_public,
                        notes,
                        created_at,
                        updated_at
                    from jne.candidato_redes_sociales
                    where id_hoja_vida = %s
                      and plataforma = 'instagram'
                    order by is_oficial desc, username asc
                    """,
                    (id_hoja_vida,),
                )
                return list(cur.fetchall())
        except Exception:  # noqa: BLE001
            return []

    def _get_instagram_profiles_latest(self, id_hoja_vida: int) -> List[Dict[str, Any]]:
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    select
                        username,
                        ig_user_id,
                        account_type,
                        display_name,
                        biography,
                        website,
                        profile_picture_url,
                        followers_count,
                        follows_count,
                        media_count,
                        captured_at,
                        payload
                    from jne.v_instagram_profile_latest
                    where id_hoja_vida = %s
                    order by followers_count desc nulls last, username asc
                    """,
                    (id_hoja_vida,),
                )
                return list(cur.fetchall())
        except Exception:  # noqa: BLE001
            return []

    def _get_instagram_media_latest(self, id_hoja_vida: int, *, media_limit: int) -> List[Dict[str, Any]]:
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    select
                        username,
                        media_id,
                        media_type,
                        media_product_type,
                        caption,
                        permalink,
                        media_url,
                        thumbnail_url,
                        timestamp_utc,
                        comments_count,
                        like_count,
                        view_count,
                        captured_at,
                        payload
                    from jne.v_instagram_media_latest
                    where id_hoja_vida = %s
                    order by timestamp_utc desc nulls last, captured_at desc
                    limit %s
                    """,
                    (id_hoja_vida, max(1, min(media_limit, 100))),
                )
                return list(cur.fetchall())
        except Exception:  # noqa: BLE001
            return []
