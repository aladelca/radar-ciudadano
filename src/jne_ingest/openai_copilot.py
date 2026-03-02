from __future__ import annotations

import json
import logging
import re
import time
from typing import Any, Dict, List, Optional

import httpx

from jne_ingest.config import AppConfig

logger = logging.getLogger("jne_ingest.openai_copilot")


class OpenAICopilotError(RuntimeError):
    pass


_VALID_PLAN_INTENTS = {"aggregate_count", "search"}
_VALID_PLAN_RESULT_TYPES = {"aggregate", "rows"}
_VALID_ANSWER_LEVELS = {"candidate", "organization", "election_segment", "general"}
_VALID_EXECUTION_MODES = {"sql", "derived"}
_VALID_DERIVED_RESOLVERS = {"income_amount_ranking"}
_SEGMENT_SENADO_PATTERN = re.compile(r"\b(senad|senador|senadores|senado)\b", flags=re.IGNORECASE)
_SEGMENT_PRESIDENCIAL_PATTERN = re.compile(
    r"\b(presidencial|presidente|presidencia)\b",
    flags=re.IGNORECASE,
)
_PARTY_HINT_PATTERN = re.compile(
    r"\b(partido|organizacion|organización|agrupacion|agrupación|movimiento)\b",
    flags=re.IGNORECASE,
)
_COUNT_HINT_PATTERN = re.compile(
    r"\b(cuant|cantidad|numero|n[uú]mero|total|conteo|contar|cuenta)\b",
    flags=re.IGNORECASE,
)
_RANKING_HINT_PATTERN = re.compile(
    r"\b(top|mas|m[aá]s|mayor|lider|lidera|ranking)\b",
    flags=re.IGNORECASE,
)


class OpenAICopilotService:
    def __init__(self, config: AppConfig) -> None:
        self._api_key = config.openai_api_key
        self._model = config.openai_model
        self._timeout = config.openai_timeout_seconds

    @property
    def enabled(self) -> bool:
        return bool(self._api_key)

    @property
    def model(self) -> str:
        return self._model

    def generate_summary(
        self,
        *,
        query: str,
        rows: List[Dict[str, Any]],
        evidence: List[Dict[str, Any]],
        estado: Optional[str],
        organizacion: Optional[str],
        conversation_history: Optional[List[Dict[str, str]]] = None,
    ) -> str:
        if not self._api_key:
            raise OpenAICopilotError("OPENAI_API_KEY no configurada.")
        start = time.perf_counter()

        prompt = self._build_summary_prompt(
            query=query,
            rows=rows,
            evidence=evidence,
            estado=estado,
            organizacion=organizacion,
            conversation_history=conversation_history or [],
        )
        response_payload = self._call_responses_api(
            prompt,
            temperature=0.2,
            max_output_tokens=600,
        )
        text = self._extract_text(response_payload)
        if not text:
            raise OpenAICopilotError("Respuesta vacia desde OpenAI.")
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "LLM summary generated | query=%s rows=%s evidence=%s elapsed_ms=%.1f",
            (query or "").strip()[:140],
            len(rows),
            len(evidence),
            elapsed_ms,
        )
        return text.strip()

    def classify_query_purpose(
        self,
        *,
        query: str,
        metric_catalog: Dict[str, Dict[str, Any]],
        conversation_history: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        if not self._api_key:
            raise OpenAICopilotError("OPENAI_API_KEY no configurada.")

        prompt = self._build_planner_prompt(
            query=query,
            metric_catalog=metric_catalog,
            conversation_history=conversation_history or [],
        )
        payload = self._call_responses_api(
            prompt,
            temperature=0.0,
            max_output_tokens=400,
        )
        text = self._extract_text(payload)
        if not text:
            raise OpenAICopilotError("Planner IA devolvio respuesta vacia.")

        parsed = self._extract_json_object(text)
        intent = str(parsed.get("intent") or "").strip().lower()
        if intent not in {"aggregate_count", "search"}:
            intent = "search"

        metric_key_raw = parsed.get("metric_key")
        metric_key = str(metric_key_raw).strip() if metric_key_raw is not None else None
        if metric_key and metric_key not in metric_catalog:
            metric_key = None

        confidence_raw = parsed.get("confidence")
        try:
            confidence = float(confidence_raw)
        except (TypeError, ValueError):
            confidence = 0.0
        confidence = max(0.0, min(confidence, 1.0))

        reasoning = str(parsed.get("reasoning") or "").strip()
        return {
            "intent": intent,
            "metric_key": metric_key,
            "confidence": confidence,
            "reasoning": reasoning,
        }

    def generate_sql_plan(
        self,
        *,
        query: str,
        schema_context: Dict[str, Any],
        limit: int,
        estado: Optional[str],
        organizacion: Optional[str],
        conversation_history: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        if not self._api_key:
            raise OpenAICopilotError("OPENAI_API_KEY no configurada.")
        start = time.perf_counter()

        objective_prompt = self._build_objective_agent_prompt(
            query=query,
            conversation_history=conversation_history or [],
        )
        objective_payload = self._call_responses_api(
            objective_prompt,
            temperature=0.0,
            max_output_tokens=350,
        )
        objective_text = self._extract_text(objective_payload)
        if not objective_text:
            raise OpenAICopilotError("Objective agent devolvio respuesta vacia.")
        objective_parsed = self._extract_json_object(objective_text)
        objective_plan = self._normalize_objective_agent_output(objective_parsed)
        if not self._has_explicit_count_intent(query) and objective_plan.get("intent") == "aggregate_count":
            objective_plan["intent"] = "search"
        if self._is_ranking_query(query) and objective_plan.get("intent") == "aggregate_count":
            objective_plan["intent"] = "search"
        logger.info(
            "Objective agent | query=%s intent=%s answer_level=%s objective=%s",
            (query or "").strip()[:140],
            objective_plan.get("intent"),
            objective_plan.get("answer_level"),
            str(objective_plan.get("objective") or "")[:160],
        )

        builder_prompt = self._build_sql_builder_prompt(
            query=query,
            objective_plan=objective_plan,
            schema_context=schema_context,
            limit=limit,
            estado=estado,
            organizacion=organizacion,
            conversation_history=conversation_history or [],
        )
        payload = self._call_responses_api(
            builder_prompt,
            temperature=0.0,
            max_output_tokens=900,
        )
        text = self._extract_text(payload)
        if not text:
            raise OpenAICopilotError("SQL builder agent devolvio respuesta vacia.")

        parsed = self._extract_json_object(text)
        sql_plan = self._normalize_sql_plan_output(
            parsed,
            query=query,
            estado=estado,
            organizacion=organizacion,
        )
        objective_intent = str(objective_plan.get("intent") or "")
        sql_intent = str(sql_plan.get("intent") or "")
        if objective_intent and sql_intent and objective_intent != sql_intent:
            if objective_intent == "search" and sql_intent == "aggregate_count":
                raise OpenAICopilotError(
                    "Desalineacion agentes: objective=search pero sql_builder=aggregate_count."
                )
            sql_plan["intent"] = objective_intent
        objective_answer_level = str(objective_plan.get("answer_level") or "").strip()
        if objective_answer_level in _VALID_ANSWER_LEVELS and objective_answer_level != "general":
            if str(sql_plan.get("answer_level") or "").strip() != objective_answer_level:
                sql_plan["answer_level"] = objective_answer_level

        objective_text_norm = str(objective_plan.get("objective") or "").strip()
        if objective_text_norm:
            sql_plan["objective"] = objective_text_norm

        sql_plan["objective_agent_reasoning"] = str(objective_plan.get("reasoning") or "").strip()
        elapsed_ms = (time.perf_counter() - start) * 1000
        sql_preview = str(sql_plan.get("sql") or "").replace("\n", " ")[:220]
        logger.info(
            "SQL builder agent | query=%s intent=%s result_type=%s answer_level=%s mode=%s resolver=%s can_answer=%s elapsed_ms=%.1f sql=%s",
            (query or "").strip()[:140],
            sql_plan.get("intent"),
            sql_plan.get("result_type"),
            sql_plan.get("answer_level"),
            sql_plan.get("execution_mode"),
            sql_plan.get("derived_resolver"),
            sql_plan.get("can_answer"),
            elapsed_ms,
            sql_preview,
        )
        return sql_plan

    @staticmethod
    def _normalize_objective_agent_output(parsed: Dict[str, Any]) -> Dict[str, Any]:
        required_keys = {"objective", "intent", "answer_level", "reasoning"}
        missing_keys = sorted(required_keys.difference(set(parsed.keys())))
        if missing_keys == ["answer_level"]:
            parsed["answer_level"] = "general"
            missing_keys = []
        if missing_keys:
            raise OpenAICopilotError(
                "Objective agent incompleto. Faltan campos: " + ", ".join(missing_keys)
            )

        objective = str(parsed.get("objective") or "").strip()
        intent = str(parsed.get("intent") or "").strip().lower()
        answer_level = OpenAICopilotService._normalize_answer_level(parsed.get("answer_level"))
        reasoning = str(parsed.get("reasoning") or "").strip()
        if intent not in _VALID_PLAN_INTENTS:
            raise OpenAICopilotError("Objective agent devolvio intent invalido.")
        if not objective:
            raise OpenAICopilotError("Objective agent no definio objective.")
        return {
            "objective": objective,
            "intent": intent,
            "answer_level": answer_level,
            "reasoning": reasoning,
        }

    @staticmethod
    def _normalize_answer_level(value: Any) -> str:
        normalized = str(value or "").strip().lower()
        aliases = {
            "candidate": "candidate",
            "candidato": "candidate",
            "candidatos": "candidate",
            "organization": "organization",
            "organizacion": "organization",
            "organización": "organization",
            "partido": "organization",
            "party": "organization",
            "election_segment": "election_segment",
            "segment": "election_segment",
            "segmento": "election_segment",
            "general": "general",
        }
        mapped = aliases.get(normalized, normalized)
        if mapped not in _VALID_ANSWER_LEVELS:
            return "general"
        return mapped

    @staticmethod
    def _parse_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes", "y", "si", "sí"}:
                return True
            if normalized in {"false", "0", "no", "n"}:
                return False
        return False

    @staticmethod
    def _normalize_required_data(value: Any) -> List[Dict[str, Any]]:
        if not isinstance(value, list):
            return []
        normalized_items: List[Dict[str, Any]] = []
        for item in value:
            if not isinstance(item, dict):
                continue
            table = str(item.get("table") or "").strip()
            columns_raw = item.get("columns")
            columns = []
            if isinstance(columns_raw, list):
                columns = [str(col).strip() for col in columns_raw if str(col).strip()]
            reason = str(item.get("reason") or "").strip()
            if table:
                normalized_items.append(
                    {
                        "table": table,
                        "columns": columns,
                        "reason": reason,
                    }
                )
        return normalized_items

    @staticmethod
    def _normalize_missing_info(value: Any) -> List[str]:
        if not isinstance(value, list):
            return []
        return [str(item).strip() for item in value if str(item).strip()]

    def _normalize_sql_plan_output(
        self,
        parsed: Dict[str, Any],
        *,
        query: str,
        estado: Optional[str],
        organizacion: Optional[str],
    ) -> Dict[str, Any]:
        required_keys = {
            "objective",
            "intent",
            "result_type",
            "answer_level",
            "can_answer",
            "required_data",
            "missing_info",
            "sql",
            "reasoning",
        }
        missing_keys = sorted(required_keys.difference(set(parsed.keys())))
        if missing_keys == ["answer_level"]:
            parsed["answer_level"] = "general"
            missing_keys = []
        if missing_keys:
            raise OpenAICopilotError(
                "Planner SQL IA incompleto. Faltan campos: " + ", ".join(missing_keys)
            )

        intent = str(parsed.get("intent") or "").strip().lower()
        if intent not in _VALID_PLAN_INTENTS:
            raise OpenAICopilotError("Planner SQL IA devolvio intent invalido.")

        result_type = str(parsed.get("result_type") or "").strip().lower()
        if result_type not in _VALID_PLAN_RESULT_TYPES:
            raise OpenAICopilotError("Planner SQL IA devolvio result_type invalido.")
        answer_level = self._normalize_answer_level(parsed.get("answer_level"))
        execution_mode_raw = str(parsed.get("execution_mode") or "").strip().lower()
        execution_mode = execution_mode_raw if execution_mode_raw in _VALID_EXECUTION_MODES else "sql"
        derived_resolver_raw = str(parsed.get("derived_resolver") or "").strip().lower()
        derived_resolver = derived_resolver_raw or None
        can_answer = self._parse_bool(parsed.get("can_answer"))
        if execution_mode == "derived" and can_answer and derived_resolver not in _VALID_DERIVED_RESOLVERS:
            raise OpenAICopilotError("Planner SQL IA devolvio derived_resolver invalido.")
        if execution_mode == "sql":
            derived_resolver = None
        if not self._has_explicit_count_intent(query) and intent == "aggregate_count":
            intent = "search"
        if self._is_ranking_query(query) and intent == "aggregate_count" and result_type == "rows":
            intent = "search"

        objective = str(parsed.get("objective") or "").strip()
        reasoning = str(parsed.get("reasoning") or "").strip()
        required_data = self._normalize_required_data(parsed.get("required_data"))
        missing_info = self._normalize_missing_info(parsed.get("missing_info"))

        sql_raw = parsed.get("sql")
        sql_text = str(sql_raw).strip() if isinstance(sql_raw, str) else None
        if sql_text == "":
            sql_text = None

        if execution_mode == "derived":
            if can_answer and not derived_resolver:
                raise OpenAICopilotError(
                    "Planner SQL IA marco execution_mode=derived sin derived_resolver."
                )
            if can_answer and derived_resolver == "income_amount_ranking" and answer_level != "candidate":
                raise OpenAICopilotError(
                    "Planner SQL IA debe usar answer_level=candidate para income_amount_ranking."
                )
            if result_type != "rows":
                raise OpenAICopilotError(
                    "Planner SQL IA debe usar result_type=rows cuando execution_mode=derived."
                )
            if intent == "aggregate_count":
                raise OpenAICopilotError(
                    "Planner SQL IA no puede usar intent=aggregate_count con execution_mode=derived."
                )
            if sql_text:
                raise OpenAICopilotError(
                    "Planner SQL IA no debe enviar SQL cuando execution_mode=derived."
                )
            sql_text = None
            if not can_answer and not missing_info:
                missing_info = ["No hay informacion suficiente para resolver con modo derivado."]
        elif not can_answer:
            sql_text = None
            if not missing_info:
                missing_info = ["No hay informacion suficiente en schema para construir SQL."]
        else:
            if not sql_text:
                raise OpenAICopilotError("Planner SQL IA marco can_answer=true pero no entrego SQL.")
            normalized_sql = sql_text.strip().lower()
            if not (normalized_sql.startswith("select") or normalized_sql.startswith("with")):
                raise OpenAICopilotError("Planner SQL IA devolvio SQL no permitido.")
            if normalized_sql.endswith(";"):
                sql_text = sql_text.rstrip(";").strip()
            sql_lower = sql_text.lower()
            if estado and "estado" not in sql_lower:
                raise OpenAICopilotError(
                    "Planner SQL IA no aplico filtro 'estado' aun estando presente en request."
                )
            if organizacion and "organizacion" not in sql_lower:
                raise OpenAICopilotError(
                    "Planner SQL IA no aplico filtro 'organizacion' aun estando presente en request."
                )
            if result_type == "rows" and answer_level == "candidate" and "id_hoja_vida" not in sql_lower:
                raise OpenAICopilotError(
                    "Planner SQL IA debe incluir id_hoja_vida para consultas de nivel candidato."
                )
            self._validate_electoral_context_sql(
                query=query,
                sql_text=sql_text,
            )

        return {
            "objective": objective,
            "intent": intent,
            "result_type": result_type,
            "answer_level": answer_level,
            "execution_mode": execution_mode,
            "derived_resolver": derived_resolver,
            "can_answer": can_answer,
            "required_data": required_data,
            "missing_info": missing_info,
            "sql": sql_text,
            "reasoning": reasoning,
        }

    @staticmethod
    def _is_ranking_query(query: str) -> bool:
        normalized_query = str(query or "").strip().lower()
        if not normalized_query:
            return False
        has_ranking = bool(_RANKING_HINT_PATTERN.search(normalized_query))
        has_explicit_count = bool(_COUNT_HINT_PATTERN.search(normalized_query))
        return has_ranking and not has_explicit_count

    @staticmethod
    def _has_explicit_count_intent(query: str) -> bool:
        normalized_query = str(query or "").strip().lower()
        if not normalized_query:
            return False
        return bool(_COUNT_HINT_PATTERN.search(normalized_query))

    @staticmethod
    def _validate_electoral_context_sql(query: str, sql_text: str) -> None:
        normalized_query = str(query or "").strip().lower()
        if not normalized_query:
            return
        sql_lower = str(sql_text or "").lower()

        mentions_senado = bool(_SEGMENT_SENADO_PATTERN.search(normalized_query))
        mentions_presidencial = bool(_SEGMENT_PRESIDENCIAL_PATTERN.search(normalized_query))
        mentions_party = bool(_PARTY_HINT_PATTERN.search(normalized_query))

        segment_keywords_present = any(
            keyword in sql_lower
            for keyword in (
                "segmento_postulacion",
                "id_tipo_eleccion",
                "tipo_eleccion",
                "v_candidatos_segmento_postulacion",
                "candidatos_postulaciones",
                "v_postulaciones_resumen_persona",
            )
        )
        if mentions_senado:
            has_senado_filter = (
                "senado" in sql_lower
                or "senador" in sql_lower
                or "id_tipo_eleccion in (20, 21)" in sql_lower
                or "id_tipo_eleccion in (20,21)" in sql_lower
                or "id_tipo_eleccion = 20" in sql_lower
                or "id_tipo_eleccion = 21" in sql_lower
            )
            if not (segment_keywords_present and has_senado_filter):
                raise OpenAICopilotError(
                    "SQL builder no aplico contexto electoral de senado en la consulta."
                )

        if mentions_presidencial:
            has_presidencial_filter = (
                "presidencial" in sql_lower
                or "id_tipo_eleccion = 1" in sql_lower
                or "id_tipo_eleccion=1" in sql_lower
            )
            if not (segment_keywords_present and has_presidencial_filter):
                raise OpenAICopilotError(
                    "SQL builder no aplico contexto electoral presidencial en la consulta."
                )

        if mentions_party and "organizacion_politica" not in sql_lower:
            raise OpenAICopilotError(
                "SQL builder no considero campo de organizacion_politica para consulta por partido/organizacion."
            )

    def _call_responses_api(
        self,
        prompt: str,
        *,
        temperature: float,
        max_output_tokens: int,
    ) -> Dict[str, Any]:
        start = time.perf_counter()
        headers = {
            "authorization": f"Bearer {self._api_key}",
            "content-type": "application/json",
        }
        body = {
            "model": self._model,
            "input": prompt,
            "temperature": temperature,
            "max_output_tokens": max_output_tokens,
        }
        try:
            with httpx.Client(timeout=self._timeout) as client:
                resp = client.post("https://api.openai.com/v1/responses", headers=headers, json=body)
        except Exception as exc:  # noqa: BLE001
            raise OpenAICopilotError("No se pudo conectar a OpenAI.") from exc

        if resp.is_error:
            raise OpenAICopilotError(f"OpenAI devolvio estado {resp.status_code}.")

        payload = resp.json()
        if not isinstance(payload, dict):
            raise OpenAICopilotError("Formato invalido en respuesta OpenAI.")
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.debug(
            "OpenAI responses call | model=%s temperature=%.2f max_tokens=%s prompt_chars=%s elapsed_ms=%.1f",
            self._model,
            temperature,
            max_output_tokens,
            len(prompt or ""),
            elapsed_ms,
        )
        return payload

    @staticmethod
    def _extract_text(payload: Dict[str, Any]) -> str:
        output_text = payload.get("output_text")
        if isinstance(output_text, str) and output_text.strip():
            return output_text

        output = payload.get("output")
        if not isinstance(output, list):
            return ""

        parts: List[str] = []
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if not isinstance(content, list):
                continue
            for piece in content:
                if not isinstance(piece, dict):
                    continue
                text = piece.get("text")
                if isinstance(text, str) and text.strip():
                    parts.append(text.strip())
        return "\n".join(parts).strip()

    @staticmethod
    def _extract_json_object(text: str) -> Dict[str, Any]:
        candidate = (text or "").strip()
        if not candidate:
            raise OpenAICopilotError("Planner IA sin contenido.")

        fence_match = re.search(r"```(?:json)?\s*(\{.*\})\s*```", candidate, flags=re.DOTALL)
        if fence_match:
            candidate = fence_match.group(1).strip()

        try:
            payload = json.loads(candidate)
            if isinstance(payload, dict):
                return payload
        except json.JSONDecodeError:
            pass

        start = candidate.find("{")
        if start < 0:
            raise OpenAICopilotError("Planner IA no devolvio JSON valido.")
        depth = 0
        for index, ch in enumerate(candidate[start:], start=start):
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    fragment = candidate[start : index + 1]
                    try:
                        payload = json.loads(fragment)
                    except json.JSONDecodeError as exc:
                        raise OpenAICopilotError("Planner IA devolvio JSON invalido.") from exc
                    if isinstance(payload, dict):
                        return payload
                    break
        raise OpenAICopilotError("Planner IA no devolvio objeto JSON.")

    @staticmethod
    def _build_planner_prompt(
        *,
        query: str,
        metric_catalog: Dict[str, Dict[str, Any]],
        conversation_history: List[Dict[str, str]],
    ) -> str:
        compact_catalog: List[Dict[str, Any]] = []
        for metric_key, spec in metric_catalog.items():
            aliases_raw = spec.get("aliases", [])
            aliases = []
            if isinstance(aliases_raw, (set, list, tuple)):
                aliases = sorted({str(alias) for alias in aliases_raw if str(alias).strip()})[:12]
            compact_catalog.append(
                {
                    "metric_key": metric_key,
                    "label": str(spec.get("label") or ""),
                    "definition": str(spec.get("definition") or ""),
                    "aliases": aliases,
                }
            )

        instructions = (
            "Eres un planner SQL para un copilot ciudadano. "
            "Tu tarea es identificar el proposito de la pregunta y elegir una metrica solo desde el catalogo dado. "
            "Si la pregunta pide conteo/agregado, usa intent='aggregate_count'. "
            "Si la pregunta es exploratoria, comparativa o no mapeable a una metrica puntual, usa intent='search'. "
            "No inventes metricas ni columnas."
        )
        output_contract = (
            "Responde SOLO un JSON valido con este esquema: "
            '{"intent":"aggregate_count|search","metric_key":"<key o null>","confidence":0.0,"reasoning":"texto breve"}.'
        )

        context = {
            "query": query,
            "conversation_history": conversation_history[-3:],
            "available_metrics": compact_catalog,
        }
        return (
            f"{instructions}\n\n"
            f"{output_contract}\n\n"
            f"Contexto JSON:\n{json.dumps(context, ensure_ascii=True)}"
        )

    @staticmethod
    def _build_objective_agent_prompt(
        *,
        query: str,
        conversation_history: List[Dict[str, str]],
    ) -> str:
        instructions = (
            "You are Objective Agent. "
            "Your only job is to detect what the user wants to know."
        )
        rules = (
            "Rules:\n"
            "- intent='aggregate_count' only when user asks explicit quantity/count/total (e.g. cuantos, cantidad, numero, total, contar).\n"
            "- intent='search' for listing, exploration, comparison, profiles or attributes.\n"
            "- answer_level='candidate' when expected output is mainly per person/candidate.\n"
            "- answer_level='organization' when the user asks by party/organization.\n"
            "- answer_level='election_segment' when primary axis is presidencial/senado/segment.\n"
            "- answer_level='general' when mixed or ambiguous.\n"
            "- Questions asking 'top/mas/mayor' by entidad are ranking queries: use intent='search'.\n"
            "- Understand election context terms: presidencial, presidencia, senado, senador, partido, organizacion politica.\n"
            "- Do not generate SQL.\n"
            "- Return JSON only."
        )
        output_contract = (
            'Output JSON schema: {"objective":"text","intent":"aggregate_count|search","answer_level":"candidate|organization|election_segment|general","reasoning":"short"}'
        )
        context = {
            "query": query,
            "conversation_history": conversation_history[-3:],
        }
        return (
            f"{instructions}\n\n"
            f"{rules}\n\n"
            f"{output_contract}\n\n"
            f"Context JSON:\n{json.dumps(context, ensure_ascii=True)}"
        )

    @staticmethod
    def _build_sql_builder_prompt(
        *,
        query: str,
        objective_plan: Dict[str, Any],
        schema_context: Dict[str, Any],
        limit: int,
        estado: Optional[str],
        organizacion: Optional[str],
        conversation_history: List[Dict[str, str]],
    ) -> str:
        filters: Dict[str, str] = {}
        if estado:
            filters["estado"] = estado
        if organizacion:
            filters["organizacion"] = organizacion

        safe_limit = max(1, min(limit, 200))
        instructions = (
            "Eres SQL Builder Agent para Postgres. "
            "Tu salida se ejecuta automaticamente en backend, debes ser exacto y seguro."
        )
        sections = (
            "SECCION 1 - Alignment with Objective Agent:\n"
            "- Respeta objective e intent recibidos.\n"
            "- Respeta answer_level recibido (candidate/organization/election_segment/general).\n"
            "- No cambies a aggregate_count si objective_agent dijo search.\n\n"
            "SECCION 2 - Data Requirements Check:\n"
            "- Verifica tablas/columnas unicamente desde schema_context.\n"
            "- Lista required_data con tabla, columnas y razon.\n"
            "- Si falta data, can_answer=false y explica en missing_info.\n\n"
            "SECCION 3 - SQL Generation:\n"
            "- Genera una sola sentencia SELECT o WITH...SELECT, sin ';'.\n"
            "- Si intent=aggregate_count, devuelve una sola fila con alias 'total'.\n"
            "- Si intent=search, devuelve filas relevantes con columnas utiles para UI."
        )
        constraints = (
            "REGLAS DURAS:\n"
            "- Solo puedes usar objetos del schema 'jne'.\n"
            f"- Debes respetar limite maximo de filas = {safe_limit}.\n"
            "- Si filtros vienen en la request (estado/organizacion), debes aplicarlos en SQL.\n"
            "- Para filtros de texto (estado/organizacion), usa comparaciones case-insensitive (UPPER(...) o ILIKE).\n"
            "- Usa domain_guide y catalogs del contexto para entender senado/presidencial/tipos/partidos.\n"
            "- Si query menciona senado/presidencial, SQL debe incluir filtro explicito por segmento/tipo_eleccion.\n"
            "- Si query menciona partido/organizacion, SQL debe usar organizacion_politica.\n"
            "- Prioriza vistas/tablas estructuradas (ej. v_*). Usa tablas *_raw solo como ultimo recurso.\n"
            "- Si usas una tabla *_raw, explica por que en required_data.reason.\n"
            "- Solo usa intent='aggregate_count' cuando la pregunta pida cantidad/total explicito (cuantos/cantidad/numero/total/contar).\n"
            "- Si la pregunta no pide cantidad explicita, usa intent='search' y result_type='rows'.\n"
            "- Si answer_level='candidate' y result_type='rows', incluye la columna id_hoja_vida.\n"
            "- Si answer_level='organization', prioriza organizacion_politica y agregaciones por partido.\n"
            "- Si answer_level='election_segment', prioriza segmento_postulacion / tipo_eleccion.\n"
            "- Usa execution_mode='sql' por defecto.\n"
            "- Usa execution_mode='derived' cuando la respuesta requiera parseo avanzado de payload JSON no expresable de forma robusta en SQL directo.\n"
            "- Resolver derivado permitido: income_amount_ranking (ranking por monto aproximado de ingresos desde declaracion_ingresos).\n"
            "- Nunca inventes tablas, columnas ni joins no presentes.\n"
            "- Si no puedes responder con schema actual: can_answer=false y sql=null."
        )
        output_contract = (
            "FORMATO DE SALIDA (OBLIGATORIO): responde SOLO JSON valido, sin markdown, sin texto extra. "
            "Esquema exacto: "
            '{"objective":"texto","intent":"aggregate_count|search","result_type":"aggregate|rows",'
            '"answer_level":"candidate|organization|election_segment|general",'
            '"execution_mode":"sql|derived","derived_resolver":"income_amount_ranking|null",'
            '"can_answer":true,"required_data":[{"table":"t","columns":["c1"],"reason":"..." }],'
            '"missing_info":["..."],"sql":"SELECT ...","reasoning":"texto breve"}'
        )
        examples = (
            "EJEMPLOS:\n"
            "1) Pregunta: 'cuantos candidatos tienen sentencias?'\n"
            'Salida esperada (shape): {"objective":"contar candidatos con sentencias","intent":"aggregate_count",'
            '"result_type":"aggregate","answer_level":"candidate","execution_mode":"sql","derived_resolver":null,"can_answer":true,'
            '"required_data":[{"table":"v_copilot_context","columns":["sentencias_penales_count","sentencias_obligaciones_count"],'
            '"reason":"contadores de sentencias"}],'
            '"missing_info":[],"sql":"select count(*)::int as total from jne.v_copilot_context where (coalesce(sentencias_penales_count,0)+coalesce(sentencias_obligaciones_count,0))>0",'
            '"reasoning":"conteo directo sobre vista consolidada"}\n'
            "2) Pregunta: 'dame evaluacion de propuestas economicas de plan de gobierno 2027'\n"
            'Si schema no tiene tabla/campos de planes: {"objective":"evaluar propuestas economicas","intent":"search","result_type":"rows","answer_level":"general","execution_mode":"sql","derived_resolver":null,'
            '"can_answer":false,"required_data":[],"missing_info":["No existe tabla/campo de planes de gobierno en schema_context"],'
            '"sql":null,"reasoning":"sin datos estructurados suficientes"}\n'
            "3) Pregunta: 'candidatos con sentencias'\n"
            'Salida esperada (shape): {"objective":"listar candidatos con sentencias","intent":"search","result_type":"rows",'
            '"answer_level":"candidate","execution_mode":"sql","derived_resolver":null,'
            '"can_answer":true,"required_data":[{"table":"v_copilot_context","columns":["id_hoja_vida","nombre_completo","sentencias_penales_count","sentencias_obligaciones_count"],'
            '"reason":"filtrar y listar candidatos"}],'
            '"missing_info":[],"sql":"select id_hoja_vida, nombre_completo, organizacion_politica, cargo, estado, sentencias_penales_count, sentencias_obligaciones_count from jne.v_copilot_context where (coalesce(sentencias_penales_count,0)+coalesce(sentencias_obligaciones_count,0))>0 order by (coalesce(sentencias_penales_count,0)+coalesce(sentencias_obligaciones_count,0)) desc, nombre_completo asc limit 20",'
            '"reasoning":"la pregunta pide lista, no conteo"}'
            "\n4) Pregunta: 'senadores de fuerza popular'\n"
            'Salida esperada (shape): {"objective":"listar senadores de una organizacion politica","intent":"search","result_type":"rows",'
            '"answer_level":"candidate","execution_mode":"sql","derived_resolver":null,'
            '"can_answer":true,"required_data":[{"table":"v_candidatos_segmento_postulacion","columns":["id_hoja_vida","nombre_completo","organizacion_politica","segmento_postulacion"],'
            '"reason":"filtrar por segmento senado y partido"}],'
            '"missing_info":[],"sql":"select id_hoja_vida, nombre_completo, organizacion_politica, cargo, estado, segmento_postulacion from jne.v_candidatos_segmento_postulacion where segmento_postulacion = \'SENADO\' and upper(coalesce(organizacion_politica,\'\')) like \'%FUERZA POPULAR%\' order by nombre_completo asc limit 20",'
            '"reasoning":"consulta de segmento electoral y partido"}'
            "\n5) Pregunta: 'candidatos presidenciales'\n"
            'Salida esperada (shape): {"objective":"listar candidaturas presidenciales","intent":"search","result_type":"rows",'
            '"answer_level":"candidate","execution_mode":"sql","derived_resolver":null,'
            '"can_answer":true,"required_data":[{"table":"v_candidatos_segmento_postulacion","columns":["id_hoja_vida","segmento_postulacion"],'
            '"reason":"filtrar solo presidencial"}],'
            '"missing_info":[],"sql":"select id_hoja_vida, nombre_completo, organizacion_politica, cargo, estado, segmento_postulacion from jne.v_candidatos_segmento_postulacion where segmento_postulacion = \'PRESIDENCIAL\' order by nombre_completo asc limit 20",'
            '"reasoning":"filtro explicito por segmento presidencial"}'
            "\n6) Pregunta: 'que partido tiene mas candidatos con denuncias?'\n"
            'Salida esperada (shape): {"objective":"identificar partido con mas candidatos con denuncias","intent":"search","result_type":"rows",'
            '"answer_level":"organization","execution_mode":"sql","derived_resolver":null,"can_answer":true,'
            '"required_data":[{"table":"v_copilot_context","columns":["organizacion_politica","expedientes_count","anotaciones_count"],'
            '"reason":"agrupar por organizacion y calcular denuncias"}],'
            '"missing_info":[],"sql":"select organizacion_politica, count(*)::int as total_candidatos, sum(coalesce(expedientes_count,0)+coalesce(anotaciones_count,0))::int as total_denuncias from jne.v_copilot_context where coalesce(trim(organizacion_politica), \'\') <> \'\' and (coalesce(expedientes_count,0)+coalesce(anotaciones_count,0)) > 0 group by organizacion_politica order by total_denuncias desc, total_candidatos desc, organizacion_politica asc limit 20",'
            '"reasoning":"la pregunta es por partido, no por persona"}'
            "\n7) Pregunta: 'candidato con mas ingresos'\n"
            'Salida esperada (shape): {"objective":"identificar candidato con mayor ingreso declarado","intent":"search","result_type":"rows",'
            '"answer_level":"candidate","execution_mode":"derived","derived_resolver":"income_amount_ranking","can_answer":true,'
            '"required_data":[{"table":"declaracion_ingresos","columns":["id_hoja_vida","payload"],"reason":"extraer montos de ingresos desde payload"},'
            '{"table":"candidatos","columns":["id_hoja_vida","nombre_completo","organizacion_politica","cargo","estado"],"reason":"enriquecer ranking por candidato"}],'
            '"missing_info":[],"sql":null,"reasoning":"se requiere parseo de payload para monto aproximado de ingresos"}'
        )

        context = {
            "query": query,
            "objective_plan": objective_plan,
            "filters": filters,
            "conversation_history": conversation_history[-3:],
            "schema_context": schema_context,
        }
        return (
            f"{instructions}\n\n"
            f"{sections}\n\n"
            f"{constraints}\n\n"
            f"{output_contract}\n\n"
            f"{examples}\n\n"
            f"Contexto JSON:\n{json.dumps(context, ensure_ascii=True)}"
        )

    @staticmethod
    def _build_summary_prompt(
        *,
        query: str,
        rows: List[Dict[str, Any]],
        evidence: List[Dict[str, Any]],
        estado: Optional[str],
        organizacion: Optional[str],
        conversation_history: List[Dict[str, str]],
    ) -> str:
        filters: Dict[str, str] = {}
        if estado:
            filters["estado"] = estado
        if organizacion:
            filters["organizacion"] = organizacion

        compact_rows: List[Dict[str, Any]] = []
        for row_idx, row in enumerate(rows, start=1):
            dynamic_counts = {
                str(key): value
                for key, value in row.items()
                if isinstance(key, str) and key.endswith("_count")
            }
            numeric_fields = {
                str(key): value
                for key, value in row.items()
                if isinstance(key, str)
                and isinstance(value, (int, float))
                and key
                not in {"id_hoja_vida", "score", "metric_value"}
                and not key.endswith("_count")
            }
            compact_rows.append(
                {
                    "row_ref": f"ROW:{row_idx}",
                    "id_hoja_vida": row.get("id_hoja_vida"),
                    "nombre_completo": row.get("nombre_completo"),
                    "organizacion_politica": row.get("organizacion_politica"),
                    "segmento_postulacion": row.get("segmento_postulacion"),
                    "tipo_eleccion": row.get("tipo_eleccion"),
                    "cargo": row.get("cargo"),
                    "estado": row.get("estado"),
                    "score": row.get("score"),
                    "metric_value": row.get("metric_value"),
                    "counts": dynamic_counts,
                    "numeric_fields": numeric_fields,
                }
            )

        compact_evidence = [
            {
                "row_ref": item.get("row_ref"),
                "id_hoja_vida": item.get("id_hoja_vida"),
                "nombre_completo": item.get("nombre_completo"),
                "organizacion_politica": item.get("organizacion_politica"),
                "findings": item.get("findings", []),
                "sources": item.get("sources", []),
            }
            for item in evidence
        ]

        instructions = (
            "Eres un asistente ciudadano. Responde en espanol de forma breve y clara. "
            "No inventes datos. Usa solo la evidencia entregada. "
            "Si no hay resultados, dilo explicitamente y sugiere reformular la consulta. "
            "Incluye citas en cada afirmacion relevante: [ID:<id_hoja_vida>] para candidatos o [ROW:<n>] para filas agregadas. "
            "Incluye una seccion final 'Fuentes:' mencionando tablas SQL relevantes."
        )

        context = {
            "query": query,
            "filters": filters,
            "results_count": len(rows),
            "conversation_history": conversation_history,
            "candidates": compact_rows,
            "evidence": compact_evidence,
        }

        return (
            f"{instructions}\n\n"
            f"Contexto JSON:\n{json.dumps(context, ensure_ascii=True)}\n\n"
            "Entrega obligatoria: 1) resumen principal (2-5 lineas), "
            "2) puntos clave en bullets (cada bullet con al menos una cita [ID:...] o [ROW:...]), "
            "3) 'Fuentes: ...'."
        )
