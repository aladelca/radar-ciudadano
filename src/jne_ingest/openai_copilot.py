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
_VALID_CRITIC_ACTIONS = {"accept", "repair", "reject"}
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
        history = conversation_history or []

        objective_plan = self._run_objective_agent(
            query=query,
            conversation_history=history,
        )
        schema_plan = self._run_schema_retrieval_agent(
            query=query,
            objective_plan=objective_plan,
            schema_context=schema_context,
            conversation_history=history,
        )

        if not schema_plan.get("can_answer"):
            result_type = "aggregate" if str(objective_plan.get("intent")) == "aggregate_count" else "rows"
            unresolved_plan = {
                "objective": str(objective_plan.get("objective") or "").strip(),
                "intent": str(objective_plan.get("intent") or "search"),
                "result_type": result_type,
                "answer_level": str(objective_plan.get("answer_level") or "general"),
                "execution_mode": "sql",
                "derived_resolver": None,
                "can_answer": False,
                "required_data": schema_plan.get("required_data", []),
                "missing_info": schema_plan.get("missing_info", [])
                or ["Schema Retrieval Agent no encontro datos suficientes para responder."],
                "sql": None,
                "reasoning": "Schema Retrieval Agent indica falta de datos estructurados para responder.",
                "objective_agent_reasoning": str(objective_plan.get("reasoning") or "").strip(),
                "schema_agent_reasoning": str(schema_plan.get("reasoning") or "").strip(),
                "critic_decision": {"approved": False, "action": "reject", "issues": ["schema_insufficient_data"]},
            }
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.info(
                "Multiagent pipeline | query=%s can_answer=false stage=schema elapsed_ms=%.1f missing_info=%s",
                (query or "").strip()[:140],
                elapsed_ms,
                len(unresolved_plan.get("missing_info", [])),
            )
            return unresolved_plan

        sql_plan = self._run_sql_builder_agent(
            query=query,
            objective_plan=objective_plan,
            schema_plan=schema_plan,
            schema_context=schema_context,
            limit=limit,
            estado=estado,
            organizacion=organizacion,
            conversation_history=history,
        )
        sql_plan = self._align_plan_with_objective(sql_plan, objective_plan, query=query)

        critic = self._run_sql_critic_agent(
            query=query,
            objective_plan=objective_plan,
            schema_plan=schema_plan,
            sql_plan=sql_plan,
            conversation_history=history,
        )

        repair_used = False
        if not critic.get("approved"):
            action = str(critic.get("action") or "")
            if action == "repair":
                repair_used = True
                sql_plan = self._run_sql_repair_agent(
                    query=query,
                    objective_plan=objective_plan,
                    schema_plan=schema_plan,
                    candidate_sql_plan=sql_plan,
                    critic=critic,
                    limit=limit,
                    estado=estado,
                    organizacion=organizacion,
                    conversation_history=history,
                )
                sql_plan = self._align_plan_with_objective(sql_plan, objective_plan, query=query)
                critic = self._run_sql_critic_agent(
                    query=query,
                    objective_plan=objective_plan,
                    schema_plan=schema_plan,
                    sql_plan=sql_plan,
                    conversation_history=history,
                )
                if not critic.get("approved"):
                    raise OpenAICopilotError(
                        "SQL Critic rechazo plan incluso despues de reparacion."
                    )
            else:
                issues = critic.get("issues") or []
                issue_text = "; ".join([str(item) for item in issues[:3]]) if isinstance(issues, list) else "n/a"
                raise OpenAICopilotError(f"SQL Critic rechazo plan: {issue_text}")

        sql_plan["objective_agent_reasoning"] = str(objective_plan.get("reasoning") or "").strip()
        sql_plan["schema_agent_reasoning"] = str(schema_plan.get("reasoning") or "").strip()
        sql_plan["critic_decision"] = {
            "approved": bool(critic.get("approved")),
            "action": str(critic.get("action") or "accept"),
            "issues": critic.get("issues") if isinstance(critic.get("issues"), list) else [],
            "repair_used": repair_used,
            "reasoning": str(critic.get("reasoning") or "").strip(),
        }
        elapsed_ms = (time.perf_counter() - start) * 1000
        sql_preview = str(sql_plan.get("sql") or "").replace("\n", " ")[:220]
        logger.info(
            "Multiagent pipeline | query=%s intent=%s result_type=%s answer_level=%s mode=%s resolver=%s can_answer=%s repair_used=%s elapsed_ms=%.1f sql=%s",
            (query or "").strip()[:140],
            sql_plan.get("intent"),
            sql_plan.get("result_type"),
            sql_plan.get("answer_level"),
            sql_plan.get("execution_mode"),
            sql_plan.get("derived_resolver"),
            sql_plan.get("can_answer"),
            repair_used,
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

    def _run_objective_agent(
        self,
        *,
        query: str,
        conversation_history: List[Dict[str, str]],
    ) -> Dict[str, Any]:
        objective_prompt = self._build_objective_agent_prompt(
            query=query,
            conversation_history=conversation_history,
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
        return objective_plan

    def _run_schema_retrieval_agent(
        self,
        *,
        query: str,
        objective_plan: Dict[str, Any],
        schema_context: Dict[str, Any],
        conversation_history: List[Dict[str, str]],
    ) -> Dict[str, Any]:
        prompt = self._build_schema_retrieval_prompt(
            query=query,
            objective_plan=objective_plan,
            schema_context=schema_context,
            conversation_history=conversation_history,
        )
        payload = self._call_responses_api(
            prompt,
            temperature=0.0,
            max_output_tokens=700,
        )
        text = self._extract_text(payload)
        if not text:
            raise OpenAICopilotError("Schema Retrieval agent devolvio respuesta vacia.")
        parsed = self._extract_json_object(text)
        schema_plan = self._normalize_schema_agent_output(parsed)
        logger.info(
            "Schema agent | query=%s can_answer=%s required_tables=%s missing_info=%s preferred_tables=%s",
            (query or "").strip()[:140],
            schema_plan.get("can_answer"),
            ",".join([str(item.get("table") or "") for item in schema_plan.get("required_data", []) if item.get("table")])
            or "-",
            len(schema_plan.get("missing_info", [])),
            ",".join(schema_plan.get("preferred_tables", [])) or "-",
        )
        return schema_plan

    def _run_sql_builder_agent(
        self,
        *,
        query: str,
        objective_plan: Dict[str, Any],
        schema_plan: Dict[str, Any],
        schema_context: Dict[str, Any],
        limit: int,
        estado: Optional[str],
        organizacion: Optional[str],
        conversation_history: List[Dict[str, str]],
    ) -> Dict[str, Any]:
        builder_prompt = self._build_sql_builder_prompt(
            query=query,
            objective_plan=objective_plan,
            schema_plan=schema_plan,
            schema_context=schema_context,
            limit=limit,
            estado=estado,
            organizacion=organizacion,
            conversation_history=conversation_history,
        )
        payload = self._call_responses_api(
            builder_prompt,
            temperature=0.0,
            max_output_tokens=1000,
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
        logger.info(
            "SQL builder agent | query=%s intent=%s result_type=%s answer_level=%s mode=%s resolver=%s can_answer=%s",
            (query or "").strip()[:140],
            sql_plan.get("intent"),
            sql_plan.get("result_type"),
            sql_plan.get("answer_level"),
            sql_plan.get("execution_mode"),
            sql_plan.get("derived_resolver"),
            sql_plan.get("can_answer"),
        )
        return sql_plan

    def _run_sql_critic_agent(
        self,
        *,
        query: str,
        objective_plan: Dict[str, Any],
        schema_plan: Dict[str, Any],
        sql_plan: Dict[str, Any],
        conversation_history: List[Dict[str, str]],
    ) -> Dict[str, Any]:
        prompt = self._build_sql_critic_prompt(
            query=query,
            objective_plan=objective_plan,
            schema_plan=schema_plan,
            sql_plan=sql_plan,
            conversation_history=conversation_history,
        )
        payload = self._call_responses_api(
            prompt,
            temperature=0.0,
            max_output_tokens=500,
        )
        text = self._extract_text(payload)
        if not text:
            raise OpenAICopilotError("SQL Critic agent devolvio respuesta vacia.")
        parsed = self._extract_json_object(text)
        critic = self._normalize_critic_agent_output(parsed)
        logger.info(
            "SQL critic agent | query=%s approved=%s action=%s issues=%s",
            (query or "").strip()[:140],
            critic.get("approved"),
            critic.get("action"),
            len(critic.get("issues", [])),
        )
        return critic

    def _run_sql_repair_agent(
        self,
        *,
        query: str,
        objective_plan: Dict[str, Any],
        schema_plan: Dict[str, Any],
        candidate_sql_plan: Dict[str, Any],
        critic: Dict[str, Any],
        limit: int,
        estado: Optional[str],
        organizacion: Optional[str],
        conversation_history: List[Dict[str, str]],
    ) -> Dict[str, Any]:
        prompt = self._build_sql_repair_prompt(
            query=query,
            objective_plan=objective_plan,
            schema_plan=schema_plan,
            candidate_sql_plan=candidate_sql_plan,
            critic=critic,
            limit=limit,
            estado=estado,
            organizacion=organizacion,
            conversation_history=conversation_history,
        )
        payload = self._call_responses_api(
            prompt,
            temperature=0.0,
            max_output_tokens=1000,
        )
        text = self._extract_text(payload)
        if not text:
            raise OpenAICopilotError("SQL Repair agent devolvio respuesta vacia.")
        parsed = self._extract_json_object(text)
        repaired = self._normalize_sql_plan_output(
            parsed,
            query=query,
            estado=estado,
            organizacion=organizacion,
        )
        logger.info(
            "SQL repair agent | query=%s intent=%s result_type=%s answer_level=%s mode=%s can_answer=%s",
            (query or "").strip()[:140],
            repaired.get("intent"),
            repaired.get("result_type"),
            repaired.get("answer_level"),
            repaired.get("execution_mode"),
            repaired.get("can_answer"),
        )
        return repaired

    def _align_plan_with_objective(
        self,
        sql_plan: Dict[str, Any],
        objective_plan: Dict[str, Any],
        *,
        query: str,
    ) -> Dict[str, Any]:
        objective_intent = str(objective_plan.get("intent") or "")
        sql_intent = str(sql_plan.get("intent") or "")
        if objective_intent and sql_intent and objective_intent != sql_intent:
            if objective_intent == "search" and sql_intent == "aggregate_count":
                raise OpenAICopilotError(
                    "Desalineacion agentes: objective=search pero sql_builder=aggregate_count."
                )
            if (
                objective_intent == "aggregate_count"
                and sql_intent == "search"
                and self._has_explicit_count_intent(query)
            ):
                raise OpenAICopilotError(
                    "Desalineacion agentes: objective=aggregate_count pero sql_builder=search."
                )
            sql_plan["intent"] = objective_intent

        objective_answer_level = str(objective_plan.get("answer_level") or "").strip()
        if objective_answer_level in _VALID_ANSWER_LEVELS and objective_answer_level != "general":
            if str(sql_plan.get("answer_level") or "").strip() != objective_answer_level:
                sql_plan["answer_level"] = objective_answer_level

        objective_text = str(objective_plan.get("objective") or "").strip()
        if objective_text:
            sql_plan["objective"] = objective_text

        intent = str(sql_plan.get("intent") or "")
        result_type = str(sql_plan.get("result_type") or "")
        can_answer = bool(sql_plan.get("can_answer"))
        if can_answer and intent == "aggregate_count" and result_type != "aggregate":
            raise OpenAICopilotError("Plan inconsistente: intent=aggregate_count requiere result_type=aggregate.")
        if can_answer and intent == "search" and result_type == "aggregate" and not self._has_explicit_count_intent(query):
            raise OpenAICopilotError("Plan inconsistente: consulta de busqueda no debe devolver agregado.")
        return sql_plan

    def _normalize_schema_agent_output(self, parsed: Dict[str, Any]) -> Dict[str, Any]:
        required_keys = {"can_answer", "required_data", "missing_info", "reasoning"}
        missing_keys = sorted(required_keys.difference(set(parsed.keys())))
        if missing_keys:
            raise OpenAICopilotError(
                "Schema Retrieval agent incompleto. Faltan campos: " + ", ".join(missing_keys)
            )

        can_answer = self._parse_bool(parsed.get("can_answer"))
        required_data = self._normalize_required_data(parsed.get("required_data"))
        missing_info = self._normalize_missing_info(parsed.get("missing_info"))
        reasoning = str(parsed.get("reasoning") or "").strip()

        preferred_tables: List[str] = []
        preferred_raw = parsed.get("preferred_tables")
        if isinstance(preferred_raw, list):
            preferred_tables = [str(item).strip() for item in preferred_raw if str(item).strip()]

        join_hints: List[str] = []
        join_raw = parsed.get("join_hints")
        if isinstance(join_raw, list):
            join_hints = [str(item).strip() for item in join_raw if str(item).strip()]

        if not can_answer and not missing_info:
            missing_info = ["No hay informacion suficiente en schema para responder la consulta."]

        return {
            "can_answer": can_answer,
            "required_data": required_data,
            "missing_info": missing_info,
            "preferred_tables": preferred_tables[:8],
            "join_hints": join_hints[:12],
            "reasoning": reasoning,
        }

    def _normalize_critic_agent_output(self, parsed: Dict[str, Any]) -> Dict[str, Any]:
        required_keys = {"approved", "action", "issues", "repair_instructions", "reasoning"}
        missing_keys = sorted(required_keys.difference(set(parsed.keys())))
        if missing_keys:
            raise OpenAICopilotError(
                "SQL Critic agent incompleto. Faltan campos: " + ", ".join(missing_keys)
            )

        approved = self._parse_bool(parsed.get("approved"))
        action_raw = str(parsed.get("action") or "").strip().lower()
        action = action_raw if action_raw in _VALID_CRITIC_ACTIONS else ("accept" if approved else "repair")

        issues_raw = parsed.get("issues")
        issues = []
        if isinstance(issues_raw, list):
            issues = [str(item).strip() for item in issues_raw if str(item).strip()]
        repair_instructions = str(parsed.get("repair_instructions") or "").strip()
        reasoning = str(parsed.get("reasoning") or "").strip()

        if approved:
            action = "accept"
        elif action == "accept":
            action = "repair"
        if not approved and not issues:
            issues = ["critic_reject_without_details"]

        return {
            "approved": approved,
            "action": action,
            "issues": issues[:12],
            "repair_instructions": repair_instructions,
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
    def _build_schema_retrieval_prompt(
        *,
        query: str,
        objective_plan: Dict[str, Any],
        schema_context: Dict[str, Any],
        conversation_history: List[Dict[str, str]],
    ) -> str:
        instructions = (
            "You are Schema Retrieval Agent. "
            "Your only job is to inspect the provided schema_context and decide what data is needed."
        )
        rules = (
            "Rules:\n"
            "- Use only tables/columns present in schema_context.\n"
            "- Do not generate SQL.\n"
            "- can_answer=true only if required tables/columns exist to answer objective.\n"
            "- Prefer consolidated views (v_*) before raw tables.\n"
            "- If objective is candidate-level, include candidate identity columns.\n"
            "- If objective is organization-level, include organizacion_politica and groupable fields.\n"
            "- For senado/presidencial context, include segment/election fields.\n"
            "- Return JSON only."
        )
        output_contract = (
            "Output JSON schema: "
            '{"can_answer":true,"required_data":[{"table":"t","columns":["c1"],"reason":"..." }],'
            '"missing_info":[],"preferred_tables":["t1"],"join_hints":["..."],"reasoning":"short"}'
        )
        context = {
            "query": query,
            "objective_plan": objective_plan,
            "conversation_history": conversation_history[-3:],
            "schema_context": schema_context,
        }
        return (
            f"{instructions}\n\n"
            f"{rules}\n\n"
            f"{output_contract}\n\n"
            f"Context JSON:\n{json.dumps(context, ensure_ascii=True)}"
        )

    @staticmethod
    def _build_sql_critic_prompt(
        *,
        query: str,
        objective_plan: Dict[str, Any],
        schema_plan: Dict[str, Any],
        sql_plan: Dict[str, Any],
        conversation_history: List[Dict[str, str]],
    ) -> str:
        instructions = (
            "You are SQL Critic Agent. "
            "Your job is to validate semantic and technical consistency of the SQL plan."
        )
        rules = (
            "Rules:\n"
            "- Validate alignment with objective_plan (intent, answer_level, entity focus).\n"
            "- Validate that required_data/schema_plan are respected.\n"
            "- Reject invented tables/columns or mismatched aggregation semantics.\n"
            "- If fixable, choose action='repair' and provide precise repair_instructions.\n"
            "- If severe/unanswerable, choose action='reject'.\n"
            "- If valid, approved=true and action='accept'.\n"
            "- Return JSON only."
        )
        output_contract = (
            "Output JSON schema: "
            '{"approved":true,"action":"accept|repair|reject","issues":["..."],'
            '"repair_instructions":"...","reasoning":"short"}'
        )
        context = {
            "query": query,
            "objective_plan": objective_plan,
            "schema_plan": schema_plan,
            "sql_plan": sql_plan,
            "conversation_history": conversation_history[-3:],
        }
        return (
            f"{instructions}\n\n"
            f"{rules}\n\n"
            f"{output_contract}\n\n"
            f"Context JSON:\n{json.dumps(context, ensure_ascii=True)}"
        )

    @staticmethod
    def _build_sql_repair_prompt(
        *,
        query: str,
        objective_plan: Dict[str, Any],
        schema_plan: Dict[str, Any],
        candidate_sql_plan: Dict[str, Any],
        critic: Dict[str, Any],
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
            "You are SQL Repair Agent. "
            "Repair the candidate SQL plan according to critic feedback."
        )
        rules = (
            "Rules:\n"
            "- Keep objective, intent and answer_level aligned with objective_plan.\n"
            "- Use only schema_context-derived tables from schema_plan/required_data.\n"
            "- Apply request filters if present.\n"
            "- Produce one final corrected plan in the exact planner schema.\n"
            "- Respect max row limit.\n"
            "- Return JSON only."
        )
        output_contract = (
            "Output JSON schema: "
            '{"objective":"texto","intent":"aggregate_count|search","result_type":"aggregate|rows",'
            '"answer_level":"candidate|organization|election_segment|general",'
            '"execution_mode":"sql|derived","derived_resolver":"income_amount_ranking|null",'
            '"can_answer":true,"required_data":[{"table":"t","columns":["c1"],"reason":"..." }],'
            '"missing_info":["..."],"sql":"SELECT ...","reasoning":"texto breve"}'
        )
        context = {
            "query": query,
            "objective_plan": objective_plan,
            "schema_plan": schema_plan,
            "candidate_sql_plan": candidate_sql_plan,
            "critic": critic,
            "filters": filters,
            "limit": safe_limit,
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
        schema_plan: Dict[str, Any],
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
            "SECCION 2 - Schema Plan Adherence:\n"
            "- Usa schema_plan.required_data como base principal de tablas/columnas.\n"
            "- No salgas de schema_context.\n"
            "- Si agregas tabla extra, justificala en required_data.reason.\n\n"
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
            "- Si la pregunta es sobre una persona especifica (ej. 'dame informacion acerca de <nombre>'), prioriza buscar por nombre_completo con tokens y devuelve perfil de candidato.\n"
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
            "\n8) Pregunta: 'dame informacion acerca de George Forsyth'\n"
            'Salida esperada (shape): {"objective":"mostrar perfil del candidato solicitado por nombre","intent":"search","result_type":"rows",'
            '"answer_level":"candidate","execution_mode":"sql","derived_resolver":null,"can_answer":true,'
            '"required_data":[{"table":"v_copilot_context","columns":["id_hoja_vida","nombre_completo","organizacion_politica","cargo","estado","sentencias_penales_count","sentencias_obligaciones_count","expedientes_count","ingresos_count"],"reason":"perfil y contadores estructurados del candidato"}],'
            '"missing_info":[],"sql":"select id_hoja_vida, nombre_completo, organizacion_politica, cargo, estado, sentencias_penales_count, sentencias_obligaciones_count, expedientes_count, ingresos_count from jne.v_copilot_context where upper(coalesce(nombre_completo, \'\')) like \'%GEORGE%\' and upper(coalesce(nombre_completo, \'\')) like \'%FORSYTH%\' order by nombre_completo asc limit 20",'
            '"reasoning":"consulta por identidad de persona; se filtra por tokens del nombre"}'
        )

        context = {
            "query": query,
            "objective_plan": objective_plan,
            "schema_plan": schema_plan,
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
