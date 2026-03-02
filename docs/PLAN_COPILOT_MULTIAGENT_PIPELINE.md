# Plan - Copilot SQL Multiagente

Fecha: 2026-03-02

## Objetivo

Implementar un pipeline multiagente para `POST /api/v1/copilot/ask-ai` que:

1. identifique el objetivo real de la consulta,
2. revise datos disponibles en esquema `jne`,
3. genere SQL de solo lectura alineado al objetivo,
4. critique consistencia semantica y tecnica del SQL,
5. repare SQL cuando aplique, antes de ejecutar.

## Alcance tecnico

- Mantener compatibilidad con contrato actual del endpoint (`CopilotAskAIResponse`).
- No introducir hardcoding de metricas por consulta.
- Reforzar guardrails para evitar respuestas inconsistentes (ej. intent agregado vs filas, mismatch de entidad, SQL sin filtros obligatorios).

## Pipeline propuesto

1. `Objective Agent`
- Entrada: `query`, historial reciente.
- Salida: `objective`, `intent`, `answer_level`, `reasoning`.

2. `Schema Retrieval Agent`
- Entrada: `query`, objetivo, `schema_context`.
- Salida: `can_answer`, `required_data`, `missing_info`, `preferred_tables`, `join_hints`, `reasoning`.

3. `SQL Builder Agent`
- Entrada: `query`, objetivo, plan de schema, filtros request (`estado`, `organizacion`), `schema_context`.
- Salida: plan SQL unificado (`intent`, `result_type`, `answer_level`, `execution_mode`, `sql`, etc).

4. `SQL Critic Agent`
- Entrada: query + objetivo + schema plan + plan SQL builder.
- Salida: `approved`, `action` (`accept|repair|reject`), `issues`, `repair_instructions`, `reasoning`.

5. `SQL Repair Agent` (max 1 intento)
- Se ejecuta solo si critic devuelve `action=repair`.
- Entrada: plan builder + issues critic.
- Salida: plan SQL corregido con mismo contrato.

6. Validacion backend y ejecucion
- Reusar guardrails actuales + nuevos metadatos del pipeline.
- Si falla critic/repair o no hay respuesta confiable: fallback local existente.

## Actividades Jira

- `JNE-072`: Plan tecnico y backlog multiagente.
- `JNE-073`: Implementar Schema Retrieval Agent + normalizacion.
- `JNE-074`: Integrar SQL Critic + SQL Repair loop (1 intento).
- `JNE-075`: Actualizar prompts/documentacion multiagente.
- `JNE-076`: Observabilidad de agentes (logs por etapa y decision).

## Criterios de aceptacion

- Preguntas por persona/partido/segmento generan SQL coherente con objetivo y esquema.
- El pipeline rechaza o repara planes inconsistentes antes de ejecutar.
- Los logs permiten auditar: objetivo, tablas sugeridas, decision critic, reparacion aplicada.
- Compilacion Python exitosa en modulos modificados.

## Riesgos y mitigaciones

- Mayor latencia por agentes extra.
- Mitigacion: limite de 1 repair, prompts JSON estrictos, fallback local existente.

