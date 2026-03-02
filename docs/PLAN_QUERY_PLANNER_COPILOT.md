# Plan Query Planner Copilot (Tema -> SQL)

Fecha: 2026-02-28

## Problema

El copilot respondia consultas con enfoque principal de ranking textual. Para preguntas de negocio
("cuantos candidatos con X"), esto podia producir resultados inconsistentes aunque la data exista.

## Objetivo

Implementar una capa de planificacion de consulta que:

1. identifique intencion (conteo agregado vs busqueda),
2. identifique tema/metrica de forma dinamica desde catalogo SQL,
3. ejecute consulta SQL correcta en base al plan,
4. separe prompts de IA en dos fases: planner (proposito) y narracion.

## Alcance tecnico

- Nuevo modulo `query_planner.py` con:
  - normalizacion de consulta,
  - deteccion de intencion de conteo.
- Catalogo dinamico de metricas desde DB:
  - introspeccion de `jne.v_copilot_context` (`*_count`),
  - metricas de familia autogeneradas por tokens recurrentes,
  - inferencia de metrica por overlap de tokens/aliases.
- Integracion en `POST /api/v1/copilot/ask` y `POST /api/v1/copilot/ask-ai`.
- Repositorio SQL con metodos agregados reutilizables (`get_aggregate_metrics`, `get_metric_overview`).
- Planner IA en `ask-ai` con prompt dedicado para:
  - clasificar `intent` (`aggregate_count` vs `search`),
  - elegir metrica valida del catalogo.
- Prompt de respuesta IA separado para narracion final con evidencia SQL.
- Mantener flujo actual para consultas no agregadas (search ranking).

## Decision sobre LangChain

No usar LangChain en esta fase.

Razon:

- El problema actual se resuelve mejor con planner deterministico y contratos de datos simples.
- Menor complejidad operativa y menos puntos de falla.
- Permite evaluar precision de routing antes de introducir framework de orquestacion.

Reevaluar LangChain/LangGraph cuando se necesite:

- multi-hop con herramientas heterogeneas,
- memoria persistente avanzada,
- trazabilidad de agentes multi-etapa.

## Actividades (Jira)

| Ticket | Actividad | Estado |
|---|---|---|
| JNE-058 | Plan tecnico del Query Planner y decision de arquitectura | DONE |
| JNE-059 | Implementar modulo `query_planner.py` (tema/intencion) | DONE |
| JNE-060 | Integrar planner en endpoints `ask` y `ask-ai` + executor SQL por plan | DONE |
| JNE-061 | Reemplazar counters hardcodeados por catalogo dinamico de metricas DB | DONE |
| JNE-062 | Evaluacion y decision documentada sobre LangChain | DONE |
| JNE-063 | Planner IA con prompt separado para proposito/intent | DONE |
| JNE-064 | Prompt de respuesta separado del planner + fallback SQL textual | DONE |

## Criterios de aceptacion

- preguntas de conteo no mapeables a metrica exacta caen a conteo SQL por coincidencia textual.
- "cuantos candidatos con ingresos" responde conteo SQL coherente.
- no existe lista fija de counters en `api_app`; la resolucion usa catalogo generado desde DB.
- consultas no agregadas como "candidatos con denuncias" siguen devolviendo ranking.
- sin `OPENAI_API_KEY`, el sistema responde por fallback SQL correctamente.

## Extension 2026-02-28 (Flexibilidad por entidad)

Para cubrir consultas por partido/segmento (no solo por candidato), se agrega:

- contrato `answer_level` en agentes IA (`candidate|organization|election_segment|general`),
- validacion SQL condicional: `id_hoja_vida` solo obligatorio para nivel `candidate`,
- soporte de evidencia/citas por fila agregada con formato `[ROW:n]`,
- heuristica de ranking (`top/mas/mayor`) para evitar forzar intent de conteo puro.
