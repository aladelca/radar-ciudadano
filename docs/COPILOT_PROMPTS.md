# Copilot Prompts - Pipeline Multiagente (SQL Planner)

Fecha: 2026-03-02

## Objetivo

Definir prompts separados para que el copilot:

1. entienda la intencion del usuario,
2. identifique data necesaria en el schema real,
3. construya SQL seguro y alineado al objetivo,
4. critique consistencia semantica/tecnica,
5. repare el SQL si aplica (1 intento maximo),
6. narre resultado final con evidencia.

## Arquitectura en runtime

- `Objective Agent`
- `Schema Retrieval Agent`
- `SQL Builder Agent`
- `SQL Critic Agent`
- `SQL Repair Agent` (solo si critic devuelve `repair`)
- `Narration Agent`

## Prompt 1: Objective Agent

Responsabilidad:

- detectar `objective`,
- clasificar `intent` (`aggregate_count|search`),
- definir `answer_level` (`candidate|organization|election_segment|general`).

Salida JSON:

```json
{
  "objective": "texto",
  "intent": "aggregate_count|search",
  "answer_level": "candidate|organization|election_segment|general",
  "reasoning": "breve"
}
```

## Prompt 2: Schema Retrieval Agent

Responsabilidad:

- revisar `schema_context` real,
- seleccionar tablas/columnas necesarias,
- declarar faltantes cuando no exista data suficiente.

Salida JSON:

```json
{
  "can_answer": true,
  "required_data": [
    {
      "table": "v_copilot_context",
      "columns": ["id_hoja_vida", "nombre_completo"],
      "reason": "perfil de candidato"
    }
  ],
  "missing_info": [],
  "preferred_tables": ["v_copilot_context"],
  "join_hints": ["id_hoja_vida"],
  "reasoning": "breve"
}
```

## Prompt 3: SQL Builder Agent

Responsabilidad:

- generar plan SQL ejecutable de solo lectura,
- respetar `objective_plan` + `schema_plan`,
- mantener filtros request (`estado`, `organizacion`),
- elegir `execution_mode` (`sql|derived`).

Salida JSON (contrato unificado):

```json
{
  "objective": "texto",
  "intent": "aggregate_count|search",
  "result_type": "aggregate|rows",
  "answer_level": "candidate|organization|election_segment|general",
  "execution_mode": "sql|derived",
  "derived_resolver": "income_amount_ranking|null",
  "can_answer": true,
  "required_data": [
    {
      "table": "t",
      "columns": ["c1"],
      "reason": "..."
    }
  ],
  "missing_info": [],
  "sql": "SELECT ...",
  "reasoning": "texto breve"
}
```

## Prompt 4: SQL Critic Agent

Responsabilidad:

- validar consistencia entre objetivo, schema y SQL propuesto,
- detectar errores semanticos (entidad, agregacion, filtros, joins),
- decidir `accept|repair|reject`.

Salida JSON:

```json
{
  "approved": true,
  "action": "accept|repair|reject",
  "issues": ["..."],
  "repair_instructions": "...",
  "reasoning": "breve"
}
```

## Prompt 5: SQL Repair Agent

Responsabilidad:

- corregir plan SQL usando feedback del critic,
- mantener contrato del SQL Builder,
- no inventar tablas/columnas.

Salida JSON: mismo contrato del SQL Builder.

## Prompt 6: Narration Agent

Responsabilidad:

- responder en espanol claro, sin inventar datos,
- citar evidencia por entidad (`[ID:...]` / `[ROW:...]`),
- terminar con `Fuentes:`.

## Guardrails backend

- JSON estricto por agente.
- SQL readonly (`SELECT`/`WITH`), sin escritura/DDL.
- Alineacion obligatoria `intent/result_type`.
- Filtros request (`estado`/`organizacion`) obligatorios cuando se envian.
- Para `answer_level=candidate` + `rows`: `id_hoja_vida` requerido.
- Critic con reparacion maxima de 1 intento.
- Fallback local si pipeline no confiable o falla validacion.

