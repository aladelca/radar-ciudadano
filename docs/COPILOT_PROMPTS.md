# Copilot Prompts (Planner + SQL + Narracion)

Fecha: 2026-02-28

## Objetivo

Definir prompts separados para que el copilot:

1. identifique el objetivo de la pregunta del usuario,
2. valide la informacion necesaria en la base de datos (`schema jne`),
3. genere SQL de solo lectura para responder,
4. narre el resultado sin inventar datos.

Arquitectura en runtime:

- `Objective Agent`: interpreta que quiere saber el usuario.
- `SQL Builder Agent`: construye query y chequea data requerida en schema.
- `Narration Agent`: redacta respuesta final sobre evidencia SQL.

## Prompt 1: Objective Detection

Responsabilidad:

- clasificar la intencion (`aggregate_count` o `search`),
- resumir el objetivo de negocio de la pregunta.
- definir `answer_level` segun entidad principal de respuesta:
  - `candidate` (persona),
  - `organization` (partido/organizacion),
  - `election_segment` (presidencial/senado),
  - `general` (mixto/ambiguo).

Salida requerida (JSON):

```json
{
  "objective": "texto",
  "intent": "aggregate_count|search",
  "answer_level": "candidate|organization|election_segment|general"
}
```

## Prompt 2: Data Requirements Check

Responsabilidad:

- revisar `schema_context` (tablas/columnas reales en `jne`),
- listar tablas/columnas necesarias para responder,
- indicar faltantes si no existe data suficiente.

Salida requerida (JSON):

```json
{
  "can_answer": true,
  "required_data": [
    {
      "table": "jne.alguna_tabla",
      "columns": ["columna_1", "columna_2"],
      "reason": "por que se necesita"
    }
  ],
  "missing_info": []
}
```

## Prompt 3: SQL Generation

Responsabilidad:

- producir una sola consulta SQL (`SELECT` o `WITH ... SELECT`) compatible con Postgres,
- no inventar tablas/columnas fuera del `schema_context`,
- en conteos usar alias sugerido `total`.
- aplicar filtros `estado/organizacion` cuando se reciban en la request.
- no usar `aggregate_count` si la pregunta no solicita cantidad explícita.
- para `answer_level=candidate` y `result_type=rows`, incluir `id_hoja_vida`.
- para `answer_level=organization`, priorizar agrupaciones por `organizacion_politica`.
- para `answer_level=election_segment`, priorizar `segmento_postulacion`/`tipo_eleccion`.
- elegir `execution_mode`:
  - `sql`: cuando basta una consulta SQL de solo lectura,
  - `derived`: cuando se requiere parseo de payload JSON (ej. ranking de monto de ingresos).
- resolvers derivados permitidos:
  - `income_amount_ranking`.

Salida requerida (JSON):

```json
{
  "answer_level": "candidate|organization|election_segment|general",
  "execution_mode": "sql|derived",
  "derived_resolver": "income_amount_ranking|null",
  "result_type": "aggregate|rows",
  "sql": "SELECT ...",
  "reasoning": "justificacion breve"
}
```

## Prompt 4: Narracion Final

Responsabilidad:

- resumir resultado SQL de forma clara,
- citar evidencia por entidad:
  - `[ID:...]` para candidatos,
  - `[ROW:...]` para filas agregadas (ej. por partido),
- incluir seccion final `Fuentes:`.

## Contrato Unificado Implementado

En runtime, el planner SQL devuelve un JSON unificado:

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

Si `can_answer=false` o no hay `sql`, el backend usa fallback SQL local.

## Guardrails Implementados en Backend

- Validacion de campos obligatorios del JSON del planner.
- Coherencia obligatoria:
  - `can_answer=true` requiere `sql` no nulo.
  - `can_answer=false` fuerza `sql=null`.
- Validacion SQL basica:
  - solo `SELECT`/`WITH`,
  - una sola sentencia.
- Validacion de filtros:
  - si llega `estado`, el SQL debe incluir condicion de `estado`,
  - si llega `organizacion`, el SQL debe incluir condicion de `organizacion`.
- Validacion de nivel de respuesta:
  - si `answer_level=candidate` y `result_type=rows`, el SQL debe exponer `id_hoja_vida`.
- Ejecucion derivada:
  - si `execution_mode=derived`, backend ejecuta resolver registrado en vez de SQL directo.
