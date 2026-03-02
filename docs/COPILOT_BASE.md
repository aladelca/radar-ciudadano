# Base Tecnica Copilot Ciudadano

## Objetivo

Habilitar una primera capa de "copilot" que navegue la informacion de candidatos ya ingerida en Supabase, priorizando:

- denuncias/sentencias declaradas,
- patrimonio (ingresos y bienes),
- expedientes y anotaciones.

## Componentes implementados

1. Vista consolidada `jne.v_copilot_context`
- Une `jne.candidatos` con tablas detalle de hoja de vida.
- Calcula contadores por categoria (sentencias, bienes, expedientes, anotaciones).
- Genera `context_text` para busqueda textual inicial.

2. Funcion `jne.search_candidatos_copilot(text, int)`
- Recibe texto de consulta y limite.
- Asigna score simple por coincidencia en:
  - nombre,
  - organizacion politica,
  - cargo,
  - contexto completo.
- Devuelve ranking listo para capa API/chat.

3. CLI `scripts/copilot_query.py`
- Permite ejecutar consultas desde terminal.
- Sirve como contrato inicial para futura API REST/LLM.

4. API `POST /api/v1/copilot/ask`
- Expone consulta textual para frontend web.
- Devuelve `summary`, `candidates` y `evidence`.

5. API `POST /api/v1/copilot/ask-ai`
- Reusa retrieval SQL y evidencia, luego resume con OpenAI (`/v1/responses`).
- Si falta `OPENAI_API_KEY` o hay error upstream, degrada a `mode=fallback`.
- Soporta `session_id` para mantener contexto conversacional por turnos.
- Fuerza citas por candidato con formato `[ID:<id_hoja_vida>]`.

6. Dashboard web inicial
- UI en `GET /` con cards y graficos.
- Endpoint `GET /api/v1/dashboard/insights` para distribuciones de ingresos, denuncias y universidades.

## Flujo recomendado para el copiloto final

1. Usuario pregunta en lenguaje natural.
2. Capa backend transforma la consulta y llama `jne.search_candidatos_copilot`.
3. Se recuperan candidatos top-k.
4. Se expanden detalles desde `hoja_vida_raw` y tablas detalle.
5. Se construye respuesta explicable citando campos estructurados.

## Siguientes mejoras (fase 2)

- Mejorar ranking con `pg_trgm` o embeddings.
- Crear resumen por candidato con campos normalizados de patrimonio.
- Agregar trazabilidad por respuesta (fuente, fecha de ingesta, run_id).
- Integrar guardrails para evitar alucinaciones y respuestas sin evidencia.
- Incluir capa de planes de gobierno (ingesta de documentos + indexacion para preguntas tematicas).
