# API Backend - Candidatos JNE 2026

## Objetivo

Exponer una API REST local para que frontend/copilot consulten la data ya ingerida en Supabase/Postgres.

## Arranque

1. Instalar dependencias:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Levantar base local y aplicar migraciones:

```bash
supabase start
supabase db reset
```

3. Ejecutar servidor:

```bash
source .venv/bin/activate
python scripts/run_api.py --host 127.0.0.1 --port 8010 --log-level INFO
```

Con `--log-level INFO` se registran eventos de servicio:

- inicio/cierre API,
- `copilot.ask` y `copilot.ask-ai` (start/plan/done),
- resultado de `Objective Agent` y `SQL Builder Agent`,
- SQL readonly ejecutado y conteo de filas,
- motivos de fallback.

4. Documentación interactiva:
- `http://127.0.0.1:8010/docs`

## Controles de beta publica

El backend soporta controles por entorno para exposicion publica:

- API keys por header `X-API-Key`,
- enforcement separado para lectura y para IA,
- rate limiting independiente para lectura y `ask-ai`,
- CORS por origen.

Variables relevantes en `.env`:

- `API_CORS_ALLOW_ORIGINS`
- `BETA_API_KEYS`
- `BETA_AI_API_KEYS`
- `BETA_ENFORCE_API_KEY_READ`
- `BETA_ENFORCE_API_KEY_AI`
- `BETA_RATE_LIMIT_READ_PER_MINUTE`
- `BETA_RATE_LIMIT_AI_PER_MINUTE`
- `BETA_RATE_LIMIT_WINDOW_SECONDS`
- `READONLY_SQL_TIMEOUT_MS`
- `READONLY_SQL_MAX_ROWS`

Smoke test de beta:

```bash
source .venv/bin/activate
python scripts/smoke_api_beta.py --base-url "http://127.0.0.1:8010" --api-key "<API_KEY_BETA>" --include-ai
```

## Endpoints

### `GET /` y `GET /ui`

Sirve una UI web local con:

- cards de resumen de cobertura,
- graficos de distribucion (ingresos, denuncias, universidades),
- panel de preguntas para `copilot/ask` y `copilot/ask-ai` (selector SQL/IA).

### `GET /health`

Respuesta de salud general y conectividad a BD.

Incluye metadata operativa adicional:

- `dashboard_cache_ttl_seconds`: TTL de cache del dashboard.
- `copilot_session_mode`: modo de sesion del copilot (ids firmados + store in-memory).

### `GET /api/v1/dashboard/insights`

Entrega los agregados para el dashboard web:

- `totals`: conteos globales,
- `charts.ingresos`: distribucion de montos aproximados de ingresos por candidato,
- `charts.denuncias`: distribucion de denuncias aproximadas,
- `charts.universidades`: top de universidades detectadas en hoja de vida.

Parametros:

- `top_universidades` (opcional, default `12`, min `5`, max `30`).

Ejemplo:

```bash
curl "http://127.0.0.1:8010/api/v1/dashboard/insights?top_universidades=12"
```

### `GET /api/v1/candidatos/search`

Parámetros:

- `q` (opcional): texto libre.
- `limit` (opcional, default `20`, max `100`).
- `estado` (opcional).
- `organizacion` (opcional).

Ejemplo:

```bash
curl "http://127.0.0.1:8010/api/v1/candidatos/search?q=acuna&limit=5"
```

### `GET /api/v1/candidatos/{id_hoja_vida}`

Devuelve detalle completo de candidato:

- perfil base,
- sentencias,
- patrimonio (ingresos y bienes),
- anotaciones y expedientes,
- bloque `instagram` (cuentas vinculadas + snapshots latest si existen).

Parametro opcional:

- `include_raw` (default `false`): incluye payloads raw grandes:
  - `hoja_vida_raw`,
  - `hoja_vida_secciones_raw`,
  - `anotaciones_raw`,
  - `expedientes_raw`.

Ejemplo:

```bash
curl "http://127.0.0.1:8010/api/v1/candidatos/245682?include_raw=false"
```

### `GET /api/v1/candidatos/{id_hoja_vida}/instagram`

Devuelve solo la capa de Instagram del candidato:

- `accounts`: cuentas vinculadas en `jne.candidato_redes_sociales`,
- `latest_profiles`: ultimo snapshot por username (`jne.v_instagram_profile_latest`),
- `latest_media`: ultimo snapshot por media (`jne.v_instagram_media_latest`).

Parámetros:

- `media_limit` (opcional, default `25`, max `100`).

Ejemplo:

```bash
curl "http://127.0.0.1:8010/api/v1/candidatos/245682/instagram?media_limit=25"
```

### `POST /api/v1/copilot/ask`

Entrada JSON:

- `query` (obligatorio),
- `limit` (opcional),
- `estado` (opcional),
- `organizacion` (opcional).

Salida:

- `summary`: texto explicable,
- `candidates`: ranking de resultados,
- `evidence`: hallazgos por candidato y tablas fuente.

Ejemplo:

```bash
curl -X POST "http://127.0.0.1:8010/api/v1/copilot/ask" \
  -H "content-type: application/json" \
  -d '{"query":"denuncias patrimonio acuna","limit":5}'
```

### `POST /api/v1/copilot/ask-ai`

Mismo contrato de entrada que `copilot/ask`, pero intenta generar respuesta con OpenAI
usando evidencia SQL recuperada en backend.

Flujo principal en modo IA:

1. Planner IA identifica objetivo de la pregunta,
2. Planner IA valida informacion requerida contra schema DB (`jne`),
3. Planner IA genera SQL de solo lectura,
4. Backend ejecuta SQL y luego genera narracion IA sobre el resultado.

Si el planner IA no puede responder con SQL valido, el backend cae a planner SQL local.

Referencia de prompts: `docs/COPILOT_PROMPTS.md`.

Requiere:

- `OPENAI_API_KEY` en entorno.
- API key valida si `BETA_API_KEYS` + `BETA_ENFORCE_API_KEY_AI=true`.

Entrada adicional opcional:

- `session_id`: identificador para mantener memoria conversacional entre turnos
  (devuelto por el backend; formato firmado).

Salida adicional:

- `mode`: `ai` o `fallback`,
- `model`: modelo configurado en OpenAI (cuando aplica),
- `warning`: mensaje de degradacion/falla cuando cae a fallback,
- `session_id`: id de sesion retornado/continuado,
- `history_used`: cantidad de turnos previos usados por el prompt,
- `citations`: lista de referencias por candidato (`[ID:...]`).

Ejemplo:

```bash
curl -X POST "http://127.0.0.1:8010/api/v1/copilot/ask-ai" \
  -H "X-API-Key: <API_KEY_BETA>" \
  -H "content-type: application/json" \
  -d '{"query":"candidatos con sentencias y expedientes","limit":5}'
```
