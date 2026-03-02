# Congreso Votaciones - Ingesta JNE 2026

Proyecto base para extraer datos de candidatos desde la Plataforma Electoral del JNE y cargarlos a Supabase local (Postgres).

## Estructura

- `docs/PLAN_INGESTA_JNE_2026.md`: plan detallado.
- `docs/PLAN_INSTAGRAM_CANDIDATOS.md`: plan por fases para integracion Instagram.
- `docs/PLAN_BETA_PUBLICA.md`: plan detallado para lanzamiento de beta publica.
- `docs/PLAYBOOK_BETA_SEGURIDAD_DDOS_OPENAI.md`: defensa DDoS y control de costo OpenAI para beta.
- `docs/JIRA_BOARD.md`: backlog tipo Jira y estado.
- `src/jne_ingest/`: cliente JNE + pipeline + repositorio Postgres.
- `scripts/run_ingest.py`: CLI de ingesta.
- `scripts/run_plan_gobierno_ingest.py`: CLI independiente para planes de gobierno.
- `scripts/run_api.py`: servidor API local (FastAPI).
- `supabase/migrations/`: migracion SQL inicial.

## Requisitos

- Python 3.11+
- Supabase CLI (para entorno local)
- Postgres local de Supabase levantado
- Chromium para Playwright (solo si usas token provider automatico)

## Setup rapido

1. Crear entorno e instalar dependencias:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Si vas a usar `--token-provider playwright`, instala navegador:

```bash
python -m playwright install chromium
```

2. Copiar variables de entorno:

```bash
cp .env.example .env
```

3. Levantar Supabase local y aplicar migracion:

```bash
supabase start
./scripts/reset_local.sh
```

`scripts/reset_local.sh` ejecuta `supabase db reset --debug` y corrige automaticamente el error conocido `storage/v1/bucket -> 502` reiniciando Kong.

Si no tienes `supabase` CLI, puedes aplicar migraciones por DSN directo:

```bash
source .venv/bin/activate
python scripts/apply_migrations.py --db-dsn "$DATABASE_DSN"
```

## Ejecucion de ingesta

### Modo normal (persistiendo en Postgres)

```bash
source .venv/bin/activate
python scripts/run_ingest.py --process-id 124 --tipo-eleccion-nombre PRESIDENCIAL
```

Notas:

- `IdTipoEleccion=1` corresponde a `PRESIDENCIAL` en EG 2026.
- `IdOrganizacionPolitica=0` (TODOS) se usa por defecto en el pipeline.
- La búsqueda usa primero `POST /api/v1/candidato/avanzadaexporta` (sin recaptcha) y solo cae al flujo con captcha si ese endpoint falla.
- Si `avanzadaexporta` trae filas repetidas de origen, la persistencia deduplica por `id_hoja_vida` (upsert idempotente).
- Puedes usar token fijo (`JNE_RECAPTCHA_TOKEN` o `--token`) o automatico:

```bash
python scripts/run_ingest.py \
  --process-id 124 \
  --tipo-eleccion-nombre PRESIDENCIAL \
  --token-provider playwright
```

Ingesta de Senado (EG 2026):

```bash
# SENADORES DISTRITO UNICO
python scripts/run_ingest.py \
  --process-id 124 \
  --tipo-eleccion-id 20 \
  --search-mode api \
  --token-provider env \
  --max-pages 1

# SENADORES DISTRITO MULTIPLE
python scripts/run_ingest.py \
  --process-id 124 \
  --tipo-eleccion-id 21 \
  --search-mode api \
  --token-provider env \
  --max-pages 1
```

Ingesta de Diputados (EG 2026, detalle completo):

```bash
PYTHONPATH=src python scripts/run_ingest.py \
  --process-id 124 \
  --tipo-eleccion-id 15 \
  --search-mode api \
  --token-provider env \
  --max-pages 1
```

Ingesta de Parlamento Andino (EG 2026, detalle completo):

```bash
PYTHONPATH=src python scripts/run_ingest.py \
  --process-id 124 \
  --tipo-eleccion-id 3 \
  --search-mode api \
  --token-provider env \
  --max-pages 1
```

Para acelerar una carga grande, puedes particionar por `id_hoja_vida`:

```bash
# Ejemplo: 4 procesos en paralelo (remainders 0..3)
PYTHONPATH=src python scripts/run_ingest.py --process-id 124 --tipo-eleccion-id 15 --search-mode api --token-provider env --max-pages 1 --partition-mod 4 --partition-rem 0
PYTHONPATH=src python scripts/run_ingest.py --process-id 124 --tipo-eleccion-id 15 --search-mode api --token-provider env --max-pages 1 --partition-mod 4 --partition-rem 1
PYTHONPATH=src python scripts/run_ingest.py --process-id 124 --tipo-eleccion-id 15 --search-mode api --token-provider env --max-pages 1 --partition-mod 4 --partition-rem 2
PYTHONPATH=src python scripts/run_ingest.py --process-id 124 --tipo-eleccion-id 15 --search-mode api --token-provider env --max-pages 1 --partition-mod 4 --partition-rem 3
```

Si quieres forzar la búsqueda desde navegador (fallback con captcha):

```bash
python scripts/run_ingest.py \
  --process-id 124 \
  --tipo-eleccion-nombre PRESIDENCIAL \
  --search-mode browser \
  --max-pages 1
```

- Si no hay token valido, la API puede responder `CAPTCHA_REQUIRED`.
- El cliente aplica reintentos/backoff configurables (`JNE_REQUEST_RETRIES`, `JNE_BACKOFF_SECONDS`).
- Para errores captcha, el pipeline renueva token automaticamente (`JNE_CAPTCHA_RETRIES`).
- En `--token-provider playwright`, el modo por defecto es visible (`JNE_PLAYWRIGHT_HEADLESS=false`) para mejorar aceptacion de reCAPTCHA.
- `--search-mode browser` usa Playwright dentro de la página para enviar `candidato/avanzada` con cookies/sesión del navegador.
- Para mejor estabilidad anti-bot en `search-mode browser`, usa perfil persistente: `JNE_PLAYWRIGHT_USER_DATA_DIR=/tmp/jne-browser-profile`.
- Puedes subir/bajar verbosidad con `--log-level DEBUG|INFO|WARNING|ERROR` o `JNE_LOG_LEVEL`.

### Modo pruebas (sin persistencia)

```bash
source .venv/bin/activate
python scripts/run_ingest.py --dry-run --process-id 124 --tipo-eleccion-nombre PRESIDENCIAL --max-pages 1
```

## Ingesta de planes de gobierno (pipeline independiente)

Este pipeline es separado de `run_ingest.py` y reutiliza candidatos ya guardados en tu BD
(`jne.candidatos` + `jne.hoja_vida_raw`) para resolver plan por candidato sin re-ejecutar
la busqueda avanzada con recaptcha.

```bash
source .venv/bin/activate
python scripts/run_plan_gobierno_ingest.py \
  --process-id 124 \
  --tipo-eleccion-nombre PRESIDENCIAL
```

Notas:

- Persiste JSON estructurado del plan y dimensiones.
- Para PDF, guarda URL + texto extraido (no guarda binario PDF en la BD).
- Si quieres omitir extraccion de texto PDF:

```bash
python scripts/run_plan_gobierno_ingest.py \
  --process-id 124 \
  --tipo-eleccion-nombre PRESIDENCIAL \
  --skip-pdf-text
```

## Ingesta Instagram (discovery inicial)

Runner base para consultar Business Discovery y guardar snapshots:

```bash
source .venv/bin/activate
python scripts/run_instagram_discovery.py \
  --id-hoja-vida 245682 \
  --username usuario_candidato \
  --app-user-ig-id "<APP_USER_IG_ID>" \
  --access-token "<TOKEN_GRAPH_API>" \
  --media-limit 25
```

Notas:

- Requiere un `app_user_ig_id` y token de Graph API con acceso habilitado para discovery.
- Si no envias `--access-token`, usa `INSTAGRAM_GRAPH_ACCESS_TOKEN`.
- El runner persiste cuenta + snapshots en tablas `jne.candidato_redes_sociales`, `jne.instagram_profiles_snapshot`, `jne.instagram_media_snapshot`.

## Base de Copilot (MVP)

Tras correr migraciones e ingesta, puedes consultar candidatos con ranking basico:

```bash
source .venv/bin/activate
python scripts/copilot_query.py "denuncias acuna" --limit 5
```

El soporte SQL vive en:

- `supabase/migrations/202602280201_copilot_base.sql`
- vista `jne.v_copilot_context`
- funcion `jne.search_candidatos_copilot(text, int)`

Persistencia total de ingesta:

- Candidato base: `jne.candidatos` (+ `raw_payload`)
- Hoja de vida completa: `jne.hoja_vida_raw`
- Hoja de vida por secciones: `jne.hoja_vida_secciones_raw`
- Anotaciones completas: `jne.anotaciones_raw` + items en `jne.anotaciones_marginales`
- Expedientes completos: `jne.expedientes_raw` + items en `jne.expedientes_candidato`
- Catálogos: `jne.catalog_procesos_electorales`, `jne.catalog_tipos_eleccion`, `jne.catalog_organizaciones_politicas`
- Diferenciacion de postulaciones: `jne.candidatos_postulaciones`, `jne.v_candidatos_segmento_postulacion`, `jne.v_postulaciones_resumen_persona`

Persistencia de planes de gobierno (pipeline independiente):

- Runs: `jne.plan_gobierno_runs`
- Relacion candidato-plan: `jne.candidato_plan_gobierno`
- Plan normalizado: `jne.planes_gobierno`
- Dimensiones del plan: `jne.planes_gobierno_dimensiones`
- Texto PDF extraido: `jne.planes_gobierno_pdf_texto`

## API backend local

Levantar API:

```bash
source .venv/bin/activate
python scripts/run_api.py --host 127.0.0.1 --port 8010 --log-level INFO
```

Para trazabilidad detallada del copilot (agentes/SQL/fallback), usa `--log-level INFO` o `DEBUG`.

Swagger:

- `http://127.0.0.1:8010/docs`

Endpoints principales:

- `GET /health`
- `GET /` (UI web dashboard + copilot)
- `GET /api/v1/dashboard/insights?top_universidades=12`
- `GET /api/v1/candidatos/search?q=acuna&limit=5`
- `GET /api/v1/candidatos/{id_hoja_vida}?include_raw=false`
- `GET /api/v1/candidatos/{id_hoja_vida}/instagram?media_limit=25`
- `POST /api/v1/copilot/ask`
- `POST /api/v1/copilot/ask-ai` (requiere `OPENAI_API_KEY`; con fallback SQL si falta)

Referencia completa:

- `docs/API_BACKEND.md`
- `docs/RUNBOOK_OPERACION_JNE_2026.md`
- `docs/PLAN_BETA_PUBLICA.md`

## Beta publica (hardening API)

Para una beta publica, el backend soporta controles operativos por entorno:

- API keys por `X-API-Key` (`BETA_API_KEYS`).
- Enforcement separado para lectura e IA (`BETA_ENFORCE_API_KEY_READ`, `BETA_ENFORCE_API_KEY_AI`).
- Rate limit por minuto para lectura e IA.
- CORS por lista de origenes permitidos (`API_CORS_ALLOW_ORIGINS`).
- Guardrails SQL readonly para planner IA (`READONLY_SQL_TIMEOUT_MS`, `READONLY_SQL_MAX_ROWS`).

Headers utiles:

- `X-API-Key: <tu_api_key>`

Variables de entorno de referencia: ver `.env.example` y `docs/PLAN_BETA_PUBLICA.md`.

Smoke test rapido de beta:

```bash
source .venv/bin/activate
python scripts/smoke_api_beta.py --base-url "http://127.0.0.1:8010" --api-key "<API_KEY_BETA>" --include-ai
```

## Copilot IA (OpenAI)

Si quieres respuestas narrativas asistidas por modelo:

1. Configura `OPENAI_API_KEY` en `.env`.
2. (Opcional) Ajusta `OPENAI_MODEL`, `OPENAI_TIMEOUT_SECONDS` y `COPILOT_SESSION_SIGNING_KEY`.
3. Usa endpoint AI:

```bash
curl -X POST "http://127.0.0.1:8010/api/v1/copilot/ask-ai" \
  -H "content-type: application/json" \
  -d '{"query":"candidatos con sentencias y expedientes","limit":5}'
```

La respuesta incluye `mode`:

- `ai`: resumen generado por OpenAI sobre evidencia SQL recuperada.
- `fallback`: resumen deterministico SQL (sin key o error upstream).

Campos utiles en `ask-ai`:

- `session_id`: id de conversacion para mantener contexto entre preguntas.
- `history_used`: cantidad de turnos recientes usados para el prompt.
- `citations`: referencias sugeridas por candidato en formato `[ID:<id_hoja_vida>]`.

Puedes reenviar `session_id` en el body para continuidad:

```json
{
  "query": "ahora filtra por presidenciales con expedientes",
  "limit": 5,
  "session_id": "s1_9d2e8f6a1b4c7d20_a41c8e0ff62b15d3"
}
```

## Estado actual

- MVP de ingesta implementado (cliente + pipeline + esquema DB).
- Token provider automatico (Playwright) implementado.
- Base de consulta para copilot ciudadano implementada (vista + funcion + CLI).
- API backend de consulta/copilot implementada con FastAPI.
- Fase Instagram iniciada: plan, migracion base social y endpoint de lectura por candidato.
