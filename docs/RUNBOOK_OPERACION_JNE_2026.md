# Runbook Operativo - Ingesta y API JNE 2026

## 1) Objetivo

Operar la plataforma para:

- extraer candidatos del JNE (EG 2026),
- persistir **toda** la data en Postgres/Supabase local (normalizada + raw),
- exponer API de consulta y copilot.

Este runbook sirve para ejecuciones iniciales, recurrentes y resolución de incidentes.

## 2) Alcance de datos persistidos

La ingesta guarda:

- `jne.candidatos` (`raw_payload` incluido),
- `jne.hoja_vida_raw`,
- `jne.hoja_vida_secciones_raw`,
- `jne.sentencias_penales`,
- `jne.sentencias_obligaciones`,
- `jne.declaracion_ingresos`,
- `jne.bienes_inmuebles`,
- `jne.bienes_muebles`,
- `jne.otros_bienes_muebles`,
- `jne.titularidad_acciones`,
- `jne.anotaciones_marginales`,
- `jne.expedientes_candidato`,
- `jne.anotaciones_raw`,
- `jne.expedientes_raw`,
- `jne.candidatos_postulaciones` (diferenciación explícita por tipo/segmento),
- catálogos:
  - `jne.catalog_procesos_electorales`,
  - `jne.catalog_tipos_eleccion`,
  - `jne.catalog_organizaciones_politicas`.

La ingesta independiente de planes de gobierno guarda:

- `jne.plan_gobierno_runs`,
- `jne.candidato_plan_gobierno`,
- `jne.planes_gobierno`,
- `jne.planes_gobierno_dimensiones`,
- `jne.planes_gobierno_pdf_texto` (texto extraido de PDF, sin binario).

## 3) Requisitos

- Python 3.11+
- PostgreSQL accesible por DSN
- Dependencias Python instaladas
- `pypdf` instalado (para extraccion de texto de PDF de planes)
- Playwright + Chromium (si se usa token automático)
- Red saliente a:
  - `plataformaelectoral.jne.gob.pe`
  - `apiplataformaelectoral*.jne.gob.pe`
  - servicios de reCAPTCHA de Google

## 4) Preparación inicial

1. Crear entorno:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Variables:

```bash
cp .env.example .env
```

3. Configuración mínima de `.env`:

```bash
DATABASE_DSN=postgresql://postgres:postgres@127.0.0.1:54322/postgres
JNE_PROCESS_ID=124
JNE_TIPO_ELECCION_NOMBRE=PRESIDENCIAL
JNE_PAGE_SIZE=20
JNE_TIMEOUT_SECONDS=45
JNE_REQUEST_RETRIES=3
JNE_BACKOFF_SECONDS=1.0
JNE_CAPTCHA_RETRIES=4
JNE_VERIFY_SSL=false
JNE_PLAYWRIGHT_HEADLESS=false
```

4. Instalar Chromium para Playwright:

```bash
source .venv/bin/activate
python -m playwright install chromium
```

## 5) Migraciones de base de datos

### Opción A: con Supabase CLI

```bash
supabase start
./scripts/reset_local.sh
```

Notas:

- `scripts/reset_local.sh` usa `supabase db reset --debug`.
- Si ocurre el fallo intermitente `GET /storage/v1/bucket -> 502`, aplica autocorrección reiniciando Kong y vuelve a validar health.

### Opción B: sin Supabase CLI (recomendada para Postgres genérico)

```bash
source .venv/bin/activate
python scripts/apply_migrations.py --db-dsn "$DATABASE_DSN"
```

Comportamiento:

- crea `public.schema_migrations`,
- aplica `.sql` en `supabase/migrations` en orden,
- no reaplica migraciones ya registradas.

## 6) Ejecución de ingesta

### 6.1 Smoke test (sin escribir en BD)

```bash
source .venv/bin/activate
python scripts/run_ingest.py \
  --dry-run \
  --process-id 124 \
  --tipo-eleccion-nombre PRESIDENCIAL \
  --token-provider playwright \
  --search-mode browser \
  --max-pages 1
```

### 6.2 Carga real completa (escribe en BD)

```bash
source .venv/bin/activate
python scripts/run_ingest.py \
  --process-id 124 \
  --tipo-eleccion-nombre PRESIDENCIAL \
  --token-provider playwright \
  --search-mode browser
```

Notas:

- `IdTipoEleccion=1` corresponde a `PRESIDENCIAL` (EG 2026).
- `IdOrganizacionPolitica=0` (TODOS) se aplica en el pipeline.
- La búsqueda usa primero `POST /api/v1/candidato/avanzadaexporta` (sin recaptcha). Solo si falla, degrada a `candidato/avanzada` con captcha.
- `search_mode=browser` ejecuta `candidato/avanzada` dentro del navegador (Playwright), con cookies/sesión del contexto web.
- para estabilizar score anti-bot, usar perfil persistente:
  - `JNE_PLAYWRIGHT_USER_DATA_DIR=/tmp/jne-browser-profile` (o un path propio persistente).
- `candidato/avanzadaexporta` puede devolver filas duplicadas por `idHojaVida` en origen; la BD deduplica por PK y mantiene idempotencia.
- El cliente replica el frontend JNE para búsqueda avanzada:
  - usa hosts `apiPath5/apiPath4/apiPath6/apiPath7`,
  - envía el `POST /api/v1/candidato/avanzada` a un solo host aleatorio por request.
- Si quieres token manual:
  - usar `.env` con `JNE_RECAPTCHA_TOKEN`,
  - o `--token-provider static --token "<token>"`.

Carga Senado (ejecutar ambos tipos):

```bash
# SENADORES DISTRITO UNICO (id=20)
python scripts/run_ingest.py \
  --process-id 124 \
  --tipo-eleccion-id 20 \
  --search-mode api \
  --token-provider env \
  --max-pages 1

# SENADORES DISTRITO MULTIPLE (id=21)
python scripts/run_ingest.py \
  --process-id 124 \
  --tipo-eleccion-id 21 \
  --search-mode api \
  --token-provider env \
  --max-pages 1
```

Carga Diputados (tipo 15):

```bash
PYTHONPATH=src python scripts/run_ingest.py \
  --process-id 124 \
  --tipo-eleccion-id 15 \
  --search-mode api \
  --token-provider env \
  --max-pages 1
```

Carga Parlamento Andino (tipo 3):

```bash
PYTHONPATH=src python scripts/run_ingest.py \
  --process-id 124 \
  --tipo-eleccion-id 3 \
  --search-mode api \
  --token-provider env \
  --max-pages 1
```

Para cargas masivas, puedes particionar por `id_hoja_vida` y correr en paralelo:

```bash
# particion 0/4
PYTHONPATH=src python scripts/run_ingest.py --process-id 124 --tipo-eleccion-id 15 --search-mode api --token-provider env --max-pages 1 --partition-mod 4 --partition-rem 0
# particion 1/4
PYTHONPATH=src python scripts/run_ingest.py --process-id 124 --tipo-eleccion-id 15 --search-mode api --token-provider env --max-pages 1 --partition-mod 4 --partition-rem 1
# particion 2/4
PYTHONPATH=src python scripts/run_ingest.py --process-id 124 --tipo-eleccion-id 15 --search-mode api --token-provider env --max-pages 1 --partition-mod 4 --partition-rem 2
# particion 3/4
PYTHONPATH=src python scripts/run_ingest.py --process-id 124 --tipo-eleccion-id 15 --search-mode api --token-provider env --max-pages 1 --partition-mod 4 --partition-rem 3
```

### 6.3 Carga de planes de gobierno (pipeline independiente)

Este paso depende de que `jne.candidatos` y `jne.hoja_vida_raw` ya tengan datos.

```bash
source .venv/bin/activate
python scripts/run_plan_gobierno_ingest.py \
  --process-id 124 \
  --tipo-eleccion-nombre PRESIDENCIAL
```

Opciones utiles:

- corrida parcial:

```bash
python scripts/run_plan_gobierno_ingest.py \
  --process-id 124 \
  --tipo-eleccion-nombre PRESIDENCIAL \
  --max-candidates 20
```

- sin extraer texto PDF:

```bash
python scripts/run_plan_gobierno_ingest.py \
  --process-id 124 \
  --tipo-eleccion-nombre PRESIDENCIAL \
  --skip-pdf-text
```

## 7) Validación post-ingesta

Conectar a tu Postgres y ejecutar:

```sql
select status, started_at, finished_at, candidates_read, candidates_persisted, errors_count
from jne.ingesta_runs
order by started_at desc
limit 5;
```

```sql
select
  (select count(*) from jne.candidatos) as candidatos,
  (select count(*) from jne.candidatos_postulaciones) as candidatos_postulaciones,
  (select count(*) from jne.hoja_vida_raw) as hoja_vida_raw,
  (select count(*) from jne.hoja_vida_secciones_raw) as hoja_vida_secciones_raw,
  (select count(*) from jne.anotaciones_raw) as anotaciones_raw,
  (select count(*) from jne.expedientes_raw) as expedientes_raw,
  (select count(*) from jne.catalog_procesos_electorales) as catalog_procesos,
  (select count(*) from jne.catalog_tipos_eleccion) as catalog_tipos,
  (select count(*) from jne.catalog_organizaciones_politicas) as catalog_orgs;
```

Validacion de diferenciacion presidencial/senado:

```sql
select segmento_postulacion, count(*) as total
from jne.candidatos_postulaciones
where id_proceso_electoral = 124
group by segmento_postulacion
order by segmento_postulacion;
```

Validacion de segmentacion incluyendo Diputados:

```sql
select segmento_postulacion, count(*) as total
from jne.candidatos_postulaciones
where id_proceso_electoral = 124
group by segmento_postulacion
order by segmento_postulacion;
```

```sql
select persona_key, numero_documento, nombre_completo, postula_presidencial, postula_senado, total_postulaciones
from jne.v_postulaciones_resumen_persona
where postula_presidencial or postula_senado
order by postula_presidencial desc, postula_senado desc, total_postulaciones desc
limit 20;
```

Validacion de planes de gobierno:

```sql
select status, started_at, finished_at, candidates_read, plans_resolved, pdf_texts_extracted, errors_count
from jne.plan_gobierno_runs
order by started_at desc
limit 5;
```

```sql
select
  (select count(*) from jne.candidato_plan_gobierno) as candidatos_plan,
  (select count(*) from jne.planes_gobierno) as planes,
  (select count(*) from jne.planes_gobierno_dimensiones) as dimensiones,
  (select count(*) from jne.planes_gobierno_pdf_texto) as pdf_textos;
```

```sql
select id_hoja_vida, nombre_completo, organizacion_politica, cargo, estado
from jne.candidatos
order by updated_at desc
limit 20;
```

Criterio mínimo de éxito:

- hay `ingesta_runs.status='completed'`,
- `candidates_persisted > 0`,
- tablas raw y normalizadas con datos.

## 8) API de consulta/copilot

Levantar API:

```bash
source .venv/bin/activate
python scripts/run_api.py --host 127.0.0.1 --port 8010
```

Swagger:

- `http://127.0.0.1:8010/docs`

Pruebas rápidas:

```bash
curl "http://127.0.0.1:8010/health"
curl "http://127.0.0.1:8010/api/v1/candidatos/search?q=acuna&limit=5"
curl "http://127.0.0.1:8010/api/v1/candidatos/245682"
curl -X POST "http://127.0.0.1:8010/api/v1/copilot/ask" \
  -H "content-type: application/json" \
  -d '{"query":"denuncias patrimonio", "limit":5}'
```

## 9) Operación recurrente

Frecuencia sugerida:

- 1 vez al día durante periodos activos de cambios.

Flujo diario:

1. verificar conectividad DB,
2. ejecutar ingesta completa,
3. validar métricas en `ingesta_runs`,
4. revisar muestra de candidatos por API,
5. registrar incidencias.

## 10) Troubleshooting

### Error: `CAPTCHA_REQUIRED` o `CAPTCHA_INVALID`

- reintentar con `--token-provider playwright`,
- si falla sandbox/entorno, usar token manual temporal.
- revisar detalle en logs (`reason=...`) para distinguir `INVALID_TOKEN`, `DUPE`, etc.
- aumentar `JNE_CAPTCHA_RETRIES` si hay rechazo intermitente.
- preferir `JNE_PLAYWRIGHT_HEADLESS=false` (navegador visible) para mejorar aceptación del token.
- si aparece `reason=LOW_SCORE mode=image`:
  - cambiar a `search_mode=browser` y definir `JNE_PLAYWRIGHT_USER_DATA_DIR` persistente,
  - el pipeline descarga captcha en imagen (`/api/v1/captcha/image`) y loguea ruta local `jne_captcha_*.png`,
  - puedes resolverlo ingresando texto en consola cuando sea solicitado,
  - para ejecución no interactiva, define `JNE_CAPTCHA_TEXT=<texto>` (si es incorrecto, la validación falla y vuelve a pedir token).

### Error SSL (`certificate verify failed`)

- confirmar `JNE_VERIFY_SSL=false` en `.env`.

### Error Playwright browser faltante

```bash
python -m playwright install chromium
```

### Error conexión DB (`connection refused`)

- revisar que el Postgres esté levantado y DSN correcto,
- validar puerto/credenciales.

### Error al extraer PDF (`No se encontro parser PDF`)

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

### Error `storage/v1/bucket -> 502` durante `supabase db reset`

- ejecutar el wrapper operativo:

```bash
./scripts/reset_local.sh
```

- causa observada: condición de carrera en restart donde Kong queda apuntando a upstream previo de Storage.

### Error de red JNE intermitente

- aumentar:
  - `JNE_REQUEST_RETRIES`
  - `JNE_BACKOFF_SECONDS`

## 11) Recuperación y reproceso

Reproceso completo:

1. limpiar/recrear base (si aplica),
2. aplicar migraciones,
3. ejecutar ingesta completa.

Reproceso parcial:

- usar `--max-pages` para acotar pruebas,
- luego ejecutar sin límite para completar.

## 12) Seguridad y cumplimiento

- Mantener secretos solo en `.env` (no commitear).
- No modificar la data fuente del JNE.
- Exponer en UI/API fecha de actualización y fuente.

## 13) Checklist de release operativo

- [ ] Migraciones aplicadas
- [ ] Ingesta `completed` sin errores críticos
- [ ] Tablas raw + normalizadas pobladas
- [ ] `/health` OK
- [ ] Endpoints principales respondiendo
- [ ] Evidencias/campos de origen visibles para copilot
