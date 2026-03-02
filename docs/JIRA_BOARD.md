# Jira Board - Congreso Votaciones (JNE 2026)

Estado permitido: `TODO`, `IN_PROGRESS`, `BLOCKED`, `DONE`.

| Ticket | Titulo | Estado | Owner | Entregable |
|---|---|---|---|---|
| JNE-001 | Documentacion de plan y arquitectura | DONE | Dev | Plan detallado en `docs/PLAN_INGESTA_JNE_2026.md` |
| JNE-002 | Backlog Jira inicial y seguimiento | DONE | Dev | Este tablero con actualizaciones de estado |
| JNE-003 | Estructura de proyecto de ingesta (Python) | DONE | Dev | Paquete `src/jne_ingest` y CLI base |
| JNE-004 | Migracion SQL para Supabase local | DONE | Dev | Archivo en `supabase/migrations` |
| JNE-005 | Cliente JNE para busqueda avanzada + detalle | DONE | Dev | Cliente HTTP y modelos de respuesta |
| JNE-006 | Persistencia Postgres/Supabase (upsert) | DONE | Dev | Repositorio de datos y escrituras idempotentes |
| JNE-007 | Pipeline de ingesta MVP por CLI | DONE | Dev | Script/CLI ejecutable end-to-end |
| JNE-008 | Configuracion y variables de entorno | DONE | Dev | `.env.example` y `README.md` actualizado |
| JNE-009 | Pruebas basicas y validaciones de calidad | DONE | Dev | Checks de compilacion y smoke test |
| JNE-010 | Base para Copilot (fase siguiente) | DONE | Dev | `docs/COPILOT_BASE.md` + vista SQL + funcion de busqueda + CLI |
| JNE-011 | API backend base (FastAPI) | DONE | Dev | App web con `health` y wiring DB |
| JNE-012 | Endpoint de busqueda y detalle de candidatos | DONE | Dev | Endpoints `search` y `candidate detail` |
| JNE-013 | Endpoint Copilot Ask explicable | DONE | Dev | Endpoint que devuelve respuesta + evidencias |
| JNE-014 | Documentacion API y validacion smoke | DONE | Dev | `docs/API_BACKEND.md` + comandos README |
| JNE-015 | Persistencia total de catálogos y payloads raw | DONE | Dev | Migracion + repositorio + pipeline para guardar toda la data |
| JNE-016 | Exponer raw completo en API de detalle | DONE | Dev | Endpoint candidato con secciones raw completas |
| JNE-017 | Runner de migraciones sin Supabase CLI | DONE | Dev | Script para aplicar SQL por DSN directo |
| JNE-018 | Robustez de ingesta (reintentos/backoff) | DONE | Dev | Cliente HTTP con retry configurable |
| JNE-019 | Runbook operativo end-to-end | DONE | Dev | `docs/RUNBOOK_OPERACION_JNE_2026.md` |
| JNE-020 | Logging operativo de ingesta | DONE | Dev | Logs por run/tipo/pagina/candidato con nivel configurable |
| JNE-021 | Autocorreccion local de reset Supabase | DONE | Dev | `scripts/reset_local.sh` + docs para resolver 502 de storage |
| JNE-022 | Robustez de captcha/recaptcha en búsqueda avanzada | DONE | Dev | Refresh de token por reintento + headers + diagnóstico de 400 |
| JNE-023 | Alineacion de endpoint de búsqueda avanzada con frontend JNE | DONE | Dev | Cliente usa hosts y estrategia de selección equivalente al frontend |
| JNE-024 | Modo browser-first para búsqueda avanzada | DONE | Dev | `search_mode=browser` + perfil persistente Playwright para mitigar `LOW_SCORE` |
| JNE-025 | Fallback híbrido anti `LOW_SCORE` (browser -> captcha imagen) | DONE | Dev | Reintento automático con captcha imagen en el mismo host del error |
| JNE-026 | Plan de integracion Instagram por fases | DONE | Dev | `docs/PLAN_INSTAGRAM_CANDIDATOS.md` + backlog de tickets |
| JNE-027 | Modelo de datos base para Instagram (DB) | DONE | Dev | Migracion `202602280401_instagram_social_base.sql` |
| JNE-028 | API de lectura Instagram por candidato | DONE | Dev | `GET /api/v1/candidatos/{id_hoja_vida}/instagram` + inclusion en detalle |
| JNE-029 | Runner CLI de Business Discovery + persistencia inicial | IN_PROGRESS | Dev | Script `scripts/run_instagram_discovery.py` + cliente Graph API |
| JNE-030 | Curacion de cuentas oficiales de candidatos | TODO | Dev | Carga/validacion de usernames oficiales en `jne.candidato_redes_sociales` |
| JNE-031 | Backfill discovery para candidatos priorizados | TODO | Dev | Corridas batch con snapshots de perfil/media |
| JNE-032 | Onboarding OAuth de cuentas profesionales | TODO | Dev | Flujo de autorizacion + almacenamiento seguro de tokens |
| JNE-033 | Ingesta de comentarios/replies para cuentas onboarded | TODO | Dev | Pipeline de comentarios/replies con permisos aprobados |
| JNE-034 | Señales Instagram en ranking de copilot | TODO | Dev | Vista/funcion SQL extendida y ajuste de endpoints copilot |
| JNE-035 | UI web dashboard + copilot | DONE | Dev | Ruta web `/` con graficos (ingresos/denuncias/universidades) y consulta `copilot/ask` |
| JNE-036 | Capa de planes de gobierno en copilot | TODO | Dev | Ingesta + modelo + endpoint para propuestas por organizacion/candidato |
| JNE-037 | Plan tecnico Copilot IA (OpenAI) | DONE | Dev | `docs/PLAN_COPILOT_IA_OPENAI.md` con alcance, arquitectura y criterios |
| JNE-038 | Servicio OpenAI + configuracion env | DONE | Dev | Cliente HTTP a OpenAI (`/v1/responses`) + llaves/env en config |
| JNE-039 | Endpoint `POST /api/v1/copilot/ask-ai` | DONE | Dev | Endpoint IA con fallback controlado al resumen SQL |
| JNE-040 | Integracion UI web con modo IA | DONE | Dev | Selector `SQL/IA` en formulario y consumo de endpoint correspondiente |
| JNE-041 | Documentacion operativa Copilot IA | DONE | Dev | README/API/.env.example actualizados para uso de `OPENAI_API_KEY` |
| JNE-042 | Memoria conversacional por sesion en copilot IA | DONE | Dev | `session_id` + historial reciente en backend para `ask-ai` |
| JNE-043 | Citas estrictas por candidato en resumen IA | DONE | Dev | Prompt reforzado con `[ID:...]` + postproceso de enforcement |
| JNE-044 | Segmentacion de postulaciones presidencial vs senado en BD | DONE | Dev | Tabla/vistas para distinguir segmentos y permitir multi-postulacion |
| JNE-045 | Ingesta completa Senado Distrito Unico (tipo 20) | DONE | Dev | Corrida productiva + validacion en BD |
| JNE-046 | Ingesta completa Senado Distrito Multiple (tipo 21) | DONE | Dev | Corrida productiva + validacion en BD |
| JNE-047 | Exponer postulaciones multiples en detalle API | DONE | Dev | `GET /api/v1/candidatos/{id}` incluye postulaciones y resumen por persona |
| JNE-053 | Hardening de scripts CLI sin dependencia de PYTHONPATH | DONE | Dev | Bootstrap `scripts/_bootstrap.py` y wiring en runners |
| JNE-054 | Endurecimiento de sesiones Copilot IA y sanitizacion de errores | DONE | Dev | `session_id` firmado + warning seguro en fallback |
| JNE-055 | Correccion de busqueda copilot con filtros antes de LIMIT | DONE | Dev | Query sobre `v_copilot_context` sin pre-limite de 100 |
| JNE-056 | Cache TTL para dashboard insights | DONE | Dev | Cache en `CandidateReadRepository` configurable por env |
| JNE-057 | Persistencia de memoria conversacional en DB | TODO | Dev | Reemplazar store in-memory por backend persistente compartido |
| JNE-048 | Estrategia anti-captcha con `candidato/avanzadaexporta` | DONE | Dev | Búsqueda primaria sin recaptcha + fallback al flujo anterior |
| JNE-058 | Plan de Query Planner tema->SQL para copilot | DONE | Dev | `docs/PLAN_QUERY_PLANNER_COPILOT.md` con alcance y decision de arquitectura |
| JNE-059 | Modulo `query_planner.py` para intencion de consulta | DONE | Dev | Planner de operacion (`aggregate_count` vs `search`) sin dependencia de UI |
| JNE-060 | Integracion de planner en `ask` y `ask-ai` | DONE | Dev | Endpoints ejecutan SQL por plan antes de narracion IA |
| JNE-061 | Catalogo dinamico de metricas desde DB (sin counters hardcodeados) | DONE | Dev | Introspeccion `v_copilot_context` + inferencia de metrica por query |
| JNE-062 | Decision LangChain vs planner deterministico | DONE | Dev | Decision documentada: no LangChain en esta fase |
| JNE-063 | Planner IA SQL (objetivo + datos requeridos + query) | DONE | Dev | `ask-ai` genera y ejecuta SQL desde prompt estructurado |
| JNE-064 | Ejecutor SQL readonly con validacion de seguridad | DONE | Dev | Ejecucion de SQL IA en backend con guardrails SELECT-only |
| JNE-065 | Documentacion de prompts por seccion | DONE | Dev | `docs/COPILOT_PROMPTS.md` con contratos y responsabilidades |
| JNE-066 | Flexibilidad por entidad en planner IA (`answer_level`) | DONE | Dev | Planner soporta candidato/partido/segmento y evita asumir solo `id_hoja_vida` |
| JNE-067 | Evidencias y citas para resultados agregados (`[ROW:n]`) | DONE | Dev | `ask-ai` y narracion IA citan filas agregadas cuando no hay ID de candidato |
| JNE-068 | Modo de ejecucion derivada en planner (`execution_mode`) | DONE | Dev | Agente decide `sql|derived`; resolver `income_amount_ranking` para montos de ingresos |
| JNE-068 | Segmentacion explícita de Diputados en BD | DONE | Dev | `segmento_postulacion=DIPUTADOS` + vistas de resumen actualizadas |
| JNE-069 | Ingesta completa Diputados con detalle (tipo 15) | DONE | Dev | Corridas productivas con detalle completo en `hoja_vida/anotaciones/expedientes` |
| JNE-070 | Particionado de ingesta para cargas masivas | DONE | Dev | Flags `--partition-mod/--partition-rem` + soporte config/env |
| JNE-071 | Ingesta completa Parlamento Andino (tipo 3) | DONE | Dev | Corrida productiva con detalle completo y validacion fuente-vs-BD |
| JNE-072 | Plan tecnico de pipeline multiagente para copilot IA | DONE | Dev | `docs/PLAN_COPILOT_MULTIAGENT_PIPELINE.md` con arquitectura y criterios |
| JNE-073 | Schema Retrieval Agent para seleccion de tablas/joins | DONE | Dev | Agente de data requirements + normalizacion/guardrails |
| JNE-074 | SQL Critic + SQL Repair loop en planner IA | DONE | Dev | Critica semantica/tecnica y reparacion automatica (1 intento) |
| JNE-075 | Refactor de prompts multiagente y contratos JSON | DONE | Dev | Prompts separados (objective/schema/builder/critic/repair) documentados |
| JNE-076 | Observabilidad de resultados por agente en logs | DONE | Dev | Logs estructurados por etapa y decision de pipeline |

## Registro de cambios

- 2026-02-28: Tablero creado.
- 2026-02-28: JNE-001, JNE-003, JNE-004, JNE-005, JNE-006, JNE-007, JNE-008 y JNE-009 movidos a `DONE` por implementacion inicial del MVP.
- 2026-02-28: JNE-010 movido a `IN_PROGRESS` para iniciar base de copilot ciudadano.
- 2026-02-28: JNE-010 movido a `DONE` por implementacion de `v_copilot_context`, `search_candidatos_copilot` y `scripts/copilot_query.py`.
- 2026-02-28: JNE-002 movido a `DONE` tras cierre de estados y bitacora de avance.
- 2026-02-28: Se abre fase API con JNE-011..JNE-014; JNE-011 inicia en `IN_PROGRESS`.
- 2026-02-28: JNE-011, JNE-012 y JNE-013 movidos a `DONE` por implementacion de FastAPI + endpoints de consulta y copilot.
- 2026-02-28: JNE-014 movido a `DONE` con `docs/API_BACKEND.md` y validacion por compilacion de modulos.
- 2026-02-28: Se abre fase de persistencia total con JNE-015..JNE-016; JNE-015 en `IN_PROGRESS`.
- 2026-02-28: JNE-015 y JNE-016 movidos a `DONE` por persistencia total de payloads raw + catálogos y exposición en API.
- 2026-02-28: Se abre fase de operatividad local con JNE-017..JNE-018; JNE-017 en `IN_PROGRESS`.
- 2026-02-28: JNE-017 movido a `DONE` con `scripts/apply_migrations.py` para aplicar SQL sin Supabase CLI.
- 2026-02-28: JNE-018 movido a `DONE` con reintentos/backoff HTTP configurables en cliente de ingesta.
- 2026-02-28: JNE-019 movido a `DONE` con runbook operativo completo de ingesta+API.
- 2026-02-28: JNE-020 movido a `DONE` con logging configurable (`--log-level`, `JNE_LOG_LEVEL`).
- 2026-02-28: JNE-021 movido a `DONE` con script de reset autocorregible y runbook actualizado para error `storage/v1/bucket -> 502`.
- 2026-02-28: JNE-022 movido a `DONE` con manejo de `CAPTCHA_REQUIRED`, reintento con token nuevo y logging de `reason/mode/score` en 400.
- 2026-02-28: JNE-023 movido a `DONE` al alinear `POST /api/v1/candidato/avanzada` con el frontend (hosts `apiPath5/apiPath4/apiPath6/apiPath7` y un host aleatorio por request).
- 2026-02-28: JNE-024 en `IN_PROGRESS` para ejecutar búsqueda avanzada desde navegador (cookies/sesión) y reducir bloqueo anti-bot por `LOW_SCORE`.
- 2026-02-28: JNE-024 movido a `DONE`: `search_mode=browser` quedó operativo; validación smoke (`max-pages=1`) exitosa usando `JNE_PLAYWRIGHT_USER_DATA_DIR=/tmp/jne-browser-profile` (20 candidatos, 0 errores).
- 2026-02-28: JNE-025 en `IN_PROGRESS` para implementar fallback automático `browser -> captcha imagen` cuando el API devuelve `mode=image` por `LOW_SCORE`.
- 2026-02-28: JNE-025 movido a `DONE`: fallback híbrido estabilizado (token browser reutilizado + captcha del host afectado) y corrida real completada con 108 candidatos persistidos, 6 páginas, 0 errores (`run_id=8cb47de8-115e-4f05-911d-041182edff7e`).
- 2026-02-28: Se abre fase Instagram con JNE-026..JNE-034 y plan técnico en `docs/PLAN_INSTAGRAM_CANDIDATOS.md`.
- 2026-02-28: JNE-026 movido a `DONE` con definición de fases (discovery publico, operacion y onboarding OAuth).
- 2026-02-28: JNE-027 movido a `DONE` con migracion base de cuentas/snapshots/runs/vistas latest para Instagram.
- 2026-02-28: JNE-028 movido a `DONE` con endpoint `GET /api/v1/candidatos/{id_hoja_vida}/instagram` y bloque `instagram` en el detalle del candidato.
- 2026-02-28: JNE-029 movido a `IN_PROGRESS` con cliente `InstagramDiscoveryClient` y runner `scripts/run_instagram_discovery.py`; falta validacion E2E con token real.
- 2026-02-28: JNE-035 movido a `DONE` con UI web en `GET /`, endpoint `GET /api/v1/dashboard/insights` y panel conectado a `POST /api/v1/copilot/ask`.
- 2026-02-28: Se abre bloque Copilot IA con JNE-037..JNE-041; tickets iniciales en `IN_PROGRESS`.
- 2026-02-28: JNE-037 movido a `DONE` con plan tecnico `docs/PLAN_COPILOT_IA_OPENAI.md`.
- 2026-02-28: JNE-038 movido a `DONE` al agregar `OpenAICopilotService` y nuevas variables en `AppConfig`.
- 2026-02-28: JNE-039 movido a `DONE` con endpoint `POST /api/v1/copilot/ask-ai` y fallback SQL controlado.
- 2026-02-28: JNE-040 movido a `DONE` con selector `Usar IA` en la UI web y routing de endpoint por modo.
- 2026-02-28: JNE-041 movido a `DONE` con actualizacion de `.env.example`, `README.md`, `docs/API_BACKEND.md` y `docs/COPILOT_BASE.md`.
- 2026-02-28: Validacion de JNE-039/JNE-040: smoke local exitoso de `ask-ai` en `mode=fallback` sin key y selector `Usar IA` operativo en UI.
- 2026-02-28: JNE-042 movido a `DONE` con store en memoria por sesion (`session_id`, historial, limites configurables) y uso en `ask-ai`.
- 2026-02-28: JNE-043 movido a `DONE` con citas por candidato en prompt IA y fallback de citas cuando el texto no incluye `[ID:...]`.
- 2026-02-28: Se abren JNE-044..JNE-047 para separar Presidencial/Senado, ejecutar ingesta Senado completa y exponer la diferenciacion en API.
- 2026-02-28: JNE-044 movido a `IN_PROGRESS` para crear tabla de postulaciones multisegmento y vistas de resumen por persona.
- 2026-02-28: Validacion de JNE-042/JNE-043: `ask-ai` mantiene `history_used` por sesion y devuelve `citations` cuando existen candidatos.
- 2026-02-28: Se abre plan de hardening en `docs/PLAN_HARDENING_COPILOT_UI.md` y backlog JNE-053..JNE-057.
- 2026-02-28: JNE-053 movido a `DONE` al agregar `scripts/_bootstrap.py` + wiring en `run_api`, `run_ingest`, `run_plan_gobierno_ingest`, `run_instagram_discovery` y `copilot_query`.
- 2026-02-28: JNE-054 movido a `DONE` con `session_id` firmado (`s1_<nonce>_<firma>`), `health` con metadata operativa y fallback IA con `warning` sanitizado.
- 2026-02-28: JNE-055 movido a `DONE` al aplicar filtros sobre ranking completo en `v_copilot_context` antes de `LIMIT` (sin truncar en top-100 previo).
- 2026-02-28: JNE-056 movido a `DONE` con cache TTL de dashboard (`DASHBOARD_CACHE_TTL_SECONDS`, default 60s).
- 2026-02-28: Se abre JNE-048 en `IN_PROGRESS` para cambiar búsqueda primaria a `candidato/avanzadaexporta` y reducir bloqueo por captcha en corridas masivas.
- 2026-02-28: Ajuste adicional de JNE-055: parser de consultas agregadas generalizado (denuncias/sentencias/expedientes/ingresos/bienes/anotaciones/titularidades) con conteos SQL directos y tokenización corregida para frases naturales.
- 2026-02-28: Se abre bloque JNE-058..JNE-062 para planner tema->SQL; plan documentado en `docs/PLAN_QUERY_PLANNER_COPILOT.md`.
- 2026-02-28: JNE-059 y JNE-060 movidos a `DONE` con modulo `query_planner.py` e integracion directa en `POST /api/v1/copilot/ask` y `ask-ai`.
- 2026-02-28: JNE-061 movido a `DONE` al reemplazar mapeos fijos por catalogo dinamico de metricas desde `jne.v_copilot_context` (incluye metricas derivadas).
- 2026-02-28: JNE-062 movido a `DONE` tras decision de arquitectura: planner deterministico primero, LangChain se reevaluara en fase multi-tool.
- 2026-02-28: Se implementa planner IA SQL en `ask-ai` (objetivo -> chequeo de datos -> SQL), con ejecucion readonly y fallback local.
- 2026-02-28: JNE-063, JNE-064 y JNE-065 movidos a `DONE` con implementacion de planner SQL IA y documentacion de prompts.
- 2026-02-28: Endurecimiento adicional de prompts/validacion del planner SQL IA: JSON estricto, reglas de filtros obligatorios (`estado/organizacion`) y ejemplos in-prompt.
- 2026-02-28: Refactor a arquitectura de dos agentes IA en `ask-ai`: `Objective Agent` (intencion/objetivo) + `SQL Builder Agent` (query/required_data), con chequeo de alineacion entre ambos.
- 2026-02-28: JNE-066 y JNE-067 movidos a `DONE` con contrato `answer_level` (candidate/organization/election_segment/general), validacion SQL por nivel de respuesta y citas `[ROW:n]` para resultados agregados por partido/segmento.
- 2026-02-28: JNE-068 movido a `DONE` con contrato `execution_mode`/`derived_resolver` para que la IA elija ejecucion derivada sin hardcode por query (caso ingresos por monto desde `declaracion_ingresos`).
- 2026-02-28: JNE-048 movido a `DONE` al dejar `candidato/avanzadaexporta` como búsqueda primaria (sin recaptcha) con fallback al flujo anterior.
- 2026-02-28: JNE-045 movido a `DONE` con corrida completa Senado tipo 20 (`run_id=7d50131b-1f81-4d7a-9b11-f3806a365d75`, `1131` leídos/persistidos, `0` errores).
- 2026-02-28: JNE-046 movido a `DONE` con corrida completa Senado tipo 21 (`run_id=e6f20bd7-50e8-44e0-b951-d87e078fbd60`, `1833` leídos/persistidos, `0` errores); en fuente `avanzadaexporta` hay un `idHojaVida` repetido (`248725`) y por eso quedan `1832` postulaciones únicas.
- 2026-02-28: JNE-044 y JNE-047 movidos a `DONE` tras validar segmentación Presidencial/Senado y exposición de postulaciones múltiples en el detalle API.
- 2026-03-01: JNE-068 movido a `DONE` al extender `segmento_postulacion` con `DIPUTADOS` y actualizar vistas (`v_candidatos_segmento_postulacion`, `v_postulaciones_resumen_persona`).
- 2026-03-01: JNE-070 movido a `DONE` al habilitar particionado por `id_hoja_vida` en `run_ingest` (`--partition-mod/--partition-rem` y `JNE_PARTITION_MOD/JNE_PARTITION_REM`).
- 2026-03-01: JNE-069 movido a `DONE` tras ingesta completa Diputados (tipo 15) en 4 particiones (`run_id=03aafaba-a555-430b-888c-52ce6f4954b9`, `6421759e-8223-4ecf-ae90-2f17af47d517`, `635983b7-a3b2-4e68-aeb6-675fe6ad5860`, `2d6bd617-3028-47f6-bb4e-6f2fd2a2e1f3`): `1377 + 1399 + 1348 + 1341 = 5465`, con detalle completo persistido y `0` errores por corrida.
- 2026-03-01: JNE-071 movido a `DONE` tras ingesta completa Parlamento Andino (tipo 3, `run_id=30ffe044-9bcd-4eeb-897c-a790b3f44f62`): `528` leídos/persistidos, `0` errores, y validación fuente-vs-BD sin faltantes.
- 2026-03-02: Se abre bloque multiagente del copilot con JNE-072..JNE-076; JNE-072 inicia en `IN_PROGRESS` para formalizar arquitectura Objective -> Schema -> Builder -> Critic -> Repair.
- 2026-03-02: JNE-072..JNE-076 movidos a `DONE` con pipeline multiagente implementado en `OpenAICopilotService` (Objective + Schema Retrieval + SQL Builder + SQL Critic + SQL Repair), observabilidad por etapa y actualización de `docs/COPILOT_PROMPTS.md`.
