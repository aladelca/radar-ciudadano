# Plan Beta Publica - Congreso Votaciones

Fecha: 2026-02-28

## 1) Objetivo de la beta

Publicar una beta accesible por internet para validar uso real, estabilidad y costo operativo del copilot/API,
sin exponer la plataforma a abuso de trafico ni a consultas peligrosas.

## 2) Criterios de salida de beta

- API estable con disponibilidad operativa >= 99% en ventana semanal.
- Limites de uso activos (rate limit + API keys para rutas sensibles).
- `POST /api/v1/copilot/ask-ai` con costo controlado y fallback SQL estable.
- Observabilidad minima disponible (latencia, errores 4xx/5xx, 429, uso IA).
- Runbook de incidentes y checklist de release actualizados.

## 3) Alcance funcional de beta

- Endpoints publicos:
  - `GET /health`
  - `GET /api/v1/candidatos/search`
  - `GET /api/v1/dashboard/insights`
  - `GET /api/v1/candidatos/{id_hoja_vida}` (sin raw por defecto)
  - `GET /api/v1/candidatos/{id_hoja_vida}/instagram`
  - `POST /api/v1/copilot/ask`
  - `POST /api/v1/copilot/ask-ai` (controlado por key)
- Capa de seguridad API:
  - API keys por header `X-API-Key`.
  - Rate limiting para lectura, `copilot/ask` y `copilot/ask-ai`.
  - CORS explicito por origen.
  - Guardrails SQL readonly endurecidos para planner IA.

## 4) Actividades detalladas

### Bloque A - Hardening backend (Semana 1)

1. Implementar API keys configurables por entorno.
2. Separar enforcement de key para lectura y para IA.
3. Implementar rate limiting en `/api/*` con limites independientes para IA.
4. Configurar CORS por `API_CORS_ALLOW_ORIGINS`.
5. Reducir superficie de datos en detalle de candidato (`include_raw=false` por defecto).
6. Endurecer SQL readonly:
   - timeout por consulta,
   - maximo de filas,
   - bloqueo de funciones/esquemas no permitidos,
   - exigir uso de objetos `jne.*`.

Estado actual:

- [DONE] A1 API keys por entorno.
- [DONE] A2 Enforcement separado `read` vs `ask-ai`.
- [DONE] A3 Rate limiting en middleware.
- [DONE] A4 CORS configurable.
- [DONE] A5 `include_raw=false` por defecto en detalle.
- [DONE] A6 Guardrails SQL + timeout + max rows.

### Bloque B - Operacion y despliegue (Semana 1-2)

1. Definir entorno de beta (dominio, proxy/WAF, TLS).
2. Configurar variables de entorno productivas (keys, CORS, limits).
3. Separar credenciales de ingesta y API.
4. Configurar jobs de ingesta diaria y validaciones post-run.
5. Publicar tablero de salud operativo (errores, latencia, uso IA).

Estado actual:

- [IN_PROGRESS] B2 Variables de entorno y defaults de beta.
- [TODO] B1/B3/B4/B5.

### Bloque C - Calidad y gobernanza (Semana 2)

1. Definir smoke tests de endpoints criticos para pre-release.
2. Definir politicas de abuso y respuesta a incidentes (429/401/5xx).
3. Agregar checklist de rollback.
4. Ajustar comunicacion de limitaciones de beta en UI/README.

Estado actual:

- [IN_PROGRESS] C1 smoke tests de endpoints (script base creado).
- [IN_PROGRESS] C4 documentacion tecnica beta.
- [TODO] C2/C3.

## 5) Variables de entorno de beta

- `API_CORS_ALLOW_ORIGINS`
- `BETA_API_KEYS`
- `BETA_AI_API_KEYS`
- `BETA_ENFORCE_API_KEY_READ`
- `BETA_ENFORCE_API_KEY_AI`
- `BETA_ALLOW_ANON_HEALTH`
- `BETA_RATE_LIMIT_READ_PER_MINUTE`
- `BETA_RATE_LIMIT_COPILOT_PER_MINUTE`
- `BETA_RATE_LIMIT_AI_PER_MINUTE`
- `BETA_RATE_LIMIT_WINDOW_SECONDS`
- `BETA_TRUST_PROXY_HEADERS`
- `READONLY_SQL_TIMEOUT_MS`
- `READONLY_SQL_MAX_ROWS`

## 6) Configuracion recomendada para primera beta publica

- `BETA_ENFORCE_API_KEY_READ=false`
- `BETA_ENFORCE_API_KEY_AI=true`
- `BETA_RATE_LIMIT_READ_PER_MINUTE=120`
- `BETA_RATE_LIMIT_COPILOT_PER_MINUTE=40`
- `BETA_RATE_LIMIT_AI_PER_MINUTE=20`
- `READONLY_SQL_TIMEOUT_MS=2500`
- `READONLY_SQL_MAX_ROWS=50`
- `API_CORS_ALLOW_ORIGINS=<dominio-beta-frontend>`

## 7) Riesgos y mitigaciones

- Riesgo: abuso de costo en `ask-ai`.
  - Mitigacion: key obligatoria en IA + rate limit IA + fallback.
- Riesgo: consultas SQL de IA costosas.
  - Mitigacion: timeout y max rows + guardrails SQL.
- Riesgo: fuga de datos raw en endpoint detalle.
  - Mitigacion: `include_raw=false` por defecto; raw solo bajo parametro explicito.
- Riesgo: bloqueo por anti-bot en fuente JNE.
  - Mitigacion: fallback `avanzadaexporta -> browser -> captcha imagen` y runbook operativo.

## 8) Siguientes hitos de implementacion

1. Completar actualizacion de `README.md` y `docs/API_BACKEND.md` para operadores de beta.
2. Agregar smoke script automatizable para validar API post-deploy.
3. Proponer tickets JNE de beta publica en `docs/JIRA_BOARD.md`.
