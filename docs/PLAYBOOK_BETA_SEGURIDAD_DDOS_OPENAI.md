# Playbook Beta - Defensa DDoS y Control de Costo OpenAI

Fecha: 2026-03-02

## 1) Objetivo

Reducir el riesgo de:

- saturacion de API por trafico abusivo,
- consumo excesivo de `POST /api/v1/copilot/ask-ai`,
- degradacion del servicio por consultas pesadas.

Aplica a despliegue en:

- App Runner (backend FastAPI),
- Supabase Cloud (Postgres),
- OpenAI API (copilot IA).

## 2) Modelo de defensa (capas)

Implementar todas las capas, no solo una:

1. Capa edge: AWS WAF delante de App Runner.
2. Capa aplicacion: API keys + rate limit por endpoint.
3. Capa SQL: timeout + limite de filas + validacion de SQL readonly.
4. Capa proveedor IA: limites de uso y monitoreo de costo OpenAI.
5. Capa operativa: kill switch y runbook de incidente.

## 3) Controles ya implementados en este repo

- API keys y enforcement por scope (`read` vs `ai`): `src/jne_ingest/api_app.py`
- Rate limiting en middleware para `/api/*`: `src/jne_ingest/api_app.py`
- Guardrails SQL readonly (`statement_timeout`, `max_rows`, blocklist): `src/jne_ingest/query_repository.py`
- Variables de entorno beta: `.env.example`
- Smoke test API beta: `scripts/smoke_api_beta.py`

## 4) Configuracion recomendada (beta inicial)

### Variables de entorno en App Runner

```dotenv
# Keys
BETA_API_KEYS=<key_a>,<key_b>
BETA_AI_API_KEYS=<key_a>
BETA_ENFORCE_API_KEY_READ=true
BETA_ENFORCE_API_KEY_AI=true
BETA_ALLOW_ANON_HEALTH=true

# Rate limit
BETA_RATE_LIMIT_READ_PER_MINUTE=120
BETA_RATE_LIMIT_AI_PER_MINUTE=10
BETA_RATE_LIMIT_WINDOW_SECONDS=60
BETA_TRUST_PROXY_HEADERS=true

# Guardrails SQL planner IA
READONLY_SQL_TIMEOUT_MS=2500
READONLY_SQL_MAX_ROWS=50
```

Notas:

- En beta publica, dejar `BETA_ENFORCE_API_KEY_READ=true`.
- Mantener `BETA_AI_API_KEYS` como subconjunto reducido.
- Si el trafico es bajo, empezar con `AI=5-10 req/min` y ajustar luego.

## 5) AWS WAF (obligatorio)

Asociar Web ACL al servicio App Runner y activar:

1. Reglas administradas:
   - `AWSManagedRulesCommonRuleSet`
   - `AWSManagedRulesKnownBadInputsRuleSet`
2. Regla rate-based global por IP:
   - accion: `Block`
   - umbral inicial: `2000 requests / 5 min`
3. Regla rate-based especifica para IA (path match `/api/v1/copilot/ask-ai`):
   - accion: `Block`
   - umbral inicial: `100 requests / 5 min`
4. (Opcional) Bot Control si detectas scraping automatizado.

Si usas CDN/proxy intermedio, validar forwarding de IP para no rate-limitar mal.

## 6) Control de costo OpenAI

1. Configurar presupuesto de proyecto y alertas en OpenAI.
2. Definir limite de rate por proyecto/modelo en OpenAI.
3. Monitorear diariamente:
   - volumen de `ask-ai`,
   - error rate,
   - costo diario.
4. Revisar prompts que disparen respuestas largas o loops de reintento.

Importante:

- El budget de OpenAI es normalmente alerta (soft), no sustituto de WAF/app limits.

## 7) Kill switch (emergencia)

Orden recomendado de accion ante abuso:

1. Bloquear temporalmente `/api/v1/copilot/ask-ai` en WAF.
2. Rotar y revocar API keys comprometidas.
3. Reducir limites:
   - `BETA_RATE_LIMIT_AI_PER_MINUTE=1`
   - `BETA_RATE_LIMIT_READ_PER_MINUTE=30`
4. Si el costo continua, remover `OPENAI_API_KEY` en App Runner y redeploy:
   - `ask-ai` queda en fallback SQL (sin consumo OpenAI).

## 8) Monitoreo y alertas

Minimo recomendado:

- Alertar por picos de `429`, `401`, `5xx`.
- Alertar por incremento abrupto de requests a `ask-ai`.
- Alertar por latencia anormal del endpoint `ask-ai`.
- Alertar por incremento diario de costo OpenAI.

## 9) Pruebas de validacion post deploy

1. Health:

```bash
curl -i "https://<tu-dominio>/health"
```

2. Endpoint protegido sin key (debe fallar `401` cuando enforcement esta activo):

```bash
curl -i "https://<tu-dominio>/api/v1/candidatos/search?q=acuna&limit=3"
```

3. Endpoint protegido con key valida:

```bash
curl -i "https://<tu-dominio>/api/v1/candidatos/search?q=acuna&limit=3" \
  -H "X-API-Key: <API_KEY_BETA>"
```

4. Smoke completo:

```bash
python scripts/smoke_api_beta.py \
  --base-url "https://<tu-dominio>" \
  --api-key "<API_KEY_BETA>" \
  --include-ai
```

## 10) Checklist de operacion semanal

- [ ] Rotacion de API keys criticas (si aplica politicamente).
- [ ] Revision de reglas WAF bloqueando trafico real.
- [ ] Revision de limites (`read` y `ai`) segun demanda.
- [ ] Revision de costo OpenAI vs presupuesto semanal.
- [ ] Ejecucion de smoke post deploy.
