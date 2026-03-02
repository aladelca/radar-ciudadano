# Plan de Implementacion - Copilot IA (OpenAI) para Web

## 1) Objetivo

Evolucionar el copilot actual (SQL/rules-based) a una version asistida por LLM usando OpenAI API, manteniendo:

- evidencia estructurada desde Postgres,
- respuestas explicables con fuentes,
- degradacion controlada cuando no exista `OPENAI_API_KEY`.

## 2) Alcance de esta iteracion

- Nuevo endpoint `POST /api/v1/copilot/ask-ai`.
- Cliente OpenAI por HTTP (`/v1/responses`) sin SDK adicional.
- Integracion en UI web para alternar `SQL` vs `IA`.
- Memoria conversacional por `session_id`.
- Citas por candidato en formato `[ID:...]`.
- Variables de entorno y docs operativas.
- Trazabilidad de modo de respuesta (`mode=ai|fallback`).

No incluye en esta iteracion:

- fine-tuning,
- agentic DB access por parte del modelo,
- almacenamiento de historico de conversaciones.

## 3) Arquitectura propuesta

1. API recibe pregunta del usuario (`query` + filtros opcionales).
2. Repositorio SQL obtiene candidatos y evidencia base.
3. Servicio IA construye prompt solo con evidencia recuperada.
4. OpenAI genera respuesta narrativa breve.
5. API retorna:
- `summary` (texto IA),
- `candidates` (ranking SQL),
- `evidence` (hallazgos trazables),
- `mode` (`ai` o `fallback`),
- `model` usado (si aplica).

## 4) Seguridad y control de alucinacion

- El modelo no consulta BD directamente.
- Solo ve datos de evidencia enviados por backend.
- Se restringe salida a maximo de candidatos entregados por SQL.
- Si falla OpenAI, se usa resumen deterministico existente.

## 5) Backlog (Jira)

- `JNE-037`: Plan tecnico Copilot IA + estrategia de fallback.
- `JNE-038`: Servicio OpenAI + configuracion env.
- `JNE-039`: Endpoint `POST /api/v1/copilot/ask-ai`.
- `JNE-040`: Integracion UI web con selector de modo (`SQL`/`IA`).
- `JNE-041`: Documentacion operativa (README + API_BACKEND + .env.example).
- `JNE-042`: Memoria conversacional por sesion.
- `JNE-043`: Enforcement de citas por candidato.

## 6) Criterios de aceptacion

- Con `OPENAI_API_KEY` valida, `ask-ai` responde `200` con `mode=ai`.
- Sin key o error upstream, `ask-ai` responde `200` con `mode=fallback`.
- UI permite seleccionar modo IA y muestra respuesta sin romper layout.
- Compilacion de modulos Python y carga de frontend sin errores JS criticos.
