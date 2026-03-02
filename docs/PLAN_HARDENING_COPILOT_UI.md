# Plan de Hardening Tecnico (API + UI Copilot)

Fecha: 2026-02-28

## Objetivo

Reducir fallas operativas y riesgos tecnicos detectados en la revision del backend/UI del copilot ciudadano:

- ejecucion local robusta de scripts sin configurar `PYTHONPATH`,
- menor superficie de abuso en `session_id`,
- menor exposicion de detalle de errores de proveedor IA,
- busqueda mas consistente con filtros (`estado`, `organizacion`),
- menor costo de recomputacion del dashboard.

## Alcance

1. Corregir bootstrap de imports en `scripts/*.py`.
2. Firmar `session_id` del copilot y normalizar resolucion en backend.
3. Sanitizar mensajes de fallback IA para frontend.
4. Reescribir query de busqueda para evitar pre-limite antes de aplicar filtros.
5. Agregar cache TTL en endpoint de dashboard.
6. Actualizar documentacion operativa.

## Actividades (estilo Jira)

| Ticket | Actividad | Estado |
|---|---|---|
| JNE-053 | Bootstrap import path para scripts CLI | DONE |
| JNE-054 | Endurecer `session_id` y fallback IA seguro | DONE |
| JNE-055 | Corregir ranking/busqueda con filtros antes de `LIMIT` | DONE |
| JNE-056 | Cache TTL para `dashboard/insights` | DONE |
| JNE-057 | Persistencia conversacional multi-worker (DB) | TODO |

## Criterios de cierre

- `python3 scripts/run_api.py --help` y CLIs principales no fallan por `ModuleNotFoundError`.
- `ask-ai` ya no retorna detalle crudo de errores upstream en `warning`.
- `session_id` generado por backend con formato firmado.
- filtros de busqueda se aplican sobre universo completo antes del `limit`.
- dashboard responde con payload cacheado dentro del TTL configurado.

## Riesgos remanentes

- La memoria conversacional sigue siendo in-memory por proceso; no hay persistencia compartida entre workers/restarts.
