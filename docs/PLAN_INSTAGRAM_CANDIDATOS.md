# Plan Instagram - Candidatos

## Objetivo

Agregar una capa de informacion de Instagram para candidatos, priorizando fuentes oficiales de Meta y cumplimiento de politicas.

## Alcance por fases

### Fase 1: Base tecnica (discovery publico)

1. Modelo de datos para cuentas vinculadas, snapshots de perfil/media y corridas de ingesta.
2. API de lectura por candidato para exponer cuentas y ultimos snapshots.
3. Runner CLI para consultar Business Discovery y persistir resultados.

Entregable: perfil publico del candidato (followers/media_count + media reciente y metricas publicas disponibles).

### Fase 2: Operacion y calidad

1. Curacion de cuentas oficiales por candidato (proceso manual + validacion).
2. Backfill inicial para candidatos priorizados.
3. Telemetria basica de corridas (exitos, fallos, volumen de media).

Entregable: pipeline repetible y monitoreable para discovery.

### Fase 3: Onboarding oficial (OAuth)

1. Flujo de autorizacion para cuentas profesionales.
2. Gestion segura de tokens y renovacion.
3. Ingesta ampliada para media/comentarios/replies/insights de cuentas onboarded.

Entregable: cobertura profunda donde exista consentimiento del candidato/equipo.

## Restricciones clave

1. Perfil publico no implica acceso irrestricto a todos los endpoints.
2. Discovery de terceros entrega datos publicos limitados y no habilita GET directo completo de media de terceros.
3. Comentarios/replies/insights de mayor profundidad requieren app users onboarded y permisos aprobados.

## Datos a exponer al ciudadano

1. Cuenta(s) Instagram asociadas al candidato (oficial/no oficial, fuente).
2. Ultimo snapshot de perfil (seguidores, cantidad de publicaciones, biografia, web).
3. Publicaciones recientes y metricas publicas (likes/comments/views segun disponibilidad).
4. Fecha/hora de captura para transparencia.

## Riesgos y mitigaciones

1. Riesgo: cambios de politicas/permissions de Meta.
Mitigacion: separar discovery publico de onboarding OAuth, con feature flags por fuente.

2. Riesgo: username incorrecto o no oficial.
Mitigacion: tabla de vinculacion con bandera `is_oficial` y notas de curacion.

3. Riesgo: limites/rate limiting.
Mitigacion: corridas incrementalmente y uso preferente de webhooks en fase onboarding.
