# Plan de Implementacion - Plataforma de Candidatos 2026

## 1) Objetivo del producto

Construir una plataforma informativa para que la poblacion pueda consultar y comparar candidatos de elecciones 2026 usando datos del JNE, incluyendo:

- Datos personales y trayectoria.
- Sentencias/denuncias declaradas.
- Declaracion jurada de ingresos, bienes y rentas.
- Anotaciones marginales.
- Expedientes relacionados.
- Navegacion asistida por un "copilot" sobre la data consolidada.

## 2) Alcance inicial (MVP)

El MVP cubre:

- Ingesta automatizada de candidatos y detalle por `idHojaVida`.
- Persistencia en Supabase local (Postgres) con modelo normalizado + JSON crudo.
- Ejecucion por CLI con filtros de proceso/tipo de eleccion y paginacion.
- Base lista para exponer API y copilot en siguiente fase.

No incluye en esta fase:

- UI final de produccion.
- Motor de copilot completo en frontend.
- Moderacion avanzada o fact-check externo.

## 3) Descubrimiento tecnico validado

Se confirmo que el portal usa endpoints API internos `apiplataformaelectoral*.jne.gob.pe` y filtros por:

- `IdProcesoElectoral`
- `IdTipoEleccion`
- `IdOrganizacionPolitica`

### Endpoints clave para candidatos

- `POST /api/v1/candidato/avanzada` (listado paginado de candidatos)
- `GET /api/v1/candidato/hoja-vida?IdHojaVida=...`
- `GET /api/v1/candidato/anotacion-marginal?IdHojaVida=...`
- `GET /api/v1/candidato/expediente?IdHojaVida=...`

### Endpoints clave para planes de gobierno

- `GET /api/v1/plan-gobierno/detalle-para-candidato?IdProcesoElectoral=...&IdTipoEleccion=...&IdOrganizacionPolitica=...&IdSolicitudLista=...`
- `GET /api/v1/plan-gobierno/detalle?IdPlanGobierno=...`

### Endpoints de catalogos

- `GET /api/v1/expediente/proceso-electoral`
- `GET /api/v1/tipo-eleccion/tipo/{idProceso}`
- `GET /api/v1/organizacion-politica/op/{idProceso}`

### Campos importantes detectados en hoja de vida

- `sentenciaPenal`
- `sentenciaObliga` (obligaciones alimentarias/contractuales/laborales/violencia)
- `declaracionJurada.ingreso`
- `declaracionJurada.bienInmueble`
- `declaracionJurada.bienMueble`
- `declaracionJurada.otroMueble`
- `declaracionJurada.titularidad`

## 4) Arquitectura objetivo

1. **Ingestor** (Python):
- Consulta catalogos.
- Ejecuta busqueda avanzada paginada.
- Recorre candidatos y obtiene detalle por `idHojaVida`.
- Aplica reintentos y control de tasa.

1. **Ingestor independiente de planes** (Python):
- Reusa `candidatos` + `hoja_vida_raw` para resolver `idSolicitudLista`.
- Obtiene detalle de plan por candidato y normaliza por `idPlanGobierno`.
- Descarga PDF solo para extraer texto; no persiste binario.

2. **Persistencia** (Supabase Postgres local):
- Tablas normalizadas para consulta.
- Tabla `raw` JSON para trazabilidad y reproceso.
- `upsert` por llaves naturales.

3. **Servicios de consulta** (fase siguiente):
- API REST para frontend.
- Capa de busqueda semantica/filtros para copilot.

## 5) Modelo de datos propuesto

### Tablas base

- `ingesta_runs`: control de ejecuciones.
- `procesos_electorales`: catalogo de procesos.
- `candidatos`: entidad principal.
- `hoja_vida_raw`: payload JSON completo.

### Tablas de detalle

- `sentencias_penales`
- `sentencias_obligaciones`
- `declaracion_ingresos`
- `bienes_inmuebles`
- `bienes_muebles`
- `otros_bienes_muebles`
- `titularidad_acciones`
- `anotaciones_marginales`
- `expedientes_candidato`

### Tablas de planes de gobierno

- `plan_gobierno_runs`
- `candidato_plan_gobierno`
- `planes_gobierno`
- `planes_gobierno_dimensiones`
- `planes_gobierno_pdf_texto`

## 6) Estrategia de extraccion

1. Cargar procesos y seleccionar objetivo (por defecto `124 = EG 2026`).
2. Para cada tipo de eleccion:
- Ejecutar `candidato/avanzada` con `IdOrganizacionPolitica=0` (TODOS).
- Paginar hasta `totalPages`.
3. Por cada candidato:
- Guardar fila base.
- Obtener `hoja-vida`, `anotacion-marginal`, `expediente`.
- Normalizar y upsert en detalle.
4. Guardar JSON crudo por version para auditoria.

## 7) Manejo de CAPTCHA / anti-bot

El endpoint de busqueda requiere `googleToken` de reCAPTCHA enterprise.

Plan tecnico:

- Diseñar `TokenProvider` desacoplado.
- Implementar proveedor manual (token inyectado) para pruebas.
- Implementar proveedor con navegador automatizado para produccion.
- Si la API responde `CAPTCHA_REQUIRED` o `CAPTCHA_INVALID`, reintentar con nuevo token y registrar evento.

## 8) Calidad y observabilidad

- Logging estructurado por candidato y run.
- Contadores por:
- total candidatos leidos
- total con detalle completo
- total con error
- hashes de payload para detectar cambios.
- `run_id` en todas las inserciones.

## 9) Seguridad y cumplimiento

- Respetar robots/terminos de uso del portal.
- No exponer datos sensibles no publicos.
- Proveer fuente y timestamp de actualizacion por registro.
- Mantener llaves y secretos fuera del repo (`.env`).

## 10) Roadmap por fases

### Fase 1 - Fundacion (actual)

- Estructura de proyecto.
- Migracion SQL inicial.
- Cliente API JNE + CLI de ingesta MVP.

### Fase 2 - Estabilizacion

- Idempotencia completa.
- Reintentos robustos y control de concurrencia.
- Tests de contrato y validaciones.

### Fase 3 - API y UI

- API de consulta para frontend.
- Paginas de comparacion y filtros avanzados.
- Dashboard web inicial con graficos de ingresos, denuncias y universidades.
- Copilot web para preguntas directas a la base consolidada.

### Fase 4 - Copilot ciudadano

- Indices para recuperacion.
- Respuestas explicables con citas de campos.
- Preguntas guiadas por tema (sentencias, patrimonio, trayectoria).
- Extension del contexto para planes de gobierno y propuestas.

## 11) Riesgos y mitigaciones

- **Bloqueo anti-bot**: token provider + backoff + ejecucion programada.
- **Cambios de contrato API**: validaciones de esquema y alertas.
- **Datos incompletos temporales**: guardar `raw` + reprocesos incrementales.
- **Coste de consultas**: paginacion + lotes + caching de catalogos.

## 12) Criterios de salida del MVP

- Ingesta de EG 2026 ejecutable por comando.
- Datos persistidos en Supabase local.
- Tablas de sentencias y declaracion jurada pobladas cuando existan.
- Reporte final de run con metricas y errores.
