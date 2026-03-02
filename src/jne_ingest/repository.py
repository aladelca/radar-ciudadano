from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json


def _to_json_hash(payload: Dict[str, Any]) -> str:
    return str(hash(json.dumps(payload, sort_keys=True, ensure_ascii=True)))


class PostgresRepository:
    def __init__(self, dsn: str) -> None:
        self.conn = psycopg.connect(dsn, row_factory=dict_row, autocommit=True)

    def close(self) -> None:
        self.conn.close()

    def create_run(self, process_id: int, tipo_eleccion_id: Optional[int]) -> UUID:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                insert into jne.ingesta_runs (process_id, tipo_eleccion_id, status, started_at)
                values (%s, %s, 'running', now())
                returning id
                """,
                (process_id, tipo_eleccion_id),
            )
            row = cur.fetchone()
            if not row or "id" not in row:
                raise RuntimeError("No se pudo crear ingesta_run.")
            return row["id"]

    def finish_run(
        self,
        run_id: UUID,
        *,
        status: str,
        candidates_read: int,
        candidates_persisted: int,
        errors_count: int,
        metadata: Dict[str, Any],
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                update jne.ingesta_runs
                set status = %s,
                    finished_at = now(),
                    candidates_read = %s,
                    candidates_persisted = %s,
                    errors_count = %s,
                    metadata = %s
                where id = %s
                """,
                (
                    status,
                    candidates_read,
                    candidates_persisted,
                    errors_count,
                    Json(metadata),
                    run_id,
                ),
            )

    def upsert_candidato(
        self,
        run_id: UUID,
        row: Dict[str, Any],
        process_id: int,
        tipo_eleccion_id: int,
        tipo_eleccion_nombre: Optional[str] = None,
    ) -> None:
        id_hoja_vida = int(row["idHojaVida"])
        with self.conn.cursor() as cur:
            cur.execute(
                """
                insert into jne.candidatos (
                    id_hoja_vida,
                    run_id,
                    id_proceso_electoral,
                    id_tipo_eleccion,
                    id_organizacion_politica,
                    organizacion_politica,
                    numero_documento,
                    nombre_completo,
                    cargo,
                    estado,
                    numero_candidato,
                    postula_departamento,
                    postula_provincia,
                    postula_distrito,
                    tx_guid_archivo_origen,
                    tx_guid_foto,
                    tx_nombre_foto,
                    raw_payload,
                    updated_at
                )
                values (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now()
                )
                on conflict (id_hoja_vida) do update set
                    run_id = excluded.run_id,
                    id_proceso_electoral = excluded.id_proceso_electoral,
                    id_tipo_eleccion = excluded.id_tipo_eleccion,
                    id_organizacion_politica = excluded.id_organizacion_politica,
                    organizacion_politica = excluded.organizacion_politica,
                    numero_documento = excluded.numero_documento,
                    nombre_completo = excluded.nombre_completo,
                    cargo = excluded.cargo,
                    estado = excluded.estado,
                    numero_candidato = excluded.numero_candidato,
                    postula_departamento = excluded.postula_departamento,
                    postula_provincia = excluded.postula_provincia,
                    postula_distrito = excluded.postula_distrito,
                    tx_guid_archivo_origen = excluded.tx_guid_archivo_origen,
                    tx_guid_foto = excluded.tx_guid_foto,
                    tx_nombre_foto = excluded.tx_nombre_foto,
                    raw_payload = excluded.raw_payload,
                    updated_at = now()
                """,
                (
                    id_hoja_vida,
                    run_id,
                    process_id,
                    tipo_eleccion_id,
                    row.get("idOrganizacionPolitica"),
                    row.get("organizacionPolitica"),
                    row.get("numeroDocumento"),
                    row.get("nombreCompleto"),
                    row.get("cargo"),
                    row.get("estado"),
                    row.get("numeroCandidato"),
                    row.get("postulaDepartamento"),
                    row.get("postulaProvincia"),
                    row.get("postulaDistrito"),
                    row.get("txGuidArchivoOrigen"),
                    row.get("txGuidFoto"),
                    row.get("txNombre"),
                    Json(row),
                ),
            )
            segmento_postulacion = self._infer_segmento_postulacion(
                tipo_eleccion_id=tipo_eleccion_id,
                tipo_eleccion_nombre=tipo_eleccion_nombre,
            )
            cur.execute(
                """
                insert into jne.candidatos_postulaciones (
                    id_hoja_vida,
                    id_proceso_electoral,
                    id_tipo_eleccion,
                    tipo_eleccion,
                    segmento_postulacion,
                    run_id,
                    id_organizacion_politica,
                    organizacion_politica,
                    numero_documento,
                    nombre_completo,
                    cargo,
                    estado,
                    numero_candidato,
                    updated_at
                )
                values (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
                )
                on conflict (id_hoja_vida, id_proceso_electoral, id_tipo_eleccion) do update set
                    tipo_eleccion = excluded.tipo_eleccion,
                    segmento_postulacion = excluded.segmento_postulacion,
                    run_id = excluded.run_id,
                    id_organizacion_politica = excluded.id_organizacion_politica,
                    organizacion_politica = excluded.organizacion_politica,
                    numero_documento = excluded.numero_documento,
                    nombre_completo = excluded.nombre_completo,
                    cargo = excluded.cargo,
                    estado = excluded.estado,
                    numero_candidato = excluded.numero_candidato,
                    updated_at = now()
                """,
                (
                    id_hoja_vida,
                    process_id,
                    tipo_eleccion_id,
                    tipo_eleccion_nombre,
                    segmento_postulacion,
                    run_id,
                    row.get("idOrganizacionPolitica"),
                    row.get("organizacionPolitica"),
                    row.get("numeroDocumento"),
                    row.get("nombreCompleto"),
                    row.get("cargo"),
                    row.get("estado"),
                    row.get("numeroCandidato"),
                ),
            )

    def upsert_hoja_vida_raw(self, run_id: UUID, id_hoja_vida: int, payload: Dict[str, Any]) -> None:
        payload_hash = _to_json_hash(payload)
        with self.conn.cursor() as cur:
            cur.execute(
                """
                insert into jne.hoja_vida_raw (
                    id_hoja_vida,
                    run_id,
                    payload_hash,
                    payload,
                    fetched_at
                )
                values (%s,%s,%s,%s,now())
                on conflict (id_hoja_vida) do update set
                    run_id = excluded.run_id,
                    payload_hash = excluded.payload_hash,
                    payload = excluded.payload,
                    fetched_at = now()
                """,
                (id_hoja_vida, run_id, payload_hash, Json(payload)),
            )

    def upsert_catalog_procesos(self, procesos: Iterable[Dict[str, Any]]) -> None:
        with self.conn.cursor() as cur:
            for proceso in procesos:
                process_id = proceso.get("idProcesoElectoral")
                if process_id is None:
                    continue
                cur.execute(
                    """
                    insert into jne.catalog_procesos_electorales (
                        id_proceso_electoral,
                        nombre_proceso_electoral,
                        siglas_proceso_electoral,
                        id_tipo_proceso,
                        payload,
                        updated_at
                    )
                    values (%s,%s,%s,%s,%s,now())
                    on conflict (id_proceso_electoral) do update set
                        nombre_proceso_electoral = excluded.nombre_proceso_electoral,
                        siglas_proceso_electoral = excluded.siglas_proceso_electoral,
                        id_tipo_proceso = excluded.id_tipo_proceso,
                        payload = excluded.payload,
                        updated_at = now()
                    """,
                    (
                        int(process_id),
                        proceso.get("nombreProcesoElectoral"),
                        proceso.get("siglasProcesoElectoral"),
                        proceso.get("idTipoProceso"),
                        Json(proceso),
                    ),
                )

    def upsert_catalog_tipos(self, process_id: int, tipos: Iterable[Dict[str, Any]]) -> None:
        with self.conn.cursor() as cur:
            for tipo in tipos:
                tipo_id = tipo.get("idTipoEleccion")
                if tipo_id is None:
                    continue
                cur.execute(
                    """
                    insert into jne.catalog_tipos_eleccion (
                        id_proceso_electoral,
                        id_tipo_eleccion,
                        tipo_eleccion,
                        payload,
                        updated_at
                    )
                    values (%s,%s,%s,%s,now())
                    on conflict (id_proceso_electoral, id_tipo_eleccion) do update set
                        tipo_eleccion = excluded.tipo_eleccion,
                        payload = excluded.payload,
                        updated_at = now()
                    """,
                    (
                        process_id,
                        int(tipo_id),
                        tipo.get("tipoEleccion"),
                        Json(tipo),
                    ),
                )

    def upsert_catalog_organizaciones(self, process_id: int, organizaciones: Iterable[Dict[str, Any]]) -> None:
        with self.conn.cursor() as cur:
            for org in organizaciones:
                org_id = org.get("idOrganizacionPolitica")
                if org_id is None:
                    continue
                cur.execute(
                    """
                    insert into jne.catalog_organizaciones_politicas (
                        id_proceso_electoral,
                        id_organizacion_politica,
                        organizacion_politica,
                        payload,
                        updated_at
                    )
                    values (%s,%s,%s,%s,now())
                    on conflict (id_proceso_electoral, id_organizacion_politica) do update set
                        organizacion_politica = excluded.organizacion_politica,
                        payload = excluded.payload,
                        updated_at = now()
                    """,
                    (
                        process_id,
                        int(org_id),
                        org.get("organizacionPolitica"),
                        Json(org),
                    ),
                )

    def upsert_hoja_vida_secciones_raw(
        self,
        run_id: UUID,
        id_hoja_vida: int,
        payload: Dict[str, Any],
    ) -> None:
        sections: List[tuple[str, Any]] = []
        if isinstance(payload, dict):
            sections = list(payload.items())
        else:
            sections = [("__root__", payload)]

        with self.conn.cursor() as cur:
            for section_name, section_payload in sections:
                cur.execute(
                    """
                    insert into jne.hoja_vida_secciones_raw (
                        id_hoja_vida,
                        section_name,
                        run_id,
                        payload,
                        fetched_at
                    )
                    values (%s,%s,%s,%s,now())
                    on conflict (id_hoja_vida, section_name) do update set
                        run_id = excluded.run_id,
                        payload = excluded.payload,
                        fetched_at = now()
                    """,
                    (
                        id_hoja_vida,
                        str(section_name),
                        run_id,
                        Json(section_payload),
                    ),
                )

    def upsert_anotaciones_raw(self, run_id: UUID, id_hoja_vida: int, payload: Any) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                insert into jne.anotaciones_raw (
                    id_hoja_vida,
                    run_id,
                    payload,
                    fetched_at
                )
                values (%s,%s,%s,now())
                on conflict (id_hoja_vida) do update set
                    run_id = excluded.run_id,
                    payload = excluded.payload,
                    fetched_at = now()
                """,
                (
                    id_hoja_vida,
                    run_id,
                    Json(payload),
                ),
            )

    def upsert_expedientes_raw(self, run_id: UUID, id_hoja_vida: int, payload: Any) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                insert into jne.expedientes_raw (
                    id_hoja_vida,
                    run_id,
                    payload,
                    fetched_at
                )
                values (%s,%s,%s,now())
                on conflict (id_hoja_vida) do update set
                    run_id = excluded.run_id,
                    payload = excluded.payload,
                    fetched_at = now()
                """,
                (
                    id_hoja_vida,
                    run_id,
                    Json(payload),
                ),
            )

    def upsert_anotaciones(self, run_id: UUID, id_hoja_vida: int, payload: Any) -> None:
        rows: Iterable[Dict[str, Any]] = []
        if isinstance(payload, dict):
            data = payload.get("data", [])
            rows = data if isinstance(data, list) else []
        elif isinstance(payload, list):
            rows = payload

        with self.conn.cursor() as cur:
            for idx, item in enumerate(rows, start=1):
                anotacion_id = item.get("idAnotacionMarginal", idx)
                cur.execute(
                    """
                    insert into jne.anotaciones_marginales (
                        id_anotacion_marginal,
                        id_hoja_vida,
                        run_id,
                        item_index,
                        payload,
                        fetched_at
                    )
                    values (%s,%s,%s,%s,%s,now())
                    on conflict (id_anotacion_marginal) do update set
                        id_hoja_vida = excluded.id_hoja_vida,
                        run_id = excluded.run_id,
                        item_index = excluded.item_index,
                        payload = excluded.payload,
                        fetched_at = now()
                    """,
                    (anotacion_id, id_hoja_vida, run_id, idx, Json(item)),
                )

    def upsert_expedientes(self, run_id: UUID, id_hoja_vida: int, payload: Any) -> None:
        rows: Iterable[Dict[str, Any]] = []
        if isinstance(payload, dict):
            data = payload.get("data", [])
            rows = data if isinstance(data, list) else []
        elif isinstance(payload, list):
            rows = payload

        with self.conn.cursor() as cur:
            for idx, item in enumerate(rows, start=1):
                expediente_id = item.get("idExpediente", f"{id_hoja_vida}-{idx}")
                cur.execute(
                    """
                    insert into jne.expedientes_candidato (
                        id_expediente,
                        id_hoja_vida,
                        run_id,
                        item_index,
                        payload,
                        fetched_at
                    )
                    values (%s,%s,%s,%s,%s,now())
                    on conflict (id_expediente) do update set
                        id_hoja_vida = excluded.id_hoja_vida,
                        run_id = excluded.run_id,
                        item_index = excluded.item_index,
                        payload = excluded.payload,
                        fetched_at = now()
                    """,
                    (str(expediente_id), id_hoja_vida, run_id, idx, Json(item)),
                )

    def upsert_hoja_vida_sections(self, run_id: UUID, id_hoja_vida: int, payload: Dict[str, Any]) -> None:
        sentencia_penal = payload.get("sentenciaPenal", []) or []
        sentencia_obliga = payload.get("sentenciaObliga", []) or []
        declaracion = payload.get("declaracionJurada", {}) or {}

        self._upsert_items(
            table_name="sentencias_penales",
            id_column="id_sentencia_penal",
            generated_prefix="pen",
            run_id=run_id,
            id_hoja_vida=id_hoja_vida,
            items=sentencia_penal,
        )
        self._upsert_items(
            table_name="sentencias_obligaciones",
            id_column="id_sentencia_obligacion",
            generated_prefix="obl",
            run_id=run_id,
            id_hoja_vida=id_hoja_vida,
            items=sentencia_obliga,
        )
        self._upsert_items(
            table_name="declaracion_ingresos",
            id_column="id_declaracion_ingreso",
            generated_prefix="ing",
            run_id=run_id,
            id_hoja_vida=id_hoja_vida,
            items=declaracion.get("ingreso", []) or [],
        )
        self._upsert_items(
            table_name="bienes_inmuebles",
            id_column="id_bien_inmueble",
            generated_prefix="inm",
            run_id=run_id,
            id_hoja_vida=id_hoja_vida,
            items=declaracion.get("bienInmueble", []) or [],
        )
        self._upsert_items(
            table_name="bienes_muebles",
            id_column="id_bien_mueble",
            generated_prefix="mue",
            run_id=run_id,
            id_hoja_vida=id_hoja_vida,
            items=declaracion.get("bienMueble", []) or [],
        )
        self._upsert_items(
            table_name="otros_bienes_muebles",
            id_column="id_otro_bien_mueble",
            generated_prefix="omue",
            run_id=run_id,
            id_hoja_vida=id_hoja_vida,
            items=declaracion.get("otroMueble", []) or [],
        )
        self._upsert_items(
            table_name="titularidad_acciones",
            id_column="id_titularidad_accion",
            generated_prefix="tit",
            run_id=run_id,
            id_hoja_vida=id_hoja_vida,
            items=declaracion.get("titularidad", []) or [],
        )

    def create_plan_gobierno_run(
        self,
        *,
        process_id: int,
        tipo_eleccion_id: Optional[int],
    ) -> UUID:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                insert into jne.plan_gobierno_runs (
                    process_id,
                    tipo_eleccion_id,
                    status,
                    started_at
                )
                values (%s, %s, 'running', now())
                returning id
                """,
                (process_id, tipo_eleccion_id),
            )
            row = cur.fetchone()
            if not row or "id" not in row:
                raise RuntimeError("No se pudo crear plan_gobierno_run.")
            return row["id"]

    def finish_plan_gobierno_run(
        self,
        run_id: UUID,
        *,
        status: str,
        candidates_read: int,
        candidates_persisted: int,
        plans_resolved: int,
        pdf_texts_extracted: int,
        errors_count: int,
        metadata: Dict[str, Any],
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                update jne.plan_gobierno_runs
                set status = %s,
                    finished_at = now(),
                    candidates_read = %s,
                    candidates_persisted = %s,
                    plans_resolved = %s,
                    pdf_texts_extracted = %s,
                    errors_count = %s,
                    metadata = %s
                where id = %s
                """,
                (
                    status,
                    candidates_read,
                    candidates_persisted,
                    plans_resolved,
                    pdf_texts_extracted,
                    errors_count,
                    Json(metadata),
                    run_id,
                ),
            )

    def list_plan_gobierno_candidate_inputs(
        self,
        *,
        process_id: int,
        tipo_eleccion_id: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        query = """
            select
                c.id_hoja_vida,
                c.id_proceso_electoral,
                c.id_tipo_eleccion,
                c.id_organizacion_politica,
                case
                    when coalesce(h.payload->'datoGeneral'->>'idSolicitudLista', '') ~ '^[0-9]+(\\.0+)?$'
                        then split_part(h.payload->'datoGeneral'->>'idSolicitudLista', '.', 1)::bigint
                    else null
                end as id_solicitud_lista
            from jne.candidatos c
            left join jne.hoja_vida_raw h
                on h.id_hoja_vida = c.id_hoja_vida
            where c.id_proceso_electoral = %s
        """
        params: List[Any] = [process_id]
        if tipo_eleccion_id is not None:
            query += " and c.id_tipo_eleccion = %s"
            params.append(tipo_eleccion_id)
        query += " order by c.id_hoja_vida"
        if limit is not None and limit > 0:
            query += " limit %s"
            params.append(limit)

        with self.conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        return [dict(row) for row in rows]

    def upsert_candidato_plan_gobierno(
        self,
        *,
        run_id: UUID,
        id_hoja_vida: int,
        id_proceso_electoral: int,
        id_tipo_eleccion: int,
        id_organizacion_politica: Optional[int],
        id_solicitud_lista: Optional[int],
        id_plan_gobierno: Optional[int],
        estado: str,
        error_message: Optional[str],
        payload_detalle_para_candidato: Optional[Dict[str, Any]],
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                insert into jne.candidato_plan_gobierno (
                    id_hoja_vida,
                    run_id,
                    id_proceso_electoral,
                    id_tipo_eleccion,
                    id_organizacion_politica,
                    id_solicitud_lista,
                    id_plan_gobierno,
                    estado,
                    error_message,
                    payload_detalle_para_candidato,
                    fetched_at
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                on conflict (id_hoja_vida) do update set
                    run_id = excluded.run_id,
                    id_proceso_electoral = excluded.id_proceso_electoral,
                    id_tipo_eleccion = excluded.id_tipo_eleccion,
                    id_organizacion_politica = excluded.id_organizacion_politica,
                    id_solicitud_lista = excluded.id_solicitud_lista,
                    id_plan_gobierno = excluded.id_plan_gobierno,
                    estado = excluded.estado,
                    error_message = excluded.error_message,
                    payload_detalle_para_candidato = excluded.payload_detalle_para_candidato,
                    fetched_at = now()
                """,
                (
                    id_hoja_vida,
                    run_id,
                    id_proceso_electoral,
                    id_tipo_eleccion,
                    id_organizacion_politica,
                    id_solicitud_lista,
                    id_plan_gobierno,
                    estado,
                    error_message,
                    Json(payload_detalle_para_candidato) if payload_detalle_para_candidato is not None else None,
                ),
            )

    def upsert_plan_gobierno(
        self,
        *,
        run_id: UUID,
        payload: Dict[str, Any],
        fallback_process_id: Optional[int] = None,
        fallback_tipo_eleccion_id: Optional[int] = None,
        fallback_organizacion_politica_id: Optional[int] = None,
    ) -> Optional[int]:
        if not isinstance(payload, dict):
            return None
        dato_general = payload.get("datoGeneral") or {}
        if not isinstance(dato_general, dict):
            return None

        id_plan_gobierno = self._to_int_or_none(dato_general.get("idPlanGobierno"))
        if id_plan_gobierno is None:
            return None

        id_proceso_electoral = self._pick_positive_int(
            dato_general.get("idProcesoElectoral"),
            fallback_process_id,
        )
        id_tipo_eleccion = self._pick_positive_int(
            dato_general.get("idTipoEleccion"),
            fallback_tipo_eleccion_id,
        )
        id_organizacion_politica = self._pick_positive_int(
            dato_general.get("idOrganizacionPolitica"),
            fallback_organizacion_politica_id,
        )

        with self.conn.cursor() as cur:
            cur.execute(
                """
                insert into jne.planes_gobierno (
                    id_plan_gobierno,
                    run_id,
                    id_proceso_electoral,
                    id_tipo_eleccion,
                    tipo_eleccion,
                    id_organizacion_politica,
                    organizacion_politica,
                    tipo_plan,
                    id_jurado_electoral,
                    jurado_electoral,
                    cod_expediente_ext,
                    url_completo,
                    url_resumen,
                    payload,
                    updated_at
                )
                values (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
                )
                on conflict (id_plan_gobierno) do update set
                    run_id = excluded.run_id,
                    id_proceso_electoral = excluded.id_proceso_electoral,
                    id_tipo_eleccion = excluded.id_tipo_eleccion,
                    tipo_eleccion = excluded.tipo_eleccion,
                    id_organizacion_politica = excluded.id_organizacion_politica,
                    organizacion_politica = excluded.organizacion_politica,
                    tipo_plan = excluded.tipo_plan,
                    id_jurado_electoral = excluded.id_jurado_electoral,
                    jurado_electoral = excluded.jurado_electoral,
                    cod_expediente_ext = excluded.cod_expediente_ext,
                    url_completo = excluded.url_completo,
                    url_resumen = excluded.url_resumen,
                    payload = excluded.payload,
                    updated_at = now()
                """,
                (
                    id_plan_gobierno,
                    run_id,
                    id_proceso_electoral,
                    id_tipo_eleccion,
                    dato_general.get("txTipoEleccion"),
                    id_organizacion_politica,
                    dato_general.get("txOrganizacionPolitica"),
                    dato_general.get("txTipoPlan"),
                    self._to_int_or_none(dato_general.get("idJuradoElectoral")),
                    dato_general.get("juradoElectoral"),
                    dato_general.get("txCodExpedienteExt"),
                    dato_general.get("txRutaCompleto"),
                    dato_general.get("txRutaResumen"),
                    Json(payload),
                ),
            )
        return id_plan_gobierno

    def replace_plan_gobierno_dimensiones(
        self,
        *,
        run_id: UUID,
        id_plan_gobierno: int,
        payload: Dict[str, Any],
    ) -> int:
        if not isinstance(payload, dict):
            return 0

        dimension_keys = [
            ("dimensionSocial", "social"),
            ("dimensionEconomica", "economica"),
            ("dimensionAmbiental", "ambiental"),
            ("dimensionInstitucional", "institucional"),
            ("dimensionPropuesta", "propuesta"),
        ]
        inserted = 0

        with self.conn.cursor() as cur:
            cur.execute("delete from jne.planes_gobierno_dimensiones where id_plan_gobierno = %s", (id_plan_gobierno,))
            for source_key, dimension_name in dimension_keys:
                rows = payload.get(source_key, [])
                if not isinstance(rows, list):
                    continue
                for idx, item in enumerate(rows, start=1):
                    if not isinstance(item, dict):
                        continue
                    dimension_item_id = self._to_int_or_none(item.get("idPlanGobDimension"))
                    plan_dimension_id = (
                        str(dimension_item_id)
                        if dimension_item_id is not None
                        else f"{id_plan_gobierno}-{dimension_name}-{idx}"
                    )
                    cur.execute(
                        """
                        insert into jne.planes_gobierno_dimensiones (
                            id_plan_dimension,
                            id_plan_gobierno,
                            run_id,
                            id_plan_gob_dimension,
                            dimension,
                            item_index,
                            problema,
                            objetivo,
                            indicador,
                            meta,
                            porcentaje,
                            payload,
                            updated_at
                        )
                        values (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
                        )
                        on conflict (id_plan_dimension) do update set
                            run_id = excluded.run_id,
                            id_plan_gob_dimension = excluded.id_plan_gob_dimension,
                            dimension = excluded.dimension,
                            item_index = excluded.item_index,
                            problema = excluded.problema,
                            objetivo = excluded.objetivo,
                            indicador = excluded.indicador,
                            meta = excluded.meta,
                            porcentaje = excluded.porcentaje,
                            payload = excluded.payload,
                            updated_at = now()
                        """,
                        (
                            plan_dimension_id,
                            id_plan_gobierno,
                            run_id,
                            dimension_item_id,
                            dimension_name,
                            idx,
                            item.get("txPgProblema"),
                            item.get("txPgObjetivo"),
                            item.get("txPgIndicador"),
                            item.get("txPgMeta"),
                            self._to_float_or_none(item.get("nuPorcentaje")),
                            Json(item),
                        ),
                    )
                    inserted += 1
        return inserted

    def upsert_plan_gobierno_pdf_texto(
        self,
        *,
        id_plan_gobierno: int,
        tipo_archivo: str,
        source_url: str,
        http_status: Optional[int],
        content_type: Optional[str],
        content_length_bytes: Optional[int],
        text_content: Optional[str],
        text_length: Optional[int],
        text_sha256: Optional[str],
        extraction_ok: bool,
        extraction_error: Optional[str],
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                insert into jne.planes_gobierno_pdf_texto (
                    id_plan_gobierno,
                    tipo_archivo,
                    source_url,
                    http_status,
                    content_type,
                    content_length_bytes,
                    text_content,
                    text_length,
                    text_sha256,
                    extraction_ok,
                    extraction_error,
                    extracted_at
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                on conflict (id_plan_gobierno, tipo_archivo) do update set
                    source_url = excluded.source_url,
                    http_status = excluded.http_status,
                    content_type = excluded.content_type,
                    content_length_bytes = excluded.content_length_bytes,
                    text_content = excluded.text_content,
                    text_length = excluded.text_length,
                    text_sha256 = excluded.text_sha256,
                    extraction_ok = excluded.extraction_ok,
                    extraction_error = excluded.extraction_error,
                    extracted_at = now()
                """,
                (
                    id_plan_gobierno,
                    tipo_archivo,
                    source_url,
                    http_status,
                    content_type,
                    content_length_bytes,
                    text_content,
                    text_length,
                    text_sha256,
                    extraction_ok,
                    extraction_error,
                ),
            )

    def create_instagram_run(
        self,
        *,
        mode: str,
        id_hoja_vida: Optional[int],
        username: Optional[str],
    ) -> UUID:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                insert into jne.instagram_ingesta_runs (
                    id_hoja_vida,
                    username,
                    mode,
                    status,
                    started_at
                )
                values (%s, %s, %s, 'running', now())
                returning id
                """,
                (
                    id_hoja_vida,
                    self._normalize_instagram_username(username) if username else None,
                    mode,
                ),
            )
            row = cur.fetchone()
            if not row or "id" not in row:
                raise RuntimeError("No se pudo crear instagram_ingesta_run.")
            return row["id"]

    def finish_instagram_run(
        self,
        run_id: UUID,
        *,
        status: str,
        metrics: Dict[str, Any],
        error_message: Optional[str] = None,
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                update jne.instagram_ingesta_runs
                set status = %s,
                    finished_at = now(),
                    metrics = %s,
                    error_message = %s
                where id = %s
                """,
                (
                    status,
                    Json(metrics),
                    error_message,
                    run_id,
                ),
            )

    def upsert_instagram_account(
        self,
        *,
        id_hoja_vida: int,
        username: str,
        source: str = "manual",
        is_oficial: bool = False,
        is_public: Optional[bool] = None,
        profile_url: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> Dict[str, Any]:
        normalized_username = self._normalize_instagram_username(username)
        if not normalized_username:
            raise ValueError("username de Instagram no puede ser vacio.")
        resolved_profile_url = (profile_url or f"https://www.instagram.com/{normalized_username}/").strip()

        with self.conn.cursor() as cur:
            cur.execute(
                """
                insert into jne.candidato_redes_sociales (
                    id_hoja_vida,
                    plataforma,
                    username,
                    profile_url,
                    source,
                    is_oficial,
                    is_public,
                    notes,
                    created_at,
                    updated_at
                )
                values (%s, 'instagram', %s, %s, %s, %s, %s, %s, now(), now())
                on conflict (id_hoja_vida, plataforma, username) do update set
                    profile_url = excluded.profile_url,
                    source = excluded.source,
                    is_oficial = excluded.is_oficial,
                    is_public = excluded.is_public,
                    notes = excluded.notes,
                    updated_at = now()
                returning
                    id,
                    id_hoja_vida,
                    plataforma,
                    username,
                    profile_url,
                    source,
                    is_oficial,
                    is_public,
                    notes,
                    created_at,
                    updated_at
                """,
                (
                    id_hoja_vida,
                    normalized_username,
                    resolved_profile_url,
                    source,
                    is_oficial,
                    is_public,
                    notes,
                ),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError("No se pudo upsert de cuenta Instagram.")
            return dict(row)

    def insert_instagram_profile_snapshot(
        self,
        *,
        run_id: Optional[UUID],
        id_hoja_vida: int,
        username: str,
        payload: Dict[str, Any],
    ) -> None:
        normalized_username = self._normalize_instagram_username(username)
        with self.conn.cursor() as cur:
            cur.execute(
                """
                insert into jne.instagram_profiles_snapshot (
                    run_id,
                    id_hoja_vida,
                    username,
                    ig_user_id,
                    account_type,
                    display_name,
                    biography,
                    website,
                    profile_picture_url,
                    followers_count,
                    follows_count,
                    media_count,
                    payload,
                    captured_at
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                """,
                (
                    run_id,
                    id_hoja_vida,
                    normalized_username,
                    payload.get("id"),
                    payload.get("account_type"),
                    payload.get("name"),
                    payload.get("biography"),
                    payload.get("website"),
                    payload.get("profile_picture_url"),
                    self._to_int_or_none(payload.get("followers_count")),
                    self._to_int_or_none(payload.get("follows_count")),
                    self._to_int_or_none(payload.get("media_count")),
                    Json(payload),
                ),
            )

    def insert_instagram_media_snapshots(
        self,
        *,
        run_id: Optional[UUID],
        id_hoja_vida: int,
        username: str,
        items: Iterable[Dict[str, Any]],
    ) -> int:
        normalized_username = self._normalize_instagram_username(username)
        inserted = 0
        with self.conn.cursor() as cur:
            for item in items:
                media_id = item.get("id")
                if not media_id:
                    continue
                cur.execute(
                    """
                    insert into jne.instagram_media_snapshot (
                        run_id,
                        id_hoja_vida,
                        username,
                        media_id,
                        media_type,
                        media_product_type,
                        caption,
                        permalink,
                        media_url,
                        thumbnail_url,
                        timestamp_utc,
                        comments_count,
                        like_count,
                        view_count,
                        payload,
                        captured_at
                    )
                    values (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
                    )
                    """,
                    (
                        run_id,
                        id_hoja_vida,
                        normalized_username,
                        str(media_id),
                        item.get("media_type"),
                        item.get("media_product_type"),
                        item.get("caption"),
                        item.get("permalink"),
                        item.get("media_url"),
                        item.get("thumbnail_url"),
                        item.get("timestamp"),
                        self._to_int_or_none(item.get("comments_count")),
                        self._to_int_or_none(item.get("like_count")),
                        self._to_int_or_none(item.get("view_count")),
                        Json(item),
                    ),
                )
                inserted += 1
        return inserted

    def _upsert_items(
        self,
        *,
        table_name: str,
        id_column: str,
        generated_prefix: str,
        run_id: UUID,
        id_hoja_vida: int,
        items: Iterable[Dict[str, Any]],
    ) -> None:
        with self.conn.cursor() as cur:
            for idx, item in enumerate(items, start=1):
                record_id = item.get(id_column) or f"{generated_prefix}-{id_hoja_vida}-{idx}"
                cur.execute(
                    f"""
                    insert into jne.{table_name} (
                        {id_column},
                        id_hoja_vida,
                        run_id,
                        item_index,
                        payload,
                        fetched_at
                    )
                    values (%s,%s,%s,%s,%s,now())
                    on conflict ({id_column}) do update set
                        id_hoja_vida = excluded.id_hoja_vida,
                        run_id = excluded.run_id,
                        item_index = excluded.item_index,
                        payload = excluded.payload,
                        fetched_at = now()
                    """,
                    (str(record_id), id_hoja_vida, run_id, idx, Json(item)),
                )

    @staticmethod
    def _normalize_instagram_username(username: str) -> str:
        normalized = username.strip()
        if normalized.startswith("@"):
            normalized = normalized[1:]
        return normalized.strip().lower()

    @staticmethod
    def _to_int_or_none(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_float_or_none(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _pick_positive_int(cls, primary: Any, fallback: Optional[int]) -> Optional[int]:
        primary_int = cls._to_int_or_none(primary)
        if primary_int is not None and primary_int > 0:
            return primary_int
        return fallback

    @staticmethod
    def _infer_segmento_postulacion(
        *,
        tipo_eleccion_id: int,
        tipo_eleccion_nombre: Optional[str],
    ) -> str:
        name_upper = str(tipo_eleccion_nombre or "").strip().upper()
        if tipo_eleccion_id == 1 or "PRESIDENCIAL" in name_upper:
            return "PRESIDENCIAL"
        if tipo_eleccion_id == 15 or "DIPUTAD" in name_upper:
            return "DIPUTADOS"
        if tipo_eleccion_id in {20, 21} or "SENADOR" in name_upper:
            return "SENADO"
        return "OTROS"
