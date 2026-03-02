from __future__ import annotations

import hashlib
import io
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set

import httpx

from jne_ingest.config import AppConfig
from jne_ingest.jne_client import JNEClient
from jne_ingest.repository import PostgresRepository

try:
    from pypdf import PdfReader
except Exception:  # noqa: BLE001
    PdfReader = None


logger = logging.getLogger("jne_ingest.plan_gobierno_pipeline")


@dataclass
class PlanGobiernoMetrics:
    candidates_read: int = 0
    candidates_persisted: int = 0
    plans_resolved: int = 0
    pdf_texts_extracted: int = 0
    missing_inputs: int = 0
    errors_count: int = 0


@dataclass
class PdfTextExtractionResult:
    http_status: Optional[int]
    content_type: Optional[str]
    content_length_bytes: Optional[int]
    text_content: Optional[str]
    text_length: Optional[int]
    text_sha256: Optional[str]
    extraction_ok: bool
    extraction_error: Optional[str]


@dataclass
class PlanGobiernoPipeline:
    config: AppConfig
    client: JNEClient
    repository: PostgresRepository
    extract_pdf_text: bool = True
    max_candidates: Optional[int] = None
    _http: httpx.Client = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._http = httpx.Client(
            timeout=self.config.request_timeout_seconds,
            verify=self.config.verify_ssl,
        )
        if self.extract_pdf_text and PdfReader is None:
            raise RuntimeError(
                "No se encontro parser PDF. Instala dependencias con `pip install -r requirements.txt`."
            )

    def close(self) -> None:
        self._http.close()

    def run(self) -> PlanGobiernoMetrics:
        metrics = PlanGobiernoMetrics()
        run_id = self.repository.create_plan_gobierno_run(
            process_id=self.config.process_id,
            tipo_eleccion_id=self.config.tipo_eleccion_id,
        )
        logger.info(
            "Run plan de gobierno creado | run_id=%s process_id=%s tipo_eleccion_id=%s",
            run_id,
            self.config.process_id,
            self.config.tipo_eleccion_id,
        )

        status = "completed"
        processed_plan_ids: Set[int] = set()
        try:
            inputs = self.repository.list_plan_gobierno_candidate_inputs(
                process_id=self.config.process_id,
                tipo_eleccion_id=self.config.tipo_eleccion_id,
                limit=self.max_candidates,
            )
            logger.info("Candidatos cargados para plan de gobierno=%s", len(inputs))

            for row in inputs:
                metrics.candidates_read += 1
                self._process_candidate(
                    row=row,
                    run_id=run_id,
                    metrics=metrics,
                    processed_plan_ids=processed_plan_ids,
                )
        except Exception:  # noqa: BLE001
            status = "failed"
            metrics.errors_count += 1
            logger.exception("Fallo durante pipeline de plan de gobierno.")
            raise
        finally:
            self.repository.finish_plan_gobierno_run(
                run_id=run_id,
                status=status,
                candidates_read=metrics.candidates_read,
                candidates_persisted=metrics.candidates_persisted,
                plans_resolved=metrics.plans_resolved,
                pdf_texts_extracted=metrics.pdf_texts_extracted,
                errors_count=metrics.errors_count,
                metadata={
                    "missing_inputs": metrics.missing_inputs,
                    "extract_pdf_text": self.extract_pdf_text,
                    "max_candidates": self.max_candidates,
                },
            )
            logger.info("Run plan de gobierno finalizado | run_id=%s status=%s", run_id, status)

        return metrics

    def _process_candidate(
        self,
        *,
        row: Dict[str, Any],
        run_id,
        metrics: PlanGobiernoMetrics,
        processed_plan_ids: Set[int],
    ) -> None:
        id_hoja_vida = int(row["id_hoja_vida"])
        id_proceso_electoral = self._to_int_or_none(row.get("id_proceso_electoral")) or self.config.process_id
        id_tipo_eleccion = self._to_int_or_none(row.get("id_tipo_eleccion")) or self.config.tipo_eleccion_id
        id_organizacion_politica = self._to_int_or_none(row.get("id_organizacion_politica"))
        id_solicitud_lista = self._to_int_or_none(row.get("id_solicitud_lista"))

        if id_tipo_eleccion is None or id_organizacion_politica is None or id_solicitud_lista is None:
            metrics.missing_inputs += 1
            self.repository.upsert_candidato_plan_gobierno(
                run_id=run_id,
                id_hoja_vida=id_hoja_vida,
                id_proceso_electoral=id_proceso_electoral,
                id_tipo_eleccion=id_tipo_eleccion or 0,
                id_organizacion_politica=id_organizacion_politica,
                id_solicitud_lista=id_solicitud_lista,
                id_plan_gobierno=None,
                estado="missing_input",
                error_message=(
                    f"Faltan llaves para consulta: tipo={id_tipo_eleccion} "
                    f"org={id_organizacion_politica} solicitud={id_solicitud_lista}"
                ),
                payload_detalle_para_candidato=None,
            )
            metrics.candidates_persisted += 1
            return

        payload_detalle_para_candidato: Optional[Dict[str, Any]] = None
        id_plan_gobierno: Optional[int] = None
        estado = "ok"
        error_message: Optional[str] = None

        try:
            payload_detalle_para_candidato = self.client.get_plan_gobierno_detalle_para_candidato(
                process_id=id_proceso_electoral,
                tipo_eleccion_id=id_tipo_eleccion,
                organizacion_politica_id=id_organizacion_politica,
                solicitud_lista_id=id_solicitud_lista,
            )
            id_plan_gobierno = self._extract_plan_id(payload_detalle_para_candidato)
            if id_plan_gobierno is None:
                estado = "no_plan"
        except Exception as exc:  # noqa: BLE001
            metrics.errors_count += 1
            estado = "error"
            error_message = f"detalle_para_candidato_error: {exc.__class__.__name__}: {str(exc)[:200]}"
            logger.warning(
                "Error consultando detalle-para-candidato | id_hoja_vida=%s detalle=%s",
                id_hoja_vida,
                error_message,
            )

        if id_plan_gobierno is not None and id_plan_gobierno not in processed_plan_ids:
            payload_plan = self._get_plan_payload(
                id_plan_gobierno=id_plan_gobierno,
                fallback_payload=payload_detalle_para_candidato,
            )
            resolved_id = self.repository.upsert_plan_gobierno(
                run_id=run_id,
                payload=payload_plan,
                fallback_process_id=id_proceso_electoral,
                fallback_tipo_eleccion_id=id_tipo_eleccion,
                fallback_organizacion_politica_id=id_organizacion_politica,
            )
            if resolved_id is not None:
                self.repository.replace_plan_gobierno_dimensiones(
                    run_id=run_id,
                    id_plan_gobierno=resolved_id,
                    payload=payload_plan,
                )
                metrics.plans_resolved += 1
                if self.extract_pdf_text:
                    metrics.pdf_texts_extracted += self._extract_and_persist_pdf_texts(
                        id_plan_gobierno=resolved_id,
                        payload=payload_plan,
                    )
                processed_plan_ids.add(resolved_id)

        self.repository.upsert_candidato_plan_gobierno(
            run_id=run_id,
            id_hoja_vida=id_hoja_vida,
            id_proceso_electoral=id_proceso_electoral,
            id_tipo_eleccion=id_tipo_eleccion,
            id_organizacion_politica=id_organizacion_politica,
            id_solicitud_lista=id_solicitud_lista,
            id_plan_gobierno=id_plan_gobierno,
            estado=estado,
            error_message=error_message,
            payload_detalle_para_candidato=payload_detalle_para_candidato,
        )
        metrics.candidates_persisted += 1

    def _get_plan_payload(
        self,
        *,
        id_plan_gobierno: int,
        fallback_payload: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        try:
            payload = self.client.get_plan_gobierno_detalle(id_plan_gobierno)
            if isinstance(payload, dict) and isinstance(payload.get("datoGeneral"), dict):
                return payload
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "No se pudo obtener detalle de plan (%s). Se usa fallback. detalle=%s",
                id_plan_gobierno,
                str(exc)[:220],
            )
        return fallback_payload if isinstance(fallback_payload, dict) else {}

    def _extract_and_persist_pdf_texts(
        self,
        *,
        id_plan_gobierno: int,
        payload: Dict[str, Any],
    ) -> int:
        dato_general = payload.get("datoGeneral", {}) if isinstance(payload, dict) else {}
        if not isinstance(dato_general, dict):
            return 0

        extracted_ok = 0
        for tipo_archivo, key in (("completo", "txRutaCompleto"), ("resumen", "txRutaResumen")):
            source_url = str(dato_general.get(key) or "").strip()
            if not source_url:
                continue

            result = self._extract_pdf_text(source_url)
            self.repository.upsert_plan_gobierno_pdf_texto(
                id_plan_gobierno=id_plan_gobierno,
                tipo_archivo=tipo_archivo,
                source_url=source_url,
                http_status=result.http_status,
                content_type=result.content_type,
                content_length_bytes=result.content_length_bytes,
                text_content=result.text_content,
                text_length=result.text_length,
                text_sha256=result.text_sha256,
                extraction_ok=result.extraction_ok,
                extraction_error=result.extraction_error,
            )
            if result.extraction_ok:
                extracted_ok += 1
        return extracted_ok

    def _extract_pdf_text(self, source_url: str) -> PdfTextExtractionResult:
        try:
            response = self._http.get(source_url, follow_redirects=True)
        except Exception as exc:  # noqa: BLE001
            return PdfTextExtractionResult(
                http_status=None,
                content_type=None,
                content_length_bytes=None,
                text_content=None,
                text_length=None,
                text_sha256=None,
                extraction_ok=False,
                extraction_error=f"download_error: {exc.__class__.__name__}: {str(exc)[:220]}",
            )

        content = response.content or b""
        content_type = response.headers.get("content-type")
        content_length_bytes = len(content)
        if response.status_code >= 400:
            return PdfTextExtractionResult(
                http_status=response.status_code,
                content_type=content_type,
                content_length_bytes=content_length_bytes,
                text_content=None,
                text_length=None,
                text_sha256=None,
                extraction_ok=False,
                extraction_error=f"http_error_{response.status_code}",
            )

        if not content:
            return PdfTextExtractionResult(
                http_status=response.status_code,
                content_type=content_type,
                content_length_bytes=0,
                text_content=None,
                text_length=None,
                text_sha256=None,
                extraction_ok=False,
                extraction_error="pdf_empty",
            )

        try:
            reader = PdfReader(io.BytesIO(content))
            fragments = []
            for page in reader.pages:
                page_text = (page.extract_text() or "").strip()
                if page_text:
                    fragments.append(page_text)
            text_content = "\n\n".join(fragments).strip()
            if not text_content:
                return PdfTextExtractionResult(
                    http_status=response.status_code,
                    content_type=content_type,
                    content_length_bytes=content_length_bytes,
                    text_content=None,
                    text_length=None,
                    text_sha256=None,
                    extraction_ok=False,
                    extraction_error="pdf_text_empty",
                )
            text_sha256 = hashlib.sha256(text_content.encode("utf-8")).hexdigest()
            return PdfTextExtractionResult(
                http_status=response.status_code,
                content_type=content_type,
                content_length_bytes=content_length_bytes,
                text_content=text_content,
                text_length=len(text_content),
                text_sha256=text_sha256,
                extraction_ok=True,
                extraction_error=None,
            )
        except Exception as exc:  # noqa: BLE001
            return PdfTextExtractionResult(
                http_status=response.status_code,
                content_type=content_type,
                content_length_bytes=content_length_bytes,
                text_content=None,
                text_length=None,
                text_sha256=None,
                extraction_ok=False,
                extraction_error=f"extract_error: {exc.__class__.__name__}: {str(exc)[:220]}",
            )

    @staticmethod
    def _extract_plan_id(payload: Optional[Dict[str, Any]]) -> Optional[int]:
        if not isinstance(payload, dict):
            return None
        dato_general = payload.get("datoGeneral")
        if not isinstance(dato_general, dict):
            return None
        return PlanGobiernoPipeline._to_int_or_none(dato_general.get("idPlanGobierno"))

    @staticmethod
    def _to_int_or_none(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
