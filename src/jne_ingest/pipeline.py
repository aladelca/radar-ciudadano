from __future__ import annotations

import base64
from dataclasses import dataclass
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
from typing import List, Optional

from jne_ingest.browser_search_provider import PlaywrightAdvancedSearchProvider
from jne_ingest.config import AppConfig
from jne_ingest.jne_client import CaptchaRequiredError, JNEClient
from jne_ingest.models import CandidateFilter, SearchMetrics
from jne_ingest.repository import PostgresRepository
from jne_ingest.token_provider import RecaptchaTokenProvider


logger = logging.getLogger("jne_ingest.pipeline")


@dataclass
class IngestionPipeline:
    config: AppConfig
    client: JNEClient
    token_provider: RecaptchaTokenProvider
    browser_search_provider: Optional[PlaywrightAdvancedSearchProvider] = None
    repository: Optional[PostgresRepository] = None

    def run(self) -> SearchMetrics:
        metrics = SearchMetrics()
        logger.info(
            "Resolviendo catalogos | process_id=%s",
            self.config.process_id,
        )
        procesos = self.client.get_procesos_electorales()
        tipos = self.client.get_tipos_eleccion(self.config.process_id)
        organizaciones = self.client.get_organizaciones_politicas(self.config.process_id)
        tipo_nombre_by_id = {
            int(t["idTipoEleccion"]): str(t.get("tipoEleccion", "")).strip()
            for t in tipos
            if t.get("idTipoEleccion") is not None
        }
        logger.info(
            "Catalogos cargados | procesos=%s tipos=%s organizaciones=%s",
            len(procesos),
            len(tipos),
            len(organizaciones),
        )
        tipo_ids = self._resolve_tipos_eleccion(tipos)
        logger.info("Tipos de eleccion seleccionados=%s", tipo_ids)
        run_id = None
        run_tipo = tipo_ids[0] if len(tipo_ids) == 1 else None
        if self.repository:
            run_id = self.repository.create_run(
                process_id=self.config.process_id,
                tipo_eleccion_id=run_tipo,
            )
            logger.info("Run creado | run_id=%s", run_id)
            self.repository.upsert_catalog_procesos(procesos)
            self.repository.upsert_catalog_tipos(self.config.process_id, tipos)
            self.repository.upsert_catalog_organizaciones(self.config.process_id, organizaciones)

        status = "completed"
        try:
            for tipo_id in tipo_ids:
                metrics.tipos_procesados.append(tipo_id)
                logger.info("Iniciando ingesta por tipo_eleccion_id=%s", tipo_id)
                self._ingest_tipo_eleccion(
                    tipo_id,
                    tipo_nombre_by_id.get(tipo_id),
                    metrics,
                    run_id,
                )
                logger.info(
                    "Tipo finalizado | tipo_eleccion_id=%s candidatos_leidos=%s persistidos=%s paginas=%s errores=%s",
                    tipo_id,
                    metrics.candidates_read,
                    metrics.candidates_persisted,
                    metrics.pages_read,
                    metrics.errors_count,
                )
        except BaseException:  # noqa: BLE001
            status = "failed"
            metrics.errors_count += 1
            logger.exception("Fallo durante la ejecucion del pipeline.")
            raise
        finally:
            if self.repository and run_id:
                self.repository.finish_run(
                    run_id=run_id,
                    status=status,
                    candidates_read=metrics.candidates_read,
                    candidates_persisted=metrics.candidates_persisted,
                    errors_count=metrics.errors_count,
                    metadata={
                        "tipos_procesados": metrics.tipos_procesados,
                        "pages_read": metrics.pages_read,
                    },
                )
                logger.info("Run finalizado | run_id=%s status=%s", run_id, status)

        return metrics

    def _resolve_tipos_eleccion(self, tipos: Optional[List[dict]] = None) -> List[int]:
        if self.config.tipo_eleccion_id:
            return [self.config.tipo_eleccion_id]

        tipos = tipos or self.client.get_tipos_eleccion(self.config.process_id)
        if self.config.tipo_eleccion_nombre:
            objetivo = self.config.tipo_eleccion_nombre.strip().upper()
            resolved = [
                int(t["idTipoEleccion"])
                for t in tipos
                if t.get("idTipoEleccion") is not None
                and str(t.get("tipoEleccion", "")).strip().upper() == objetivo
            ]
            if resolved:
                return sorted(resolved)
            raise ValueError(
                f"No se encontro tipo eleccion '{self.config.tipo_eleccion_nombre}' "
                f"en proceso {self.config.process_id}."
            )

        return sorted([int(t["idTipoEleccion"]) for t in tipos if t.get("idTipoEleccion") is not None])

    def _ingest_tipo_eleccion(
        self,
        tipo_id: int,
        tipo_eleccion_nombre: Optional[str],
        metrics: SearchMetrics,
        run_id,
    ) -> None:
        filtro = CandidateFilter(
            process_id=self.config.process_id,
            tipo_eleccion_id=tipo_id,
            organizacion_politica_id=0,
            estado_id=0,
            sentencia_declarada_id=0,
            grado_academico_id=0,
            expediente_dadiva_id=0,
        )

        skip = 1
        total_pages = None
        while True:
            if self.config.max_pages and skip > self.config.max_pages:
                logger.info(
                    "Se alcanzo limite max_pages=%s en tipo_eleccion_id=%s",
                    self.config.max_pages,
                    tipo_id,
                )
                break
            if total_pages and skip > total_pages:
                break

            result = self._search_page_with_captcha_retry(
                filter_payload=filtro.to_api_filter(),
                skip=skip,
            )
            data = result.get("data", [])
            total_pages = int(result.get("totalPages") or 0)
            metrics.pages_read += 1
            logger.info(
                "Pagina procesada | tipo_eleccion_id=%s page=%s total_pages=%s candidatos_en_pagina=%s",
                tipo_id,
                skip,
                total_pages,
                len(data),
            )

            if not data:
                logger.info("Sin mas resultados | tipo_eleccion_id=%s page=%s", tipo_id, skip)
                break

            for candidate in data:
                if not self._candidate_in_partition(candidate):
                    continue
                metrics.candidates_read += 1
                try:
                    self._ingest_candidate(
                        candidate,
                        tipo_id,
                        tipo_eleccion_nombre,
                        metrics,
                        run_id,
                    )
                except Exception:  # noqa: BLE001
                    metrics.errors_count += 1
                    logger.exception(
                        "Error al procesar candidato | tipo_eleccion_id=%s id_hoja_vida=%s",
                        tipo_id,
                        candidate.get("idHojaVida"),
                    )
                    continue

            skip += 1

    def _candidate_in_partition(self, candidate: dict) -> bool:
        partition_mod = self.config.partition_mod
        partition_rem = self.config.partition_rem
        if partition_mod is None or partition_rem is None:
            return True

        raw_id = candidate.get("idHojaVida")
        try:
            id_hoja_vida = int(raw_id)
        except (TypeError, ValueError):
            try:
                id_hoja_vida = int(float(raw_id))
            except (TypeError, ValueError):
                logger.warning("No se pudo parsear idHojaVida para particionado: %s", raw_id)
                return False

        return (id_hoja_vida % partition_mod) == partition_rem

    def _search_page_with_captcha_retry(self, *, filter_payload: dict, skip: int) -> dict:
        # Endpoint sin recaptcha (estable para cargas masivas). Si falla, degradar al flujo previo.
        try:
            return self.client.search_candidatos_avanzada_exporta(
                filter_payload=filter_payload,
                page_size=self.config.page_size,
                skip=skip,
            )
        except Exception as export_exc:  # noqa: BLE001
            logger.warning(
                "Fallback a flujo con recaptcha por fallo en avanzadaexporta | page=%s detalle=%s",
                skip,
                str(export_exc)[:260],
            )

        if self.config.search_mode == "browser":
            return self._search_page_with_browser_retry(filter_payload=filter_payload, skip=skip)

        last_error: Optional[Exception] = None
        for attempt in range(1, self.config.captcha_retries + 1):
            token = self.token_provider.get_token("buscarCandidatoAvanzada")
            try:
                return self.client.search_candidatos_avanzada(
                    filter_payload=filter_payload,
                    page_size=self.config.page_size,
                    skip=skip,
                    google_token=token,
                )
            except CaptchaRequiredError as exc:
                last_error = exc
                if "mode=image" in str(exc).lower():
                    logger.info("Captcha en modo imagen detectado | page=%s", skip)
                    try:
                        return self._search_with_image_captcha(filter_payload=filter_payload, skip=skip)
                    except Exception as captcha_exc:  # noqa: BLE001
                        last_error = captcha_exc
                        logger.warning(
                            "Fallo en captcha imagen; continuando con refresh de token | page=%s detalle=%s",
                            skip,
                            str(captcha_exc)[:260],
                        )
                logger.warning(
                    "Captcha rechazado; reintentando con nuevo token | page=%s intento=%s/%s detalle=%s",
                    skip,
                    attempt,
                    self.config.captcha_retries,
                    str(exc)[:260],
                )
                if attempt < self.config.captcha_retries:
                    time.sleep(max(0.2, self.config.request_backoff_seconds))
                    continue
                raise

        if last_error:
            raise last_error
        raise RuntimeError("No se pudo ejecutar busqueda avanzada por error inesperado de captcha.")

    def _search_page_with_browser_retry(self, *, filter_payload: dict, skip: int) -> dict:
        if not self.browser_search_provider:
            raise RuntimeError("search_mode=browser requiere browser_search_provider inicializado.")

        last_error: Optional[Exception] = None
        for attempt in range(1, self.config.captcha_retries + 1):
            try:
                return self.browser_search_provider.search_candidatos_avanzada(
                    filter_payload=filter_payload,
                    page_size=self.config.page_size,
                    skip=skip,
                )
            except CaptchaRequiredError as exc:
                last_error = exc
                detail = str(exc)
                if "mode=image" in detail.lower():
                    preferred_base = self._extract_base_from_captcha_error(detail)
                    logger.warning(
                        "Captcha en modo browser; activando fallback captcha imagen | page=%s intento=%s/%s base=%s detalle=%s",
                        skip,
                        attempt,
                        self.config.captcha_retries,
                        preferred_base or "(auto)",
                        detail[:260],
                    )
                    try:
                        browser_token = self.browser_search_provider.get_google_token(
                            "buscarCandidatoAvanzada"
                        )
                        return self._search_with_image_captcha(
                            filter_payload=filter_payload,
                            skip=skip,
                            preferred_base=preferred_base,
                            google_token=browser_token,
                        )
                    except Exception as fallback_exc:  # noqa: BLE001
                        last_error = fallback_exc
                        logger.warning(
                            "Fallback captcha imagen fallo en modo browser | page=%s intento=%s/%s detalle=%s",
                            skip,
                            attempt,
                            self.config.captcha_retries,
                            str(fallback_exc)[:260],
                        )
                else:
                    logger.warning(
                        "Captcha en modo browser; reintentando | page=%s intento=%s/%s detalle=%s",
                        skip,
                        attempt,
                        self.config.captcha_retries,
                        detail[:260],
                    )
                if attempt < self.config.captcha_retries:
                    time.sleep(max(0.2, self.config.request_backoff_seconds))
                    continue
                raise
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                logger.warning(
                    "Fallo busqueda modo browser | page=%s intento=%s/%s detalle=%s",
                    skip,
                    attempt,
                    self.config.captcha_retries,
                    str(exc)[:260],
                )
                if attempt < self.config.captcha_retries:
                    time.sleep(max(0.2, self.config.request_backoff_seconds))
                    continue
                raise

        if last_error:
            raise last_error
        raise RuntimeError("No se pudo ejecutar busqueda avanzada en modo browser.")

    @staticmethod
    def _extract_base_from_captcha_error(detail: str) -> Optional[str]:
        match = re.search(r"(https://[^/\s]+)/api/v1/candidato/avanzada", detail)
        if not match:
            return None
        return match.group(1).rstrip("/")

    def _search_with_image_captcha(
        self,
        *,
        filter_payload: dict,
        skip: int,
        preferred_base: Optional[str] = None,
        google_token: Optional[str] = None,
    ) -> dict:
        preferred_bases = [preferred_base] if preferred_base else None
        captcha, captcha_base = self.client.get_captcha_image_with_base(preferred_bases=preferred_bases)
        token = str(captcha.get("token") or "").strip()
        image_base64 = str(captcha.get("imageBase64") or "").strip()
        if not token or not image_base64:
            raise RuntimeError("Captcha image endpoint no devolvio token/imageBase64.")

        image_bytes = base64.b64decode(image_base64)
        image_path = self._write_captcha_image(image_bytes)
        logger.warning("Captcha imagen guardado en %s | base=%s", image_path, captcha_base)

        solved_text = self._solve_captcha_text(image_path=image_path)
        if not solved_text:
            raise RuntimeError("No se obtuvo texto captcha valido.")

        resolved_google_token = google_token or self.token_provider.get_token("buscarCandidatoAvanzada")
        logger.info("Captcha imagen resuelto; reintentando pagina=%s", skip)
        return self.client.search_candidatos_avanzada(
            filter_payload=filter_payload,
            page_size=self.config.page_size,
            skip=skip,
            google_token=resolved_google_token,
            captcha_token=token,
            captcha_text=solved_text,
            forced_base=captcha_base,
        )

    @staticmethod
    def _write_captcha_image(image_bytes: bytes) -> str:
        with tempfile.NamedTemporaryFile(prefix="jne_captcha_", suffix=".png", delete=False) as f:
            f.write(image_bytes)
            return f.name

    def _solve_captcha_text(self, *, image_path: str) -> str:
        # 1) Texto precargado por variable de entorno (útil para ejecución no interactiva)
        preset = os.getenv("JNE_CAPTCHA_TEXT", "").strip()
        if preset:
            logger.info("Usando JNE_CAPTCHA_TEXT para resolver captcha.")
            return preset

        # 2) OCR best-effort con tesseract.
        # Evitar /captcha/validate previo: puede invalidar captcha de un solo uso.
        ocr_candidates = self._ocr_captcha_candidates(image_path)
        if ocr_candidates:
            logger.info("Captcha resuelto por OCR (sin pre-validacion).")
            return ocr_candidates[0]

        # 3) Prompt manual en terminal interactiva
        if not sys.stdin.isatty():
            raise RuntimeError(
                "Captcha requiere ingreso manual. Ejecuta en TTY y escribe el texto "
                f"del archivo: {image_path}"
            )

        typed = input(f"Ingresa el texto del captcha ({image_path}): ").strip().upper()
        if not typed:
            raise RuntimeError("No se ingreso texto captcha.")
        return typed

    @staticmethod
    def _ocr_captcha_candidates(image_path: str) -> List[str]:
        commands = [
            ["tesseract", image_path, "stdout", "--psm", "7", "-c", "tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"],
            ["tesseract", image_path, "stdout", "--psm", "8", "-c", "tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"],
            ["tesseract", image_path, "stdout", "--psm", "13", "-c", "tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"],
        ]
        candidates: List[str] = []
        for cmd in commands:
            try:
                out = subprocess.run(cmd, capture_output=True, text=True, timeout=8)
            except Exception:  # noqa: BLE001
                continue
            text = "".join(re.findall(r"[A-Z0-9]+", out.stdout.upper()))
            if 3 <= len(text) <= 8:
                candidates.append(text)

        # Deduplicar manteniendo orden
        dedup: List[str] = []
        seen = set()
        for value in candidates:
            if value not in seen:
                seen.add(value)
                dedup.append(value)
        return dedup

    def _ingest_candidate(
        self,
        candidate: dict,
        tipo_id: int,
        tipo_eleccion_nombre: Optional[str],
        metrics: SearchMetrics,
        run_id,
    ) -> None:
        id_hoja_vida = int(candidate["idHojaVida"])
        logger.debug(
            "Procesando candidato | id_hoja_vida=%s nombre=%s org=%s",
            id_hoja_vida,
            candidate.get("nombreCompleto"),
            candidate.get("organizacionPolitica"),
        )

        if self.repository and run_id:
            self.repository.upsert_candidato(
                run_id=run_id,
                row=candidate,
                process_id=self.config.process_id,
                tipo_eleccion_id=tipo_id,
                tipo_eleccion_nombre=tipo_eleccion_nombre,
            )

        hoja_vida = self.client.get_hoja_vida(id_hoja_vida)
        anotaciones = self.client.get_anotaciones_marginales(id_hoja_vida)
        expedientes = self.client.get_expedientes_candidato(id_hoja_vida)

        if self.repository and run_id:
            self.repository.upsert_hoja_vida_raw(run_id, id_hoja_vida, hoja_vida)
            self.repository.upsert_hoja_vida_secciones_raw(run_id, id_hoja_vida, hoja_vida)
            self.repository.upsert_hoja_vida_sections(run_id, id_hoja_vida, hoja_vida)
            self.repository.upsert_anotaciones_raw(run_id, id_hoja_vida, anotaciones)
            self.repository.upsert_anotaciones(run_id, id_hoja_vida, anotaciones)
            self.repository.upsert_expedientes_raw(run_id, id_hoja_vida, expedientes)
            self.repository.upsert_expedientes(run_id, id_hoja_vida, expedientes)
            metrics.candidates_persisted += 1
            logger.debug("Candidato persistido | id_hoja_vida=%s", id_hoja_vida)
