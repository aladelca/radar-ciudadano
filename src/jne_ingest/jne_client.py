from __future__ import annotations

import logging
import math
import random
import time
from typing import Any, Dict, Iterable, List, Optional

import httpx

from jne_ingest.config import AppConfig
from jne_ingest.models import JsonDict


logger = logging.getLogger("jne_ingest.client")


class CaptchaRequiredError(RuntimeError):
    """Error funcional del endpoint cuando el token captcha no es valido."""


class JNEClient:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.client = httpx.Client(
            timeout=config.request_timeout_seconds,
            verify=config.verify_ssl,
        )
        self._search_headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "origin": "https://plataformaelectoral.jne.gob.pe",
            "referer": "https://plataformaelectoral.jne.gob.pe/candidatos/busqueda-avanzada/buscar",
            # Ayuda a evitar bloqueos anti-bot basicos en algunos nodos del API.
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36"
            ),
        }

        # Frontend JNE (busqueda avanzada) usa estos nodos y selecciona uno al azar por request.
        self.search_bases = [
            config.api_path5,
            config.api_path4,
            config.api_path6,
            config.api_path7,
        ]
        self.catalog_bases = [config.api_path3, config.api_path4, config.api_path2]
        self.hoja_vida_bases = [config.api_path7, config.api_path6, config.api_path8]
        self.anotacion_bases = [config.api_path2, config.api_path4, config.api_path]
        self.expediente_bases = [config.api_path5, config.api_path4, config.api_path3]
        self.captcha_bases = [config.api_path6, config.api_path7, config.api_path8]
        self.plan_detalle_para_candidato_bases = [config.api_path8, config.api_path7, config.api_path6]
        self.plan_detalle_bases = [config.api_path2, config.api_path3, config.api_path4]

    @staticmethod
    def _extract_error_context(resp: httpx.Response) -> str:
        try:
            payload = resp.json()
        except Exception:  # noqa: BLE001
            text = resp.text.strip()
            return text[:300] if text else "(sin body)"
        if isinstance(payload, dict):
            message = str(payload.get("message", "")).strip()
            reason = str(payload.get("reason", "")).strip()
            mode = str(payload.get("mode", "")).strip()
            score = payload.get("score")
            parts = []
            if message:
                parts.append(f"message={message}")
            if reason:
                parts.append(f"reason={reason}")
            if mode:
                parts.append(f"mode={mode}")
            if score is not None:
                parts.append(f"score={score}")
            if parts:
                return " ".join(parts)
        return str(payload)[:300]

    @staticmethod
    def _is_captcha_error(resp: httpx.Response, context: str) -> bool:
        if resp.status_code != 400:
            return False
        upper = context.upper()
        return "CAPTCHA_REQUIRED" in upper or "INVALID_TOKEN" in upper

    def close(self) -> None:
        self.client.close()

    def _get_json(self, endpoint: str, bases: Iterable[str]) -> JsonDict:
        last_error: Optional[Exception] = None
        base_list = list(bases)
        for attempt in range(self.config.request_retries):
            for base in base_list:
                url = f"{base}{endpoint}"
                try:
                    resp = self.client.get(url)
                    resp.raise_for_status()
                    if attempt > 0:
                        logger.info("GET recuperado tras reintento | endpoint=%s intento=%s", endpoint, attempt + 1)
                    return resp.json()
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    logger.warning(
                        "GET fallo | endpoint=%s base=%s intento=%s/%s error=%s",
                        endpoint,
                        base,
                        attempt + 1,
                        self.config.request_retries,
                        exc.__class__.__name__,
                    )
            if attempt < self.config.request_retries - 1:
                time.sleep(self.config.request_backoff_seconds * (2**attempt))
        if last_error:
            raise RuntimeError(f"GET {endpoint} fallo en todas las bases.") from last_error
        raise RuntimeError(f"GET {endpoint} fallo sin error registrado.")

    def _post_json(self, endpoint: str, body: JsonDict, bases: Iterable[str]) -> JsonDict:
        last_error: Optional[Exception] = None
        base_list = list(bases)
        for attempt in range(self.config.request_retries):
            for base in base_list:
                url = f"{base}{endpoint}"
                try:
                    resp = self.client.post(url, json=body, headers=self._search_headers)
                    if resp.is_error:
                        context = self._extract_error_context(resp)
                        if self._is_captcha_error(resp, context):
                            raise CaptchaRequiredError(
                                f"Captcha rechazado en {base}{endpoint}: {context}"
                            )
                        # Los 4xx (excepto captcha) son errores funcionales y no mejoran probando otros nodos.
                        if 400 <= resp.status_code < 500 and resp.status_code != 429:
                            raise RuntimeError(
                                f"POST {endpoint} client error {resp.status_code} en {base}: {context}"
                            )
                    resp.raise_for_status()
                    if attempt > 0:
                        logger.info("POST recuperado tras reintento | endpoint=%s intento=%s", endpoint, attempt + 1)
                    return resp.json()
                except CaptchaRequiredError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    logger.warning(
                        "POST fallo | endpoint=%s base=%s intento=%s/%s error=%s detalle=%s",
                        endpoint,
                        base,
                        attempt + 1,
                        self.config.request_retries,
                        exc.__class__.__name__,
                        str(exc)[:300],
                    )
            if attempt < self.config.request_retries - 1:
                time.sleep(self.config.request_backoff_seconds * (2**attempt))
        if last_error:
            raise RuntimeError(f"POST {endpoint} fallo en todas las bases.") from last_error
        raise RuntimeError(f"POST {endpoint} fallo sin error registrado.")

    def get_procesos_electorales(self) -> List[JsonDict]:
        payload = self._get_json("/api/v1/expediente/proceso-electoral", self.catalog_bases)
        return payload.get("data", [])

    def get_tipos_eleccion(self, process_id: int) -> List[JsonDict]:
        payload = self._get_json(
            f"/api/v1/tipo-eleccion/tipo/{process_id}",
            self.catalog_bases,
        )
        return payload.get("data", [])

    def get_organizaciones_politicas(self, process_id: int) -> List[JsonDict]:
        payload = self._get_json(
            f"/api/v1/organizacion-politica/op/{process_id}",
            self.catalog_bases,
        )
        return payload.get("data", [])

    def search_candidatos_avanzada(
        self,
        *,
        filter_payload: JsonDict,
        page_size: int,
        skip: int,
        google_token: str,
        captcha_token: Optional[str] = None,
        captcha_text: Optional[str] = None,
        forced_base: Optional[str] = None,
    ) -> JsonDict:
        request_body = {
            "data": {
                "pageSize": page_size,
                "skip": skip,
                "filter": filter_payload,
            },
            "recaptcha": {
                "googleToken": google_token,
                "action": "buscarCandidatoAvanzada",
                "captchaToken": captcha_token,
                "captchaText": captcha_text,
            },
        }
        # Imitar comportamiento del frontend: un solo host por intento.
        # Cuando se usa captcha imagen, debe ser el mismo host que emitió ese captcha.
        base = forced_base or random.choice(self.search_bases)
        return self._post_json("/api/v1/candidato/avanzada", request_body, [base])

    def search_candidatos_avanzada_exporta(
        self,
        *,
        filter_payload: JsonDict,
        page_size: int,
        skip: int,
    ) -> JsonDict:
        request_body = {
            "pageSize": page_size,
            "skip": skip,
            "sortField": None,
            "sortDir": None,
            "filter": filter_payload,
        }
        base = random.choice(self.search_bases)
        payload = self._post_json("/api/v1/candidato/avanzadaexporta", request_body, [base])

        # Estandarizar con el contrato que usa el pipeline (totalPages).
        if "totalPages" not in payload:
            try:
                total = float(payload.get("count") or 0)
            except (TypeError, ValueError):
                total = 0
            payload["totalPages"] = int(math.ceil(total / page_size)) if page_size > 0 and total > 0 else 0
        return payload

    def get_hoja_vida(self, id_hoja_vida: int) -> JsonDict:
        return self._get_json(
            f"/api/v1/candidato/hoja-vida?IdHojaVida={id_hoja_vida}",
            self.hoja_vida_bases,
        )

    def get_anotaciones_marginales(self, id_hoja_vida: int) -> JsonDict:
        return self._get_json(
            f"/api/v1/candidato/anotacion-marginal?IdHojaVida={id_hoja_vida}",
            self.anotacion_bases,
        )

    def get_expedientes_candidato(self, id_hoja_vida: int) -> JsonDict:
        return self._get_json(
            f"/api/v1/candidato/expediente?IdHojaVida={id_hoja_vida}",
            self.expediente_bases,
        )

    def get_plan_gobierno_detalle_para_candidato(
        self,
        *,
        process_id: int,
        tipo_eleccion_id: int,
        organizacion_politica_id: int,
        solicitud_lista_id: int,
    ) -> JsonDict:
        return self._get_json(
            (
                "/api/v1/plan-gobierno/detalle-para-candidato"
                f"?IdProcesoElectoral={process_id}"
                f"&IdTipoEleccion={tipo_eleccion_id}"
                f"&IdOrganizacionPolitica={organizacion_politica_id}"
                f"&IdSolicitudLista={solicitud_lista_id}"
            ),
            self.plan_detalle_para_candidato_bases,
        )

    def get_plan_gobierno_detalle(self, plan_gobierno_id: int) -> JsonDict:
        return self._get_json(
            f"/api/v1/plan-gobierno/detalle?IdPlanGobierno={plan_gobierno_id}",
            self.plan_detalle_bases,
        )

    def get_captcha_image_with_base(self, preferred_bases: Optional[Iterable[str]] = None) -> tuple[JsonDict, str]:
        ordered_bases: List[str] = []
        if preferred_bases:
            for base in preferred_bases:
                normalized = str(base or "").strip().rstrip("/")
                if normalized and normalized not in ordered_bases:
                    ordered_bases.append(normalized)
        for base in self.captcha_bases:
            if base not in ordered_bases:
                ordered_bases.append(base)

        last_error: Optional[Exception] = None
        for attempt in range(self.config.request_retries):
            for base in ordered_bases:
                url = f"{base}/api/v1/captcha/image"
                try:
                    resp = self.client.get(url)
                    resp.raise_for_status()
                    return resp.json(), base
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    logger.warning(
                        "GET fallo | endpoint=%s base=%s intento=%s/%s error=%s",
                        "/api/v1/captcha/image",
                        base,
                        attempt + 1,
                        self.config.request_retries,
                        exc.__class__.__name__,
                    )
            if attempt < self.config.request_retries - 1:
                time.sleep(self.config.request_backoff_seconds * (2**attempt))
        if last_error:
            raise RuntimeError("GET /api/v1/captcha/image fallo en todas las bases.") from last_error
        raise RuntimeError("GET /api/v1/captcha/image fallo sin error registrado.")

    def get_captcha_image(self) -> JsonDict:
        payload, _ = self.get_captcha_image_with_base()
        return payload

    def validate_captcha(self, token: str, text: str) -> bool:
        payload = self._post_json(
            "/api/v1/captcha/validate",
            {"token": token, "text": text},
            self.captcha_bases,
        )
        return bool(payload.get("ok"))
