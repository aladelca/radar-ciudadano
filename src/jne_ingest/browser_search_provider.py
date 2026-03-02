from __future__ import annotations

import logging
import os
import random
from typing import Optional

from jne_ingest.config import AppConfig
from jne_ingest.jne_client import CaptchaRequiredError
from jne_ingest.models import JsonDict


logger = logging.getLogger("jne_ingest.browser_search")


class PlaywrightAdvancedSearchProvider:
    def __init__(
        self,
        config: AppConfig,
        *,
        page_url: str = "https://plataformaelectoral.jne.gob.pe/bandeja/filtros",
        advanced_url: str = "https://plataformaelectoral.jne.gob.pe/candidatos/busqueda-avanzada/buscar",
        site_key: str = "6LcOHUMsAAAAAA5ysy09CdMpOhHoEuFzHdnsun2V",
        headless: bool | None = None,
        timeout_ms: int | None = None,
        browser_channel: str | None = None,
        user_data_dir: str | None = None,
    ) -> None:
        self.config = config
        self._page_url = page_url
        self._advanced_url = advanced_url
        self._site_key = site_key
        self._headless = (
            headless
            if headless is not None
            else os.getenv("JNE_PLAYWRIGHT_HEADLESS", "false").strip().lower()
            in {"1", "true", "t", "yes", "y", "on"}
        )
        self._timeout_ms = timeout_ms if timeout_ms is not None else int(os.getenv("JNE_PLAYWRIGHT_TIMEOUT_MS", "60000"))
        self._browser_channel = (
            browser_channel
            if browser_channel is not None
            else (os.getenv("JNE_PLAYWRIGHT_CHANNEL", "chrome").strip() or None)
        )
        self._user_data_dir = user_data_dir if user_data_dir is not None else os.getenv("JNE_PLAYWRIGHT_USER_DATA_DIR", "").strip()

        self._bases = [
            config.api_path5,
            config.api_path4,
            config.api_path6,
            config.api_path7,
        ]

        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None
        self._persistent_context = False

    @staticmethod
    def _extract_error_context(payload: Optional[JsonDict], fallback_text: str) -> str:
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
        return (fallback_text or "(sin body)")[:300]

    @staticmethod
    def _is_captcha_error(status_code: int, context: str) -> bool:
        if status_code != 400:
            return False
        upper = context.upper()
        return "CAPTCHA_REQUIRED" in upper or "INVALID_TOKEN" in upper or "CAPTCHA_INVALID" in upper

    def _build_launch_kwargs(self) -> dict:
        launch_kwargs: dict = {
            "headless": self._headless,
            "args": ["--disable-blink-features=AutomationControlled"],
        }
        if self._browser_channel:
            launch_kwargs["channel"] = self._browser_channel
        return launch_kwargs

    def _launch_ephemeral_context(self, launch_kwargs: dict) -> None:
        if self._playwright is None:
            raise RuntimeError("Playwright no inicializado para lanzar contexto temporal.")

        kwargs = dict(launch_kwargs)
        try:
            self._browser = self._playwright.chromium.launch(**kwargs)
        except Exception:  # noqa: BLE001
            # Fallback: algunos entornos no soportan channel=chrome.
            kwargs.pop("channel", None)
            self._browser = self._playwright.chromium.launch(**kwargs)

        self._context = self._browser.new_context(ignore_https_errors=True)
        self._context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
        self._page = self._context.new_page()

    def _start(self) -> None:
        if self._page is not None:
            return

        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise RuntimeError(
                "Playwright no esta instalado. Ejecuta: `pip install playwright` "
                "y luego `python -m playwright install chromium`."
            ) from exc

        launch_kwargs = self._build_launch_kwargs()
        try:
            self._playwright = sync_playwright().start()

            if self._user_data_dir:
                self._persistent_context = True
                try:
                    context = self._playwright.chromium.launch_persistent_context(
                        self._user_data_dir,
                        **launch_kwargs,
                        ignore_https_errors=True,
                    )
                    page = context.pages[0] if context.pages else context.new_page()
                    self._context = context
                    self._page = page
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "No se pudo abrir perfil persistente '%s'; fallback a contexto temporal. detalle=%s",
                        self._user_data_dir,
                        str(exc)[:260],
                    )
                    self._persistent_context = False
                    self._launch_ephemeral_context(launch_kwargs)
            else:
                self._persistent_context = False
                self._launch_ephemeral_context(launch_kwargs)

            if self._page is None:
                raise RuntimeError("Playwright page no inicializada tras launch.")

            self._page.set_default_timeout(self._timeout_ms)
            self._page.goto(self._page_url, wait_until="domcontentloaded", timeout=self._timeout_ms)
            self._page.wait_for_timeout(1200)
            self._page.evaluate(
                """(processId) => {
                    sessionStorage.setItem("idProcesoElectoral", String(processId));
                }""",
                self.config.process_id,
            )
            self._page.goto(self._advanced_url, wait_until="domcontentloaded", timeout=self._timeout_ms)
            self._page.wait_for_function(
                "() => !!window.grecaptcha && !!window.grecaptcha.enterprise",
                timeout=self._timeout_ms,
            )
            self._page.wait_for_timeout(900)
        except Exception:  # noqa: BLE001
            # Evita estados parciales que rompen reintentos posteriores.
            self.close()
            raise

        logger.info(
            "Playwright browser-search listo | headless=%s channel=%s profile=%s",
            self._headless,
            self._browser_channel,
            self._user_data_dir or "(temporal)",
        )

    def close(self) -> None:
        if self._context:
            self._context.close()
            self._context = None
        if self._browser:
            self._browser.close()
            self._browser = None
        if self._playwright:
            self._playwright.stop()
            self._playwright = None
        self._page = None

    def search_candidatos_avanzada(
        self,
        *,
        filter_payload: JsonDict,
        page_size: int,
        skip: int,
    ) -> JsonDict:
        self._start()
        if self._page is None:
            raise RuntimeError("Playwright page no inicializada.")

        request_body = {
            "data": {
                "pageSize": page_size,
                "skip": skip,
                "filter": filter_payload,
            },
            "recaptcha": {
                "googleToken": "",
                "action": "buscarCandidatoAvanzada",
                "captchaToken": None,
                "captchaText": None,
            },
        }

        base = random.choice(self._bases)
        endpoint = f"{base}/api/v1/candidato/avanzada"
        token = self.get_google_token("buscarCandidatoAvanzada")
        request_body["recaptcha"]["googleToken"] = token

        if self._context is None:
            raise RuntimeError("Playwright context no inicializado.")

        api_response = self._context.request.post(
            endpoint,
            data=request_body,
            headers={
                "accept": "application/json, text/plain, */*",
                "content-type": "application/json",
                "origin": "https://plataformaelectoral.jne.gob.pe",
                "referer": "https://plataformaelectoral.jne.gob.pe/candidatos/busqueda-avanzada/buscar",
            },
            fail_on_status_code=False,
        )
        text = api_response.text()
        try:
            payload = api_response.json()
        except Exception:  # noqa: BLE001
            payload = None

        response = {
            "status": api_response.status,
            "ok": api_response.ok,
            "endpoint": endpoint,
            "json": payload,
            "text": text[:2000],
        }

        status = int(response.get("status") or 0)
        payload = response.get("json")
        if status >= 400:
            context = self._extract_error_context(payload, str(response.get("text") or ""))
            if self._is_captcha_error(status, context):
                raise CaptchaRequiredError(
                    f"Captcha rechazado en {response.get('endpoint')}: {context}"
                )
            raise RuntimeError(
                f"POST /api/v1/candidato/avanzada client error {status} en {response.get('endpoint')}: {context}"
            )
        if not isinstance(payload, dict):
            raise RuntimeError("Respuesta invalida de busqueda avanzada en modo browser.")
        return payload

    def get_google_token(self, action: str = "buscarCandidatoAvanzada") -> str:
        self._start()
        if self._page is None:
            raise RuntimeError("Playwright page no inicializada para obtener token.")
        token = self._page.evaluate(
            """async ({ siteKey, action }) => {
                return await window.grecaptcha.enterprise.execute(siteKey, { action });
            }""",
            {"siteKey": self._site_key, "action": action},
        )
        return str(token or "").strip()
