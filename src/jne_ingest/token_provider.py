from __future__ import annotations

import os
from abc import ABC, abstractmethod


class RecaptchaTokenProvider(ABC):
    @abstractmethod
    def get_token(self, action: str) -> str:
        """Retorna token de recaptcha enterprise para una accion."""


class StaticTokenProvider(RecaptchaTokenProvider):
    def __init__(self, token: str) -> None:
        self._token = token.strip()

    def get_token(self, action: str) -> str:
        if not self._token:
            raise ValueError("Token recaptcha vacio en StaticTokenProvider.")
        return self._token


class EnvTokenProvider(RecaptchaTokenProvider):
    def __init__(self, env_var: str = "JNE_RECAPTCHA_TOKEN") -> None:
        self._env_var = env_var

    def get_token(self, action: str) -> str:
        token = os.getenv(self._env_var, "").strip()
        if not token:
            raise ValueError(
                f"No existe token en {self._env_var}. "
                "Configura un token o implementa un proveedor automatizado."
            )
        return token


class PlaywrightTokenProvider(RecaptchaTokenProvider):
    def __init__(
        self,
        *,
        page_url: str = "https://plataformaelectoral.jne.gob.pe/bandeja/filtros",
        site_key: str = "6LcOHUMsAAAAAA5ysy09CdMpOhHoEuFzHdnsun2V",
        headless: bool | None = None,
        timeout_ms: int | None = None,
        browser_channel: str | None = None,
    ) -> None:
        if headless is None:
            headless = os.getenv("JNE_PLAYWRIGHT_HEADLESS", "false").strip().lower() in {
                "1",
                "true",
                "t",
                "yes",
                "y",
                "on",
            }
        if timeout_ms is None:
            timeout_ms = int(os.getenv("JNE_PLAYWRIGHT_TIMEOUT_MS", "60000"))
        if browser_channel is None:
            browser_channel = os.getenv("JNE_PLAYWRIGHT_CHANNEL", "chrome").strip() or None
        self._page_url = page_url
        self._advanced_url = "https://plataformaelectoral.jne.gob.pe/candidatos/busqueda-avanzada/buscar"
        self._site_key = site_key
        self._headless = headless
        self._timeout_ms = timeout_ms
        self._browser_channel = browser_channel

    def get_token(self, action: str) -> str:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise RuntimeError(
                "Playwright no esta instalado. Ejecuta: `pip install playwright` "
                "y luego `python -m playwright install chromium`."
            ) from exc

        with sync_playwright() as playwright:
            launch_kwargs = {"headless": self._headless}
            if self._browser_channel:
                launch_kwargs["channel"] = self._browser_channel
            browser = playwright.chromium.launch(**launch_kwargs)
            context = browser.new_context(ignore_https_errors=True)
            page = context.new_page()
            try:
                page.goto(self._page_url, wait_until="domcontentloaded", timeout=self._timeout_ms)
                # Flujo sugerido por operación: filtros -> búsqueda avanzada.
                page.wait_for_timeout(1200)
                page.goto(self._advanced_url, wait_until="domcontentloaded", timeout=self._timeout_ms)
                page.wait_for_function(
                    """() => {
                        const sel = document.querySelector('select[formcontrolname="idTipoEleccion"]');
                        return !!sel && !!sel.options && sel.options.length > 1;
                    }""",
                    timeout=self._timeout_ms,
                )
                page.wait_for_timeout(1000)
                token = page.evaluate(
                    """async ({ siteKey, action }) => {
                        if (!window.grecaptcha || !window.grecaptcha.enterprise) {
                            throw new Error("grecaptcha enterprise no disponible");
                        }
                        return await window.grecaptcha.enterprise.execute(siteKey, { action });
                    }""",
                    {"siteKey": self._site_key, "action": action},
                )
            finally:
                context.close()
                browser.close()

        if not token or not isinstance(token, str):
            raise RuntimeError("No se pudo generar token reCAPTCHA con Playwright.")
        return token
