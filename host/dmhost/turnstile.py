"""The Cloudflare Turnstile check of the Host (the bot check on each submit).

The Webapp shows the Turnstile widget, and the widget gives the page a token. The API
sends the token to the Cloudflare Siteverify endpoint with the secret key
(https://developers.cloudflare.com/turnstile/get-started/server-side-validation/):

- ``POST https://challenges.cloudflare.com/turnstile/v0/siteverify``, form fields
  ``secret``, ``response`` (the token), and ``remoteip`` (optional).
- The JSON answer has ``success`` and, on a failure, ``error-codes``.
- A token is at most 2048 characters, is valid for 300 seconds, and works only once.

:class:`TurnstileVerifier` takes the HTTP call as ``post``, so that tests need no network.
The check fails closed: if Cloudflare does not answer, the submit fails.

For local runs, Cloudflare gives test keys
(https://developers.cloudflare.com/turnstile/troubleshooting/testing/): the site key
``1x00000000000000000000AA`` and the secret key ``1x0000000000000000000000000000000AA``
always pass.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Mapping, Optional

SITEVERIFY_URL = "https://challenges.cloudflare.com/turnstile/v0/siteverify"
MAX_TOKEN_LENGTH = 2048

# ``(token, remote_ip) -> bool``: the type of every Turnstile check, real or fake.
TurnstileCheck = Callable[[str, Optional[str]], bool]
# ``(url, form_fields) -> JSON body``: the HTTP call to Siteverify.
SiteverifyPost = Callable[[str, Mapping[str, str]], Dict[str, Any]]

logger = logging.getLogger(__name__)


def httpx_post(url: str, form: Mapping[str, str]) -> Dict[str, Any]:
    """Send the form to ``url`` and return the JSON body (the production HTTP call)."""
    import httpx

    response = httpx.post(url, data=dict(form), timeout=10.0)
    response.raise_for_status()
    body = response.json()
    return body if isinstance(body, dict) else {}


class TurnstileVerifier:
    """Check a Turnstile token with Siteverify."""

    def __init__(self, secret: str, post: SiteverifyPost = httpx_post) -> None:
        if not secret:
            raise ValueError("The Turnstile secret key is empty. Set DM_HOST_TURNSTILE_SECRET.")
        self._secret = secret
        self._post = post

    def __call__(self, token: str, remote_ip: Optional[str]) -> bool:
        if not token or len(token) > MAX_TOKEN_LENGTH:
            return False
        form = {"secret": self._secret, "response": token}
        if remote_ip:
            form["remoteip"] = remote_ip
        try:
            body = self._post(SITEVERIFY_URL, form)
        except Exception as exc:  # noqa: BLE001 - any failure of the call fails the check
            logger.warning("Turnstile Siteverify failed: %s", exc)
            return False
        if body.get("success") is True:
            return True
        logger.info("Turnstile refused a token: %s", body.get("error-codes", []))
        return False
