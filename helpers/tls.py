"""TLS helpers that use the operating system CA trust store."""

from __future__ import annotations

import ssl

import httpx
import requests
import truststore
from requests.adapters import HTTPAdapter


def verified_ssl_context() -> ssl.SSLContext:
    """Return a client context with certificate and hostname verification enabled."""
    return truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)


class _SystemTrustAdapter(HTTPAdapter):
    """Requests adapter backed by the OS trust store."""

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        pool_kwargs["ssl_context"] = verified_ssl_context()
        return super().init_poolmanager(connections, maxsize, block=block, **pool_kwargs)

    def proxy_manager_for(self, proxy, **proxy_kwargs):
        proxy_kwargs["ssl_context"] = verified_ssl_context()
        return super().proxy_manager_for(proxy, **proxy_kwargs)


def system_trust_session() -> requests.Session:
    """Return a Requests session with verified OS/corporate CA support."""
    session = requests.Session()
    session.mount("https://", _SystemTrustAdapter())
    return session


def verified_httpx_client(timeout: float = 120.0) -> httpx.Client:
    """Return an HTTPX client with verified OS/corporate CA support."""
    return httpx.Client(verify=verified_ssl_context(), timeout=timeout)
