"""
SSL Proxy Configuration for Corporate Networks

This module demonstrates how to configure SSL for Python applications
in corporate environments where:
  - SSL verification must be disabled (verify=False) for internal CAs
  - A custom CA bundle must be injected via REQUESTS_CA_BUNDLE
  - Corporate proxy settings must be respected

Use cases:
  1. Airflow workers behind a corporate proxy that does MITM SSL inspection
  2. Spark executors accessing S3-compatible storage with self-signed certs
  3. FastAPI/httpx clients in enterprise networks with custom CAs
  4. Scrapy spiders behind SSL inspection proxies

Security Note:
  Using verify=False disables SSL certificate validation, making the
  application vulnerable to MITM attacks. Prefer injecting a custom CA
  bundle whenever possible. verify=False should only be used in
  controlled, internal network environments.
"""

from __future__ import annotations

import logging
import os
import ssl
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import httpx
import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass
class SSLProxyConfig:
    """SSL and proxy configuration for corporate environments.

    Attributes:
        proxy_url: HTTP proxy URL (e.g., http://proxy.example.com:8080).
        https_proxy: HTTPS proxy URL (defaults to proxy_url if not set).
        no_proxy: Comma-separated hosts to bypass proxy.
        verify_ssl: Whether to verify SSL certificates.
        ca_bundle_path: Path to custom CA bundle file.
        cert_path: Path to client certificate file (mutual TLS).
        key_path: Path to client private key file (mutual TLS).
        env_prefix: Prefix for environment variable overrides.
    """

    proxy_url: str | None = None
    https_proxy: str | None = None
    no_proxy: str | None = None
    verify_ssl: bool = True
    ca_bundle_path: str | None = None
    cert_path: str | None = None
    key_path: str | None = None
    env_prefix: str = "HTTP_"

    def __post_init__(self) -> None:
        """Apply environment variable overrides after init."""
        # Proxy settings from environment
        env_proxy = os.getenv(f"{self.env_prefix}PROXY")
        if env_proxy and not self.proxy_url:
            self.proxy_url = env_proxy

        # HTTPS proxy (defaults to HTTP proxy)
        env_https_proxy = os.getenv(f"{self.env_prefix}HTTPS_PROXY")
        if env_https_proxy:
            self.https_proxy = env_https_proxy
        elif self.proxy_url:
            self.https_proxy = self.proxy_url

        # No-proxy list
        env_no_proxy = os.getenv(f"{self.env_prefix}NO_PROXY", "").lower()
        if env_no_proxy:
            self.no_proxy = env_no_proxy

        # CA bundle
        env_ca_bundle = os.getenv("REQUESTS_CA_BUNDLE") or os.getenv("SSL_CERT_FILE")
        if env_ca_bundle and not self.ca_bundle_path:
            self.ca_bundle_path = env_ca_bundle

        # SSL verify override
        env_verify = os.getenv(f"{self.env_prefix}SSL_VERIFY", "").lower()
        if env_verify in ("false", "0", "no"):
            self.verify_ssl = False

    @property
    def proxies(self) -> dict[str, str]:
        """Get a proxies dict suitable for requests/httpx."""
        proxies: dict[str, str] = {}
        if self.proxy_url:
            proxies["http://"] = self.proxy_url
        if self.https_proxy:
            proxies["https://"] = self.https_proxy
        return proxies

    @property
    def verify_param(self) -> bool | str:
        """Get the verify parameter for requests/httpx.

        Returns:
            True (verify with default CAs), False (skip verify),
            or a path to a custom CA bundle.
        """
        if not self.verify_ssl:
            return False
        if self.ca_bundle_path:
            return self.ca_bundle_path
        return True


# ---------------------------------------------------------------------------
# Default configuration from environment
# ---------------------------------------------------------------------------
def load_config_from_env(env_prefix: str = "HTTP_") -> SSLProxyConfig:
    """Load SSL/proxy configuration from environment variables.

    Environment variables (with prefix HTTP_):
      HTTP_PROXY          - HTTP proxy URL
      HTTPS_PROXY         - HTTPS proxy URL
      NO_PROXY            - Comma-separated no-proxy list
      SSL_VERIFY          - Set to 'false' to disable SSL verification
      REQUESTS_CA_BUNDLE  - Path to CA bundle (also honored by requests/httpx natively)

    Args:
        env_prefix: Prefix for environment variables.

    Returns:
        Configured SSLProxyConfig instance.
    """
    return SSLProxyConfig(env_prefix=env_prefix)


# ---------------------------------------------------------------------------
# requests Session with Corporate SSL/Proxy
# ---------------------------------------------------------------------------
class CorporateSession(requests.Session):
    """A requests.Session pre-configured for corporate proxy/SSL environments."""

    def __init__(self, config: SSLProxyConfig | None = None) -> None:
        """
        Args:
            config: SSL/proxy configuration. If None, loads from environment.
        """
        super().__init__()
        self._ssl_config = config or load_config_from_env()

        # Configure proxies
        if self._ssl_config.proxies:
            self.proxies.update(self._ssl_config.proxies)

        # Configure verify
        self.verify = self._ssl_config.verify_param

        # Configure client cert
        if self._ssl_config.cert_path and self._ssl_config.key_path:
            self.cert = (self._ssl_config.cert_path, self._ssl_config.key_path)

        # Log configuration
        logger.info(
            "CorporateSession configured: verify=%s, proxy=%s, ca_bundle=%s",
            self.verify,
            bool(self._ssl_config.proxies),
            self._ssl_config.ca_bundle_path,
        )


class SSLAdapter(HTTPAdapter):
    """HTTPAdapter that uses a custom SSL context.

    Useful when you need fine-grained SSL control (protocol version,
    cipher suites) beyond verify=True/False.
    """

    def __init__(
        self,
        ssl_context: ssl.SSLContext | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Args:
            ssl_context: Pre-configured SSL context. If None, creates one
                that disables cert verification (for internal CAs).
            **kwargs: Additional HTTPAdapter arguments.
        """
        self._ssl_context = ssl_context or self._create_internal_ssl_context()
        super().__init__(**kwargs)

    @staticmethod
    def _create_internal_ssl_context() -> ssl.SSLContext:
        """Create an SSL context that trusts internal corporate CAs.

        Uses certifi as base but adds common corporate CA paths.
        """
        context = ssl.create_default_context()

        # Add common internal CA bundle paths
        ca_paths = [
            "/etc/ssl/certs/ca-certificates.crt",  # Debian/Ubuntu
            "/etc/pki/tls/certs/ca-bundle.crt",     # RHEL/CentOS
            "/etc/ssl/cert.pem",                     # macOS (brew)
            "/usr/local/share/ca-certificates/",      # Custom CAs
        ]

        for ca_path in ca_paths:
            if Path(ca_path).exists():
                if Path(ca_path).is_dir():
                    context.load_verify_locations(cafile=ca_path)
                else:
                    context.load_verify_locations(cafile=ca_path)
                logger.debug("Loaded CA bundle: %s", ca_path)

        return context

    def init_poolmanager(
        self,
        connections: int,
        maxsize: int,
        block: bool = False,
        **kwargs: Any,
    ) -> PoolManager:
        """Override to inject custom SSL context."""
        kwargs["ssl_context"] = self._ssl_context
        return super().init_poolmanager(connections, maxsize, block, **kwargs)


# ---------------------------------------------------------------------------
# httpx AsyncClient with Corporate SSL/Proxy
# ---------------------------------------------------------------------------
def create_httpx_client(
    config: SSLProxyConfig | None = None,
    **kwargs: Any,
) -> httpx.AsyncClient:
    """Create an httpx.AsyncClient configured for corporate environments.

    Args:
        config: SSL/proxy configuration. If None, loads from environment.
        **kwargs: Additional httpx.AsyncClient arguments.

    Returns:
        Configured httpx.AsyncClient instance.
    """
    cfg = config or load_config_from_env()

    client_kwargs: dict[str, Any] = {
        "verify": cfg.verify_param,
        "proxies": cfg.proxies if cfg.proxies else None,
        "timeout": kwargs.pop("timeout", httpx.Timeout(30.0)),
        **kwargs,
    }

    # Remove None values
    client_kwargs = {k: v for k, v in client_kwargs.items() if v is not None}

    # If SSL is disabled but we want to be explicit about it
    if not cfg.verify_ssl:
        logger.warning(
            "SSL verification is DISABLED. "
            "This should only be used in controlled internal networks."
        )

    client = httpx.AsyncClient(**client_kwargs)
    return client


# ---------------------------------------------------------------------------
# Spark Session SSL Configuration
# ---------------------------------------------------------------------------
def spark_ssl_config(
    config: SSLProxyConfig | None = None,
) -> dict[str, str]:
    """Get Spark configuration entries for corporate SSL/proxy.

    These config entries can be passed to SparkSession.builder.config()
    to make Spark respect corporate proxy and SSL settings.

    Args:
        config: SSL/proxy configuration.

    Returns:
        Dict of Spark config key-value pairs.
    """
    cfg = config or load_config_from_env()
    spark_conf: dict[str, str] = {}

    # Proxy configuration for Hadoop/S3A
    if cfg.proxy_url:
        spark_conf["spark.hadoop.fs.s3a.proxy.host"] = cfg.proxy_url
        spark_conf["spark.hadoop.fs.s3a.proxy.port"] = "8080"  # override if needed

    # SSL verification for S3A
    if not cfg.verify_ssl:
        spark_conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] = "false"
        spark_conf["spark.hadoop.fs.s3a.impl.disable.cache"] = "true"

    # Custom CA bundle
    if cfg.ca_bundle_path:
        spark_conf["spark.hadoop.fs.s3a.custom.s3.endpoint"] = ""  # override
        # Some S3-compatible stores accept CA_BUNDLE via Java SSL props
        spark_conf["spark.executor.extraJavaOptions"] = (
            f"-Djavax.net.ssl.trustStore={cfg.ca_bundle_path}"
        )
        spark_conf["spark.driver.extraJavaOptions"] = (
            f"-Djavax.net.ssl.trustStore={cfg.ca_bundle_path}"
        )

    return spark_conf


# ---------------------------------------------------------------------------
# Requests-style shortcut functions
# ---------------------------------------------------------------------------
def corporate_get(url: str, **kwargs: Any) -> requests.Response:
    """Make a GET request using corporate SSL/proxy settings.

    Args:
        url: Request URL.
        **kwargs: Additional requests.get arguments.

    Returns:
        Response object.
    """
    session = CorporateSession()
    return session.get(url, **kwargs)


def corporate_post(url: str, **kwargs: Any) -> requests.Response:
    """Make a POST request using corporate SSL/proxy settings."""
    session = CorporateSession()
    return session.post(url, **kwargs)


# ---------------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Example 1: Load from environment
    print("=== Corporate SSL Proxy Configuration ===")
    config = load_config_from_env()
    print(f"  Proxy URL:     {config.proxy_url or '(none)'}")
    print(f"  HTTPS Proxy:   {config.https_proxy or '(none)'}")
    print(f"  No Proxy:      {config.no_proxy or '(none)'}")
    print(f"  Verify SSL:    {config.verify_ssl}")
    print(f"  CA Bundle:     {config.ca_bundle_path or '(default)'}")

    # Example 2: Create a httpx client
    print("\n=== httpx Client ===")
    client = create_httpx_client()
    print(f"  Client created with verify={client._transport._ssl_context}")

    # Example 3: Create a requests session
    print("\n=== requests Session ===")
    session = CorporateSession()
    print(f"  Session verify: {session.verify}")
    print(f"  Session proxies: {session.proxies}")

    # Example 4: Spark SSL config
    print("\n=== Spark SSL Config ===")
    spark_conf = spark_ssl_config()
    for k, v in spark_conf.items():
        print(f"  {k} = {v}")
