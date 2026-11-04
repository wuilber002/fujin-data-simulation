"""Shared Fujin release and configuration helpers.

Fujin is an independent simulator service. It has no Raijin execution mode or
access to a Raijin database; consumers integrate solely through HTTP.
"""

from __future__ import annotations

import os
from urllib.parse import urlparse


# Version identifiers are deliberately centralized: the About page and the
# API report the release that is actually running, rather than a duplicated
# presentation-only value.
FUJIN_SERVICE_VERSION = os.environ.get("FUJIN_SERVICE_VERSION", "0.1.0").strip() or "0.1.0"
FUJIN_BUILD_REVISION = os.environ.get("FUJIN_BUILD_REVISION", "development").strip() or "development"
SIMULATOR_CONTRACT_VERSION = "2"
SIMULATOR_SERVICE_VERSION = "0.1.0"
PROVIDER_CONTRACT_VERSION = "1"


class FujinConfigurationError(RuntimeError):
    """Raised when a Fujin service is started with an unsafe configuration."""


def database_name(database_url: str) -> str:
    """Return the database component of a SQLAlchemy URL."""
    parsed = urlparse(database_url)
    return parsed.path.lstrip("/").split("?", 1)[0]
