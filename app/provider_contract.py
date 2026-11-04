"""Versioned discovery contract for the independent Fujin Provider."""

from __future__ import annotations

from pydantic import BaseModel, Field

from app.runtime_context import (
    FUJIN_BUILD_REVISION,
    FUJIN_SERVICE_VERSION,
    PROVIDER_CONTRACT_VERSION,
)


class ProviderHandshake(BaseModel):
    """Public, credential-free discovery payload for a Fujin Provider."""

    service: str = "fujin-provider"
    service_version: str = FUJIN_SERVICE_VERSION
    build_revision: str = FUJIN_BUILD_REVISION
    contract_version: str = PROVIDER_CONTRACT_VERSION
    ready: bool = False
    capabilities: list[str] = Field(default_factory=list)
    endpoint_profile: dict[str, str] = Field(default_factory=dict)


class ProviderContractError(RuntimeError):
    """Raised when a consumer receives an incompatible Provider handshake."""


def validate_handshake(
    handshake: ProviderHandshake,
    expected_contract: str = PROVIDER_CONTRACT_VERSION,
) -> ProviderHandshake:
    if handshake.service != "fujin-provider":
        raise ProviderContractError("Unexpected provider service identity")
    if handshake.contract_version != expected_contract:
        raise ProviderContractError(
            f"Provider contract mismatch: expected {expected_contract}, got {handshake.contract_version}"
        )
    if not handshake.ready:
        raise ProviderContractError("Provider is not ready")
    return handshake
