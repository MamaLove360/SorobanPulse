"""Typed response models for the SorobanPulse REST API."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass(frozen=True)
class MetaResponse:
    """Build metadata and compiled features returned by ``GET /v1/meta``."""

    features: List[str] = field(default_factory=list)
    version: str = ""
    build_sha: str = ""
    api_version: str = ""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MetaResponse":
        return cls(
            features=list(data.get("features") or []),
            version=str(data.get("version", "")),
            build_sha=str(data.get("build_sha", "")),
            api_version=str(data.get("api_version", "")),
        )

    def has_feature(self, name: str) -> bool:
        """Whether the server was compiled with the Cargo feature ``name``."""
        return name in self.features
