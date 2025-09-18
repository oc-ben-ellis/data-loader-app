"""Core framework components and base classes.

This module provides the fundamental building blocks of the OC loader framework,
including the base DataRegistryLoaderConfig and configuration creation utilities.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Imports used only for type checking to avoid runtime import side effects
    from oc_pipeline_bus.config import Annotated, strategy


@dataclass
class DataRegistryLoaderConfig:
    """YAML-based loader configuration using strategy factory registry."""

    # Loader-specific configuration fields
    config_id: str = ""
    batch_max_size: int = 1_000_000
    # Protocol configurations for resolving relative configs