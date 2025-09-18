"""Strategy type definitions for the data service.

This module defines the abstract base classes and protocols for different
strategy types used in the data service, providing proper type
annotations instead of using generic Callable types.
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # Only for static typing; avoids runtime circular imports
    # Import your service config here
    # from data_loader_core.core import DataRegistryServiceConfig
    pass


# Type aliases for common strategy types
# Add your custom strategy type aliases here


# Strategy configuration types (for YAML config)
# Add your custom strategy configuration types here
ServiceStrategyConfig = dict[str, Any]  # Configuration dict for service strategies