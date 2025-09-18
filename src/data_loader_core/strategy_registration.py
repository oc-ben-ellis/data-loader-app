"""Strategy registration for the loader service.

This module provides a centralized way to register all strategy factories
with the StrategyFactoryRegistry, enabling YAML-based configuration loading.
"""

from oc_pipeline_bus.strategy_registry import StrategyFactoryRegistry



def create_strategy_registry() -> StrategyFactoryRegistry:
    """Create and register all available strategy factories with a new registry.

    Returns:
        Registry with all strategies registered
    """
    registry = StrategyFactoryRegistry()

    # Add your custom strategy registrations here
    # Example:
    # register_custom_strategies(registry, custom_manager=custom_manager)

    return registry
