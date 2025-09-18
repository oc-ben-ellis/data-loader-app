# Stubs Directory

This directory contains mock implementations, type stubs, and test doubles for the OC loader project.

## Purpose

The stubs directory serves as a centralized location for:

1. **Type Stubs**: Mock implementations of external libraries and services
2. **Test Doubles**: Mock services for testing external dependencies
3. **Development Stubs**: Temporary implementations for development when real services are unavailable

## Contents

### Mock Services

- `__init__.py` - Module initialization and documentation

Note: Mock services are now organized in the project root under `mocks/` directory (e.g., `mocks/fr/images/siren_api/` for the French SIREN API mock).

## Usage

### In Tests

Mock services are automatically used by test fixtures and can be started manually:

```bash
# Manual testing (optional)
cd mocks/fr/images/siren_api
docker build -t siren_api_mock .
docker run -p 5000:5000 siren_api_mock
```

### Configuration

The stubs directory is automatically included in the mypy path configuration in `pyproject.toml`:

```toml
mypy_path = "$MYPY_CONFIG_FILE_DIR/stubs:$MYPY_CONFIG_FILE_DIR/test"
```

## Development Guidelines

1. **Naming**: Use descriptive names that clearly indicate the purpose (e.g., `MockLineageEmitter`)
2. **Documentation**: Include comprehensive docstrings explaining the mock's purpose and behavior
3. **Type Safety**: Ensure all mock classes have proper type annotations
4. **Consistency**: Follow the same patterns as the real implementations they're mocking
5. **Testing**: Mock implementations should be tested to ensure they behave as expected

## Migration from tests/mocks

This directory consolidates mock implementations that were previously scattered across the codebase:

- Mock services moved from `tests/mocks/` to the project root `mocks/` directory

All imports and references have been updated to use the new organization.
