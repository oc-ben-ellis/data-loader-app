# Configuration Integration Tests

This directory contains integration tests for the configuration loading system in the data loader service.

## Test Files

### `test_config_integration.py`
Tests the basic configuration loading functionality:
- ConfigLoader initialization with and without strategy registry
- Loading configurations from YAML files
- Handling of relative configuration references
- Error handling for invalid configurations
- Default value application

### `test_strategy_registry_integration.py`
Tests the integration between the strategy registry and configuration loading:
- Strategy registry creation and population
- Strategy factory registration and validation
- Strategy instance creation from configuration
- Error handling for invalid strategies

### `test_strategy_creation_integration.py`
Tests the `_create_strategy_from_config` method specifically:
- Method signature verification (3 parameters: strategy_type, strategy_id, strategy_config)
- Strategy creation with valid and invalid parameters
- Integration with YAML configuration loading
- Complex configuration structure handling

### `test_strategy_instance_creation.py`
Tests actual strategy instance creation and configuration usage:
- **SFTP loader instance creation** with correct SFTP configuration
- **HTTP loader instance creation** with correct HTTP configuration
- **SFTP locator instance creation** with correct configuration attributes
- **HTTP locator instance creation** with correct configuration attributes
- **Strategy instances with filters** and complex nested configurations
- **End-to-end strategy registry integration** testing

### `test_protocol_config_integration.py`
Tests protocol configuration resolution in strategy instances:
- **SFTP protocol config resolution** - verifies SFTP configs are loaded and used correctly
- **HTTP protocol config resolution** - verifies HTTP configs are loaded and used correctly
- **Multiple protocol configs** - tests handling of multiple protocol types
- **Protocol config validation** - tests error handling for missing/invalid protocols

## Running the Tests

### Prerequisites
1. Install the loader service dependencies:
   ```bash
   cd services/loader
   poetry install
   ```

2. Ensure the pipeline-bus library is available (should be installed as a relative dependency)

### Running Individual Tests
```bash
# Run all configuration integration tests
pytest tests/test_integration/data_loader_core/ -v

# Run specific test file
pytest tests/test_integration/data_loader_core/test_config_integration.py -v

# Run specific test method
pytest tests/test_integration/data_loader_core/test_config_integration.py::TestConfigIntegration::test_config_loader_initialization -v
```

### Running with Docker Dependencies
Some tests may require Docker containers (LocalStack, Redis):
```bash
# Run tests that require containers
pytest tests/test_integration/data_loader_core/ -m "localstack" -v
```

## Test Configuration Examples

The tests use various sample YAML configurations to verify different scenarios:

### Basic HTTP Configuration
```yaml
data_source_id: test_source
concurrency: 5
target_queue_size: 50

loader:
  http_loader:
    meta_load_name: test_http_loader
    http_config: test_http

locators:
  - single_http_locator:
      http_config: test_http
      urls:
        - "https://api.example.com/single"
      state_management_prefix: test_provider

protocols:
  http:
    test_http: http_config.yaml
```

### SFTP Configuration
```yaml
data_source_id: test_sftp_source
concurrency: 2
target_queue_size: 25

loader:
  sftp_loader:
    meta_load_name: test_sftp_loader
    sftp_config: test_sftp

locators:
  - sftp_directory_locator:
      sftp_config: test_sftp
      remote_dir: "/data"
      filename_pattern: "*.txt"
      state_management_prefix: test_sftp_provider

protocols:
  sftp:
    test_sftp: sftp_config.yaml
```

### Complex Configuration with Filters
```yaml
data_source_id: test_complex
concurrency: 1
target_queue_size: 10

loader:
  http_loader:
    meta_load_name: test_complex_loader
    http_config: test_http

locators:
  - pagination_http_locator:
      http_config: test_http
      base_url: "https://api.example.com/data"
      date_start: "2023-01-01"
      date_end: "2023-12-31"
      file_filter:
        type: date_filter
        start_date: "2023-06-01"
        date_pattern: "YYYY-MM-DD"
      state_management_prefix: test_complex_provider

protocols:
  http:
    test_http: http_config.yaml
```

## What the Tests Verify

### Configuration Loading
- ✅ YAML files are parsed correctly
- ✅ Relative configuration references are resolved
- ✅ Default values are applied when not specified
- ✅ Invalid YAML syntax is handled gracefully
- ✅ Missing files are handled with appropriate errors

### Strategy Registry Integration
- ✅ Strategy factories are registered correctly
- ✅ Strategy instances can be created from configuration
- ✅ Invalid strategy types/IDs are rejected
- ✅ Strategy validation works correctly

### Strategy Creation Method
- ✅ Method signature is correct (3 parameters)
- ✅ Strategy instances are created successfully
- ✅ Error handling works for invalid parameters
- ✅ Complex configuration structures are handled

### Strategy Instance Creation and Configuration Usage
- ✅ **SFTP loader instances** are created with correct SFTP protocol configurations
- ✅ **HTTP loader instances** are created with correct HTTP protocol configurations
- ✅ **SFTP locator instances** use the correct remote directories, file patterns, and SFTP configs
- ✅ **HTTP locator instances** use the correct URLs, rate limits, and HTTP configs
- ✅ **Protocol configurations** are resolved from relative config references
- ✅ **Strategy attributes** are set correctly from YAML configuration
- ✅ **Nested configurations** (filters, query params, headers) are handled properly
- ✅ **Multiple protocol types** can be used in the same configuration

## Expected Test Results

When all tests pass, you should see output like:
```
tests/test_integration/data_loader_core/test_config_integration.py::TestConfigIntegration::test_config_loader_initialization PASSED
tests/test_integration/data_loader_core/test_config_integration.py::TestConfigIntegration::test_load_config_from_yaml_file PASSED
tests/test_integration/data_loader_core/test_strategy_registry_integration.py::TestStrategyRegistryIntegration::test_strategy_registry_has_expected_factories PASSED
tests/test_integration/data_loader_core/test_strategy_creation_integration.py::TestStrategyCreationIntegration::test_create_strategy_from_config_signature PASSED
...
```

## Troubleshooting

### Import Errors
If you see import errors like `ModuleNotFoundError: No module named 'data_loader_core'`:
1. Make sure you're in the `services/loader` directory
2. Run `poetry install` to install dependencies
3. Ensure the pipeline-bus library is available

### Docker Container Errors
If tests fail with Docker-related errors:
1. Make sure Docker is running
2. Check that testcontainers is installed: `poetry add testcontainers`
3. Some tests can be run without containers by excluding the `localstack` marker

### Strategy Registry Errors
If strategy registry tests fail:
1. Check that all strategy factories are properly registered
2. Verify that the pipeline-bus library is up to date
3. Ensure all required dependencies are installed
