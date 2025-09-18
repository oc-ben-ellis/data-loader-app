SHELL=/bin/bash
DOCKER=BUILDKIT_PROGRESS=plain docker
DOCKER_COMPOSE=USER_ID=$$(id -u) GROUP_ID=$$(id -g) BUILDKIT_PROGRESS=plain docker-compose
GIT_REPOSITORY_NAME=$$(basename `git rev-parse --show-toplevel`)
GIT_COMMIT_ID=$$(git rev-parse --short HEAD)

# Allocate a TTY for colorful output when stdout is a terminal
TTY_FLAG:=$(shell if [ -t 1 ]; then echo -t; else echo ; fi)

# Default parameter values
AWS_PROFILE ?= oc-management-dev
DATE ?= $(shell date +%Y%m%d)
ENV ?= play
SOURCE_ID ?=
AWS_REGION = eu-west-2
AWS_ACCOUNT_ID ?= $(shell aws sts get-caller-identity --query Account --output text --profile $(AWS_PROFILE))
ECR_REPOSITORY ?= data-loader-sftp

# Set LOCAL mode automatically if in container, unless explicitly overridden
ifeq ($(USER),vscode)
	# Dev Container environment (detected by USER=vscode)
	MODE ?= local
endif

# Define run commands based on mode
ifeq ($(MODE), local)
	RUN=poetry run
	RUN_NO_DEPS=poetry run
else
	# Reuse an already running dev-container; otherwise start it, then exec
	RUN=$(DOCKER_COMPOSE) exec $(TTY_FLAG) dev-container poetry run
	RUN_NO_DEPS=$(DOCKER_COMPOSE) exec $(TTY_FLAG) dev-container poetry run
endif

# Define a function to run commands in the appropriate environment
define run_in_container
	@if [ "$(MODE)" = "local" ]; then \
		poetry run $(1); \
	else \
		$(DOCKER_COMPOSE) exec $(TTY_FLAG) dev-container poetry run $(1); \
	fi
endef

# Default pytest arguments for test targets
PYTEST_ARGS=-v --tb=line

# User-provided arguments (can be overridden from command line)
ARGS=

# Default number of parallel workers for tests (auto-detect CPU cores)
# Parallel execution can significantly speed up test runs, especially on multi-core systems
# Use TEST_WORKERS=1 to run tests sequentially if you encounter issues
TEST_WORKERS ?= auto

.PHONY: all-checks build/for-deployment format lint test test/unit test/integration test/functional test/not-in-parallel test/parallel test/with-coverage test/snapshot-update run run/with-observability
.PHONY: lint/ruff lint/mypy help examples debug docs docs/open headers pre-commit pre-commit/init pre-commit/run pre-commit/run-all
.PHONY: setup build-pipeline clean-pipeline ensure-in-docker ensure-docker-compose

all-checks: format lint test/with-coverage

build/for-deployment:
	$(DOCKER) build -t "$(GIT_REPOSITORY_NAME):$(GIT_COMMIT_ID)" \
	--build-arg POETRY_HTTP_BASIC_OCPY_PASSWORD \
	.

# Check docker-compose availability, mounts, ensure devcontainer running and ready
ensure-docker-compose:
	@if [ "$(MODE)" != "local" ]; then \
		if [ -z "$$DOCKER_GID" ]; then \
			echo ""; \
			echo "❌ Error: DOCKER_GID environment variable is not set."; \
			echo ""; \
			echo "🔧 To fix this, run the following command to add DOCKER_GID to your shell configuration:"; \
			echo ""; \
			echo "   DOCKER_GID_LINE='export DOCKER_GID=\$$(stat -c \"%g\" /var/run/docker.sock); export UID=\$$(id -u); export GID=\$$(id -g)'; \\"; \
			echo "   for rc in ~/.bashrc ~/.zshrc; do \\"; \
			echo "     [ -f \"\$$rc\" ] || touch \"\$$rc\"; \\"; \
			echo "     sed -i '/export DOCKER_GID=/d' \"\$$rc\"; \\"; \
			echo "     printf '%s\\n' \"\$$DOCKER_GID_LINE\" >> \"\$$rc\"; \\"; \
			echo "   done"; \
			echo ""; \
			echo "   You may have to restart your IDE before you can open the devcontainer."; \
			echo ""; \
			echo "   Or manually run: export DOCKER_GID=\$$(stat -c \"%g\" /var/run/docker.sock); export UID=\$$(id -u); export GID=\$$(id -g)"; \
			echo ""; \
			echo "   Then restart your shell or run: source ~/.bashrc (or ~/.zshrc)"; \
			echo ""; \
			exit 1; \
		fi; \
		\
		if ! docker-compose version >/dev/null 2>&1; then \
			echo "Error: docker-compose (v1) not found. Please install docker-compose v1 (Note: docker V2 (docker compose) is not supported)."; \
			exit 1; \
		fi; \
		\
		MISSING_DIRS=""; \
		REQUIRED_DIRS="$$HOME/.gnupg $$HOME/.ssh $$HOME/.aws"; \
		for dir in $$REQUIRED_DIRS; do \
			if [ ! -d "$$dir" ]; then \
				MISSING_DIRS="$$MISSING_DIRS $$dir"; \
			fi; \
		done; \
		if [ -n "$$MISSING_DIRS" ]; then \
			echo ""; \
			echo "❌ Error: Required directories for docker-compose mounts are missing:"; \
			for dir in $$MISSING_DIRS; do \
				echo "   - $$dir"; \
			done; \
			echo ""; \
			echo "🔧 How to resolve:"; \
			echo "   1. Create the missing directories:"; \
			for dir in $$MISSING_DIRS; do \
				echo "      mkdir -p $$dir"; \
			done; \
			echo ""; \
			exit 1; \
		fi; \
		ID=`$(DOCKER_COMPOSE) ps -q dev-container || true`; \
		if [ -z "$$ID" ]; then \
			echo "Starting dev-container..."; \
			$(DOCKER_COMPOSE) up -d dev-container; \
		else \
			echo "Reusing running dev-container $$ID"; \
		fi; \
		echo "Ensuring dev-container has dependencies installed..."; \
		$(DOCKER_COMPOSE) exec $(TTY_FLAG) dev-container bash -lc 'poetry run ruff --version >/dev/null 2>&1 || poetry install --no-root'; \
	fi

format: ensure-docker-compose
	$(call run_in_container,ruff format .)
	$(call run_in_container,ruff check --fix .)

headers: ensure-docker-compose
	@echo "Adding standard headers to Python files..."
	$(call run_in_container,python tmp/add_headers.py)
	@echo "Headers added. Use 'make headers/dry-run' to preview changes."

headers/dry-run: ensure-docker-compose
	@echo "Previewing header changes (dry run)..."
	$(call run_in_container,python tmp/add_headers.py --dry-run)

pre-commit: ensure-docker-compose
	@echo "Installing pre-commit hooks..."
	$(call run_in_container,pre-commit install)
	@echo "Pre-commit hooks installed. They will run automatically on commit."

pre-commit/init: ensure-docker-compose
	@echo "Initializing pre-commit environments..."
	$(call run_in_container,pre-commit install-hooks)
	@echo "Pre-commit environments initialized. First run will be much faster."

pre-commit/run: ensure-docker-compose
	@echo "Running pre-commit on staged files..."
	$(call run_in_container,pre-commit run)

pre-commit/run-all: ensure-docker-compose
	@echo "Running pre-commit on all files..."
	$(call run_in_container,pre-commit run --all-files)

lint: lint/ruff lint/mypy

lint/ruff: ensure-docker-compose
	$(call run_in_container,ruff check .)

lint/mypy: ensure-docker-compose
	$(call run_in_container,mypy .)

test: test/unit test/integration test/functional
	@echo "All tests completed successfully"

test/not-in-parallel: ensure-docker-compose
	$(call run_in_container,pytest $(PYTEST_ARGS))

test/parallel: ensure-docker-compose
	$(call run_in_container,pytest $(PYTEST_ARGS) -n $(TEST_WORKERS))

test/with-coverage: ensure-docker-compose
	$(call run_in_container,coverage run -m pytest $(PYTEST_ARGS) -n $(TEST_WORKERS))
	$(call run_in_container,coverage html --fail-under=0)
	@echo "Coverage report at file://$(PWD)/tmp/htmlcov/index.html"
	$(call run_in_container,coverage report)

test/unit: ensure-docker-compose
	$(call run_in_container,pytest $(PYTEST_ARGS) -n $(TEST_WORKERS) tests/test_unit/)

test/integration: ensure-docker-compose
	$(call run_in_container,pytest $(PYTEST_ARGS) -n $(TEST_WORKERS) tests/test_integration/)

test/functional: ensure-docker-compose
	$(call run_in_container,pytest $(PYTEST_ARGS) -n $(TEST_WORKERS) tests/test_functional/)

run: ensure-docker-compose
ifeq ($(MODE),local)
	$(RUN) python -m data_loader.main $(ARGS)
else
	# Use docker-compose to run the app
	$(DOCKER_COMPOSE) run --rm app-runner poetry run python -m data_loader.main $(ARGS)
endif

run/with-observability:
ifeq ($(MODE),local)
	echo $(MODE)
	$(error $@ not available in MODE=$(MODE))
else
	@echo "Running with observability..."
	$(DOCKER_COMPOSE) run --rm app-runner poetry run python -m data_loader.main $(ARGS)
endif

help:
	@echo "Available commands:"
	@echo "  all-checks          - Run format, lint, and tests with coverage"
	@echo "  build/for-deployment - Build Docker image for deployment"
	@echo "  format              - Format code with ruff"
	@echo "  headers             - Add standard headers to Python files"
	@echo "  pre-commit          - Install pre-commit hooks for automatic checks"
	@echo "  pre-commit/init     - Initialize pre-commit environments (faster first run)"
	@echo "  pre-commit/run      - Run pre-commit on staged files"
	@echo "  pre-commit/run-all  - Run pre-commit on all files"
	@echo "  lint                - Run all linters"
	@echo "  test                - Run tests in parallel (default, faster)"
	@echo "  test/unit           - Run unit tests only"
	@echo "  test/integration    - Run integration tests only"
	@echo "  test/functional     - Run functional tests only"
	@echo "  test/fast           - Run only fast tests (excludes slow tests)"
	@echo "  test/slow           - Run only slow tests (takes >30 seconds)"
	@echo "  test/integration-fast - Run fast integration tests only"
	@echo "  test/integration-slow - Run slow integration tests only"
	@echo "  test/not-in-parallel - Run tests sequentially (fallback)"
	@echo "  test/parallel      - Run tests in parallel (explicit)"
	@echo "  test/with-coverage - Run tests with coverage report (parallel)"
	@echo "  run                 - Run the loader (use ARGS=<loader_id>)"
	@echo "  run/with-observability - Run with observability features"
	@echo "  docs                - Build HTML documentation from markdown files"
	@echo "  docs/serve          - Start development server (accessible from host)"
	@echo "  docs/serve/local    - Start development server locally"
	@echo "  docs/build          - Build documentation locally"
	@echo "  docs/clean          - Clean build artifacts"
	@echo "  docs/open           - Build and open documentation in browser"
	@echo "  docs/validate       - Validate MkDocs configuration"
	@echo "  docs/check-links    - Check for broken links"
	@echo "  docs/deploy         - Deploy to GitHub Pages"
	@echo "  docs/help           - Show detailed documentation help"
	@echo "  examples            - Run example scripts"
	@echo "  debug               - Show environment detection and mode settings"
	@echo ""
	@echo "Mode detection:"
	@echo "  - Automatically detects container environments (Docker, DevContainer, etc.)"
	@echo "  - Uses LOCAL mode (poetry run) when in containers"
	@echo "  - Can be overridden with MODE=local or MODE=docker"
	@echo ""
	@echo "Usage examples:"
	@echo "  make run ARGS=us-il"
	@echo "  make test"
	@echo "  make test/unit"
	@echo "  make test/integration"
	@echo "  make test/functional"
	@echo "  make build-pipeline"
	@echo "  make test/not-in-parallel"
	@echo "  make test/parallel TEST_WORKERS=4"
	@echo "  make test PYTEST_ARGS='-v -s tests/test_loader.py'"
	@echo "  make MODE=local run ARGS=us-fl"
	@echo "  make MODE=docker run ARGS=us-fl"
	@echo ""
	@echo "Test parallelization:"
	@echo "  TEST_WORKERS=auto  - Auto-detect CPU cores (default)"
	@echo "  TEST_WORKERS=4     - Use 4 parallel workers"
	@echo "  TEST_WORKERS=1     - Run tests sequentially"
	@echo ""
	@echo "Argument variables:"
	@echo "  ARGS               - User arguments for non-test targets (e.g., loader IDs)"
	@echo "  PYTEST_ARGS        - Pytest arguments (default: -v --tb=line, can be overridden)"

examples: ensure-docker-compose
	$(call run_in_container,python examples/using_config_system.py)

debug:
	@echo "Environment detection:"
	@echo "  USER: $(USER)"
	@echo "  TERM_PROGRAM: $(TERM_PROGRAM)"
	@echo "  /.dockerenv exists: $$(shell ls /.dockerenv >/dev/null 2>&1 && echo yes || echo no)"
	@echo ""
	@echo "Mode settings:"
	@echo "  MODE: $(MODE)"
	@echo "  RUN command: $(RUN)"
	@echo "  RUN_NO_DEPS command: $(RUN_NO_DEPS)"
	@echo "  DOCKER_COMPOSE command: $(DOCKER_COMPOSE)"

# Documentation targets
docs:
	@echo "Building documentation with TechDocs Docker (no bind mounts)..."
	rm -rf site
	bin/techdocs-build-docker.sh

docs/serve:
	@echo "Starting TechDocs (mkdocs serve) via Docker on http://localhost:8000"
	@echo "Press Ctrl+C to stop the server"
	bin/techdocs-build-docker.sh --serve --port 8000

docs/serve/local:
	@echo "Starting TechDocs (mkdocs serve) via Docker on http://127.0.0.1:8000"
	@echo "Press Ctrl+C to stop the server"
	bin/techdocs-build-docker.sh --serve --port 8000

docs/build:
	@echo "Building documentation with TechDocs Docker..."
	rm -rf site
	bin/techdocs-build-docker.sh

docs/clean:
	@echo "Cleaning documentation build artifacts..."
	rm -rf site/
	rm -rf docs/rendered/

docs/open:
	@echo "Building and opening documentation in browser..."
	rm -rf site
	bin/techdocs-build-docker.sh
	@if command -v xdg-open >/dev/null 2>&1; then \
		xdg-open site/index.html; \
	elif command -v open >/dev/null 2>&1; then \
		open site/index.html; \
	else \
		echo "Please open site/index.html in your browser"; \
	fi

docs/validate:
	@echo "Validating MkDocs configuration (strict) via TechDocs Docker..."
	rm -rf site
	bin/techdocs-build-docker.sh
	@test -f site/index.html
	@echo "✅ Documentation validation passed"

docs/check-links:
	@echo "Checking for broken links in documentation..."
	rm -rf site
	bin/techdocs-build-docker.sh
	@if command -v linkchecker >/dev/null 2>&1; then \
		linkchecker site/index.html --ignore-url="^http://127.0.0.1" --ignore-url="^http://localhost"; \
	else \
		echo "⚠️  linkchecker not installed. Install with: pip install linkchecker"; \
		echo "   Or use: poetry add --group dev linkchecker"; \
		exit 1; \
	fi

docs/deploy:
	@echo "Deploying documentation to GitHub Pages..."
	# Build with TechDocs Docker first to validate
	rm -rf site
	bin/techdocs-build-docker.sh
	# Use mkdocs to publish to gh-pages branch
	$(RUN_NO_DEPS) mkdocs gh-deploy --force


docs/help:
	@echo "Documentation targets:"
	@echo "  docs              - Build documentation (in container)"
	@echo "  docs/serve        - Start development server (in container, accessible from host)"
	@echo "  docs/serve/local  - Start development server locally"
	@echo "  docs/build        - Build documentation locally"
	@echo "  docs/clean        - Clean build artifacts"
	@echo "  docs/open         - Build and open in browser"
	@echo "  docs/validate     - Validate MkDocs configuration"
	@echo "  docs/check-links  - Check for broken links"
	@echo "  docs/deploy       - Deploy to GitHub Pages"
	@echo "  docs/help         - Show this help"
	@echo ""
	@echo "Test environment:"
	@echo "  test-env-up        - Start local test environment (localstack, redis, buckets, queues)"
	@echo "  test-env-down      - Stop local test environment"
	@echo "Development workflow:"
	@echo "  1. make docs/serve        - Start development server"
	@echo "  2. Edit documentation in docs/ directory"
	@echo "  3. View changes at http://0.0.0.0:8000"
	@echo "  4. make docs/validate     - Validate before committing"
	@echo "  5. make docs/deploy       - Deploy to GitHub Pages"

.PHONY: test-env-up test-env-down

test-env-up:
	@echo "Starting local test environment..."
	bash scripts/test-env-up.sh

test-env-down:
	@echo "Stopping local test environment..."
	bash scripts/test-env-down.sh
