# Environment Specification for AI Coding Agents

## Quick Reference

**Project Type**: Python Package  
**Python Version**: 3.12  
**Build System**: setuptools + pyproject.toml  
**Test Framework**: pytest  
**Package Manager**: pip  
**CI/CD**: GitHub Actions  

## One-Line Setup
```bash
# Minimal environment (works everywhere)
pip install -e ".[dev]" && pytest -m "not azurite and not external_service and not slow"

# Full environment (requires external services)
pip install -e ".[dev]" && pytest
```

## Environment Variables
- `PYTHONPATH`: Not required (package installed in development mode)
- `PYTEST_CURRENT_TEST`: Set by pytest during test execution

## Essential Commands

### Development
```bash
# Setup
pip install -e ".[dev]"

# Test (minimal - works in all environments)
pytest -m "not azurite and not external_service and not slow"

# Test (full - requires external services like Azurite)
pytest

# Test with coverage (minimal environment)
pytest -m "not azurite and not external_service and not slow" --cov=actions_package --cov-report=term-missing -v

# Test with tox (multiple environments)
tox -e py312-minimal    # Minimal environment
tox -e py312-fast       # Fast tests only
tox -e py312            # All tests

# Lint (optional)
ruff check src/ tests/

# Build
python -m build
```

### Container Development
```bash
# Build container
docker build -t actions-package-dev .

# Run tests in container
docker run actions-package-dev

# Interactive development
docker run -it -v $(pwd):/app actions-package-dev /bin/bash

# Using Docker Compose
docker-compose up dev    # Interactive development
docker-compose up test   # Run tests
docker-compose up lint   # Run linting
```

## File Structure Context

```
actions/
├── src/actions_package/     # Source code
│   ├── __init__.py         # Package exports
│   └── hello.py            # Main module
├── tests/                  # Test suite
│   └── test_hello.py       # Test file
├── pyproject.toml          # Build configuration
├── DEVELOPMENT.md          # Comprehensive dev guide
├── Dockerfile              # Container specification
├── docker-compose.yml      # Container orchestration
└── dev-setup.sh           # Automated setup script
```

## Testing Context

- **Test Discovery**: Automatic via pytest
- **Test Pattern**: `test_*.py` files in `tests/`
- **Test Categories**: 
  - Minimal (6 tests): Import tests, mock data generation - no external dependencies
  - Full (12 tests): All tests including Azurite storage integration  
- **Test Selection**: Use pytest markers to run subsets
  - `pytest -m "not azurite and not external_service and not slow"` - minimal environment
  - `pytest -m "not slow"` - fast tests only
  - `pytest` - all tests
- **Coverage Target**: >80% overall, >90% for core functionality (mock data generator: 100%)

## Dependencies Context

**Runtime**: None (pure Python)  
**Development**: pytest>=7.0, pytest-cov>=4.0  
**Optional**: ruff (linting), build (packaging)  

## CI/CD Context

**Platform**: GitHub Actions  
**Trigger**: Push/PR to main, manual dispatch  
**Runner**: ubuntu-latest  
**Python Matrix**: 3.12  
**Workflow**: test → build → upload artifacts  

## Known Issues/Limitations

- Some tests require Azurite storage emulator (automatically skipped if not available)
- Large file generation tests (700MB) marked as slow and skipped in fast test runs
- Tests gracefully degrade based on available services
- Minimal environment supports 6 essential tests, full environment supports all 12 tests

## AI Agent Hints

1. **Quick Start**: Use `pip install -e ".[dev]" && pytest -m "not azurite and not external_service and not slow"`
2. **Minimal Testing**: For codex/agent environments, use marker-based test selection
3. **Add Tests**: Create test files in `tests/` with `test_` prefix, use appropriate markers
4. **Coverage**: Run `pytest --cov=actions_package` for coverage reports
5. **Container**: Use `docker-compose up dev` for isolated development
6. **Setup Script**: Run `./dev-setup.sh` for automated environment setup
7. **Test Selection**: See TESTING.md for complete guide on running tests in different environments
