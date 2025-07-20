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

# Test
pytest

# Test with coverage
pytest --cov=actions_package --cov-report=term-missing -v

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
- **Coverage Target**: >80% (currently ~69%)
- **Test Count**: 12 tests total
- **Test Categories**: Unit tests, parametrized tests

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

- Coverage at 69% (4 lines uncovered in hello.py main function)
- No runtime dependencies (minimal package)
- Tests run on Ubuntu only in CI

## AI Agent Hints

1. **Quick Start**: Use `pip install -e ".[dev]" && pytest`
2. **Add Tests**: Create test files in `tests/` with `test_` prefix
3. **Coverage**: Run `pytest --cov=actions_package` for coverage reports
4. **Container**: Use `docker-compose up dev` for isolated development
5. **Setup Script**: Run `./dev-setup.sh` for automated environment setup
