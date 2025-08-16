# Development Environment Guide

This document provides comprehensive build and development environment details for AI coding agents and developers working on the ice-stream package.

## Quick Environment Summary

- **Language**: Python 3.12
- **Build System**: setuptools with pyproject.toml
- **Test Framework**: pytest with coverage
- **CI/CD**: GitHub Actions
- **Package Manager**: pip
- **Development Dependencies**: pytest>=7.0, pytest-cov>=4.0

## Environment Setup

### System Requirements

- Python 3.12 or higher
- pip (Python package manager)
- Git

### Development Environment Setup

```bash
# 1. Clone the repository
git clone https://github.com/okz/actions.git
cd actions

# 2. Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install package in development mode with dependencies
pip install --upgrade pip
pip install -e ".[dev]"
```

## Build Process

### Package Structure
```
actions/
├── src/
│   └── ice_stream/
│       ├── __init__.py          # Package initialization
│       └── hello.py             # Main functionality
├── tests/
│   ├── __init__.py
│   └── test_hello.py            # Test suite
├── .github/
│   └── workflows/
│       └── ci.yml               # CI/CD pipeline
├── pyproject.toml               # Build configuration
├── README.md                    # User documentation
└── DEVELOPMENT.md               # This file
```

### Build Commands

```bash
# Install dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run tests with coverage
pytest --cov=ice_stream --cov-report=xml --cov-report=term-missing -v

# Run linting (optional)
pip install ruff
ruff check src/ tests/

# Build package
pip install build
python -m build
```

## Testing

### Test Framework Configuration

- **Framework**: pytest
- **Coverage**: pytest-cov
- **Configuration**: pyproject.toml `[tool.pytest.ini_options]`
- **Test Directory**: `tests/`
- **Coverage Source**: `src/`

### Running Tests

```bash
# Run all tests
pytest

# Run tests with verbose output
pytest -v

# Run tests with coverage report
pytest --cov=ice_stream --cov-report=term-missing

# Run specific test file
pytest tests/test_azurite_service.py
```

### Collecting Test Artifacts

Tests can record files such as generated data or plots using the
`artifacts` fixture. Files saved through this fixture are copied to an
`artifacts/` directory and a `manifest.json` file is written containing the
size of each artifact. The manifest is created even when a test fails so that
CI systems can inspect the results.

Example:

```python
def test_example(artifacts):
    artifacts.save_text("output.txt", "content")
```

### Current Test Coverage

- **Total Coverage**: ~69%
- **Files**: 2 source files
- **Test Cases**: ~12 tests
- **Test Categories**:
  - Azurite integration tests
  - Mock data generation tests

## Dependencies

### Runtime Dependencies
- None (pure Python package)

### Development Dependencies
- pytest>=7.0 (testing framework)
- pytest-cov>=4.0 (coverage reporting)

### Optional Dependencies
- ruff (linting)
- build (package building)

## CI/CD Pipeline

### GitHub Actions Workflow (.github/workflows/ci.yml)

**Triggers**:
- Push to main branch
- Pull requests to main branch
- Manual dispatch

**Jobs**:
1. **Test Job** (runs-on: ubuntu-latest)
   - Python 3.12 matrix
   - Install dependencies
   - Run linting (optional)
   - Run tests with coverage
   - Upload coverage reports

2. **Build Job** (runs-on: ubuntu-latest)
   - Build package
   - Upload build artifacts

### Build Commands in CI
```bash
python -m pip install --upgrade pip
pip install -e ".[dev]"
ruff check src/ tests/ || echo "Ruff check completed"
pytest --cov=ice_stream --cov-report=xml --cov-report=term-missing -v
python -m build
```

## Package Configuration

### pyproject.toml Key Sections

```toml
[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"

[project]
name = "ice-stream"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = []  # No runtime dependencies

[project.optional-dependencies]
dev = ["pytest>=7.0", "pytest-cov>=4.0"]

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py", "*_test.py"]
addopts = ["--strict-markers", "--strict-config", "--verbose"]
```

## Development Workflow

### Making Changes
1. Create feature branch
2. Make code changes in `src/ice_stream/`
3. Add/update tests in `tests/`
4. Run tests locally: `pytest`
5. Check coverage: `pytest --cov=ice_stream`
6. Submit pull request

### Code Quality
- Follow PEP 8 style guidelines
- Add docstrings to all functions
- Maintain test coverage above 80%
- All tests must pass in CI

## Troubleshooting

### Common Issues

1. **Import errors**: Ensure package is installed with `pip install -e ".[dev]"`
2. **Test failures**: Check Python version is 3.12+
3. **Coverage issues**: Ensure tests are in `tests/` directory
4. **Build failures**: Check pyproject.toml configuration

### Debug Commands
```bash
# Check package installation
pip list | grep ice-stream

# Check Python version
python --version

# Verify package import
python -c "import ice_stream; print(ice_stream.__version__)"

# Check test discovery
pytest --collect-only
```

## Package Usage

### As Python Module
```python
import ice_stream

print(ice_stream.__version__)
```

This comprehensive guide provides all the environment and build details needed for AI coding agents to work efficiently with this repository.
