# Development Environment Guide

This document provides comprehensive build and development environment details for AI coding agents and developers working on the actions package.

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
│   └── actions_package/
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
pytest --cov=actions_package --cov-report=xml --cov-report=term-missing -v

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
pytest --cov=actions_package --cov-report=term-missing

# Run specific test file
pytest tests/test_hello.py

# Run specific test class
pytest tests/test_hello.py::TestHelloWorld

# Run specific test method
pytest tests/test_hello.py::TestHelloWorld::test_hello_world_default
```

### Current Test Coverage

- **Total Coverage**: ~69%
- **Files**: 2 source files
- **Test Cases**: 12 tests
- **Test Categories**: 
  - Unit tests for hello_world function
  - Unit tests for get_greeting_count function
  - Parametrized tests for edge cases

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
pytest --cov=actions_package --cov-report=xml --cov-report=term-missing -v
python -m build
```

## Package Configuration

### pyproject.toml Key Sections

```toml
[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"

[project]
name = "actions-package"
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
2. Make code changes in `src/actions_package/`
3. Add/update tests in `tests/`
4. Run tests locally: `pytest`
5. Check coverage: `pytest --cov=actions_package`
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
pip list | grep actions-package

# Check Python version
python --version

# Verify package import
python -c "from actions_package import hello_world; print(hello_world())"

# Check test discovery
pytest --collect-only
```

## Package Usage

### As Python Module
```python
from actions_package import hello_world

# Basic usage
print(hello_world())  # "Hello, World!"

# With custom name
print(hello_world("Python"))  # "Hello, Python!"
```

### As CLI Tool
```bash
python -m actions_package.hello
```

This comprehensive guide provides all the environment and build details needed for AI coding agents to work efficiently with this repository.
