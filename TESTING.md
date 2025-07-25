# Testing Guide

This document explains how to run tests in different environments and understand the test categories.

## Quick Start

```bash
# Install dependencies and run all tests
pip install -e ".[dev]" && pytest

# Run only basic tests (no external dependencies)
pytest -m "not azurite and not external_service and not slow"

# Run with coverage
pytest --cov=actions_package --cov-report=term-missing -v
```

## Test Categories

Tests are organized into categories using pytest markers:

### Markers
- `azurite`: Tests that require Azurite storage emulator
- `external_service`: Tests that require external services
- `slow`: Tests that take significant time to run (>30 seconds)
- `integration`: End-to-end integration tests

### Test Selection Examples

```bash
# Run only import and unit tests (minimal environment)
pytest -m "not azurite and not external_service and not slow"

# Run all tests except slow ones
pytest -m "not slow"

# Run only Azurite-dependent tests (requires Azurite service)
pytest -m "azurite"

# Run integration tests
pytest -m "integration"
```

## Environment Requirements

### Minimal Environment (Codex/Agent/Basic Dev)
**Requirements:** Python 3.12, basic package dependencies  
**Tests:** 6 tests (import tests, mock data generator unit tests)  
**Command:** `pytest -m "not azurite and not external_service and not slow"`

**What runs:**
- Basic package import tests
- Mock data generation tests (file-based, no external services)
- Parameter validation tests

### Standard Environment (Full Dev Environment)
**Requirements:** Python 3.12, all dependencies, Docker (optional)  
**Tests:** All fast tests (excludes slow 700MB file generation)  
**Command:** `pytest -m "not slow"`

**What runs:**
- All minimal environment tests
- Azurite storage tests (if Azurite available)
- Integration tests (if services available)

### Full Environment (CI/Integration)
**Requirements:** Python 3.12, all dependencies, Docker/Azurite  
**Tests:** All tests including slow ones  
**Command:** `pytest` (all tests)

**What runs:**
- All tests including large file generation tests

## Running Tests with Tox

```bash
# Run tests in minimal environment
tox -e py312-minimal

# Run fast tests (no slow tests)
tox -e py312-fast

# Run integration tests only
tox -e py312-integration

# Run all tests
tox -e py312
```

## External Service Setup

### Azurite (Azure Storage Emulator)

#### Option 1: Docker (Recommended)
```bash
# Start Azurite
docker run -d --name azurite \
  -p 10000:10000 -p 10001:10001 -p 10002:10002 \
  mcr.microsoft.com/azure-storage/azurite:latest \
  azurite --blobHost 0.0.0.0 --skipApiVersionCheck --silent

# Run tests
pytest

# Stop Azurite
docker stop azurite && docker rm azurite
```

#### Option 2: NPX (Node.js)
```bash
# Install and start Azurite
npx azurite --skipApiVersionCheck --silent --location /tmp/azurite &

# Run tests
pytest

# Stop Azurite
pkill -f azurite
```

## Test Behavior by Environment

### When Azurite is NOT available:
- Azurite-dependent tests are automatically skipped
- Basic functionality tests still run
- No test failures due to missing services

### When Azurite IS available:
- All tests run (unless excluded by markers)
- Integration tests with Azure storage emulation
- End-to-end data pipeline tests

## Troubleshooting

### Tests hang or timeout
- Usually indicates Azurite connection issues
- Check if Azurite is running: `nc -z localhost 10000`
- Run minimal tests to verify basic functionality

### Import errors
- Ensure package is installed: `pip install -e ".[dev]"`
- Check Python version: requires Python 3.12+

### Network issues in CI
- Use cached Docker images for Azurite
- Consider test selection based on environment capabilities

## Coverage Information

```bash
# Run with coverage reporting
pytest --cov=actions_package --cov-report=term-missing -v

# Coverage by test category
pytest -m "not azurite and not external_service and not slow" --cov=actions_package
pytest -m "azurite" --cov=actions_package
```

Current coverage targets:
- Core functionality: >90%
- Overall package: >80%