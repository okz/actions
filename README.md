# Actions Package

A basic skeleton Python 3.12 package with pytest support, GitHub Actions CI/CD, and copilot coding agent compatibility.

## Features

- Python 3.12 support
- Modern `pyproject.toml` configuration
- Pytest testing framework
- GitHub Actions CI/CD pipeline
- Hello world functionality as an example
- Azure Storage integration with Azurite emulator support
- Comprehensive test coverage
- **🔧 Complete development environment documentation**
- **🐳 Containerized development support**
- **🤖 AI coding agent optimized**

## Development Setup

### Prerequisites

- Python 3.8+ (Python 3.12+ recommended)
- Git

### Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/okz/actions.git
   cd actions
   ```

2. **Create and activate a virtual environment**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install the package in development mode**
   ```bash
   pip install -e .[dev]
   ```

4. **Verify the installation**
   ```bash
   python -c "from actions_package import hello_world; print(hello_world())"
   pytest tests/ -v
   ```

### Alternative Setup with Tox

For testing across multiple Python versions:

```bash
# Install tox
pip install tox

# Run tests across all configured environments
tox

# Run tests for specific Python version
tox -e py312

# Run linting only
tox -e lint
```

### Available Make Commands

```bash
make install     # Install package in development mode
make test        # Run tests with pytest
make test-cov    # Run tests with coverage
make lint        # Run code linting with ruff
make format      # Format code with black
make clean       # Clean build artifacts
make tox         # Run all tox environments
```

## Installation

### Development Installation (Recommended)

If you're planning to contribute or modify the code:

```bash
# Clone and setup development environment
git clone https://github.com/okz/actions.git
cd actions
python3 -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

### Production Installation

For using the package in your projects:

```bash
pip install actions-package
```

## Troubleshooting

### Common Setup Issues

#### Import Error: `ModuleNotFoundError: No module named 'actions_package'`

This usually means the package wasn't installed in development mode. Make sure you run:

```bash
pip install -e .[dev]
```

#### Missing Dependencies in Tests

If tests fail due to missing packages like `numpy`, ensure you installed with dev dependencies:

```bash
pip install -e .[dev]
```

#### Python Version Compatibility

The package requires Python 3.8+. Check your Python version:

```bash
python --version
```

If you have multiple Python versions, ensure you're using the correct one:

```bash
python3.12 -m venv .venv  # Use specific Python version
```

#### Virtual Environment Issues

If you're having issues with your virtual environment:

```bash
# Remove existing environment
rm -rf .venv

# Create fresh environment
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .[dev]
```

#### Running Tests

If tests aren't being discovered:

```bash
# Run from project root
pytest tests/ -v

# Or run specific test file
pytest tests/test_hello.py -v
```

## Usage

### As a Python module

```python
from actions_package import hello_world, AzuriteStorageClient

# Basic usage
print(hello_world())  # Output: Hello, World!

# With custom name
print(hello_world("Python"))  # Output: Hello, Python!

# Azure Storage operations (requires Azurite running)
client = AzuriteStorageClient()
client.create_container()
client.upload_blob("test.txt", "Hello, Azure Storage!")
content = client.download_blob("test.txt")
print(content)  # Output: Hello, Azure Storage!
```

### As a CLI tool

```bash
# Run the main function
python -m actions_package.hello
```

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run tests with coverage
pytest --cov=actions_package

# Run tests with verbose output
pytest -v

# Run only Azure Storage tests (requires Azurite)
pytest tests/test_azurite_service.py -v
```

### Testing with Azurite

This project includes Azure Storage functionality that can be tested using the Azurite emulator.

#### Installing and Running Azurite

1. **Using Docker (Recommended)**:
   ```bash
   # Pull and run Azurite container
   docker run -d --name azurite -p 10000:10000 -p 10001:10001 -p 10002:10002 mcr.microsoft.com/azure-storage/azurite:latest
   ```

2. **Using npm**:
   ```bash
   # Install Azurite globally
   npm install -g azurite
   
   # Run Azurite
   azurite --silent --location /tmp/azurite --debug /tmp/azurite/debug.log
   ```

#### Running Azure Storage Tests

Once Azurite is running, you can test the Azure Storage functionality:

> **Note**: When running `pytest` locally, a lightweight Azurite instance will
> be started automatically using `npx azurite` if nothing is already listening on
> port `10000`. This allows the storage tests to run out of the box without any
> additional setup.

```bash
# Run Azure Storage tests
pytest tests/test_azurite_service.py -v

# Run all tests including Azure Storage
pytest --cov=actions_package --cov-report=term-missing -v
```

#### Manual Testing

```python
from actions_package import AzuriteStorageClient

# Connect to Azurite (default connection)
client = AzuriteStorageClient()

# Create a container
client.create_container()

# Upload a blob
client.upload_blob("example.txt", "Hello, Azurite!")

# List blobs
blobs = client.list_blobs()
print(f"Blobs: {blobs}")

# Download a blob
content = client.download_blob("example.txt")
print(f"Content: {content}")

# Delete a blob
client.delete_blob("example.txt")
```

#### Azurite Connection Details

The default Azurite connection uses these endpoints:
- **Blob Service**: `http://127.0.0.1:10000/devstoreaccount1`
- **Queue Service**: `http://127.0.0.1:10001/devstoreaccount1`
- **Table Service**: `http://127.0.0.1:10002/devstoreaccount1`

**Account Name**: `devstoreaccount1`
**Account Key**: `Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==`

### Project Structure

```
actions/
├── src/
│   └── actions_package/
│       ├── __init__.py
│       └── hello.py
├── tests/
│   ├── __init__.py
│   └── test_hello.py
├── .github/
│   └── workflows/
│       └── ci.yml
├── pyproject.toml
└── README.md
```

## Development Environment

For AI coding agents and developers, this repository provides comprehensive environment documentation:

- **📋 [DEVELOPMENT.md](DEVELOPMENT.md)** - Complete development guide with build details
- **📑 [AGENTS.md](AGENTS.md)** - Instructions for Codex and GitHub's coding agent
- **🤖 [AI_ENVIRONMENT.md](AI_ENVIRONMENT.md)** - Quick reference for AI coding agents
- **🐳 [Dockerfile](Dockerfile)** - Reproducible container environment
- **⚙️ [dev-setup.sh](dev-setup.sh)** - Automated development setup script

### Quick Setup

```bash
# Automated setup
./dev-setup.sh

# OR manual setup
pip install -e ".[dev]"
pytest
```

### Container Development

```bash
# Using Docker Compose
docker-compose up dev    # Interactive development
docker-compose up test   # Run tests
docker-compose up lint   # Run linting

# Using Docker directly
docker build -t actions-package-dev .
docker run -it -v $(pwd):/app actions-package-dev /bin/bash
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

See [DEVELOPMENT.md](DEVELOPMENT.md) for detailed development guidelines.

## License

MIT License - see LICENSE file for details.
