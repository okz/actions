# Actions Package

A basic skeleton Python 3.12 package with pytest support, GitHub Actions CI/CD, and copilot coding agent compatibility.

## Features

- Python 3.12 support
- Modern `pyproject.toml` configuration
- Pytest testing framework
- GitHub Actions CI/CD pipeline
- Hello world functionality as an example
- Comprehensive test coverage
- **🔧 Complete development environment documentation**
- **🐳 Containerized development support**
- **🤖 AI coding agent optimized**

## Installation

### Development Installation

```bash
# Clone the repository
git clone https://github.com/okz/actions.git
cd actions

# Install in development mode with test dependencies
pip install -e ".[dev]"
```

### Production Installation

```bash
pip install actions-package
```

## Usage

### As a Python module

```python
from actions_package import hello_world

# Basic usage
print(hello_world())  # Output: Hello, World!

# With custom name
print(hello_world("Python"))  # Output: Hello, Python!
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
```

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
