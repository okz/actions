# Actions Package

A basic skeleton Python 3.12 package with pytest support, GitHub Actions CI/CD, and copilot coding agent compatibility.

## Features

- Python 3.12 support
- Modern `pyproject.toml` configuration
- Pytest testing framework
- GitHub Actions CI/CD pipeline
- Hello world functionality as an example
- Comprehensive test coverage

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

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## License

MIT License - see LICENSE file for details.
