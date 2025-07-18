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
pytest tests/test_azure_storage.py -v
```

### Testing with Azurite

This project includes Azure Storage functionality that can be tested using the Azurite emulator.

#### Installing and Running Azurite

1. **Using Docker (Recommended)**:
   ```bash
   # Pull and run Azurite container
   docker run -d --name azurite \
     -p 10000:10000 \
     -p 10001:10001 \
     -p 10002:10002 \
     mcr.microsoft.com/azure-storage/azurite:latest
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

```bash
# Run Azure Storage tests
pytest tests/test_azure_storage.py -v

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

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## License

MIT License - see LICENSE file for details.
