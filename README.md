# ice-stream

Utilities for streaming Icechunk data with pytest support, GitHub Actions CI/CD, and copilot coding agent compatibility.

## Features

- Python 3.12 support
- Modern `pyproject.toml` configuration
- Pytest testing framework
- GitHub Actions CI/CD pipeline
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
   python -c "import ice_stream; print(ice_stream.__version__)"
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
pip install ice-stream
```

## Troubleshooting

### Common Setup Issues

#### Import Error: `ModuleNotFoundError: No module named 'ice_stream'`

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
pytest tests/test_azurite_service.py -v
```

## Usage

### As a Python module

```python
import ice_stream
from ice_stream import mock_data_generator

print(ice_stream.__version__)
```

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run tests with coverage
pytest --cov=ice_stream

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
pytest --cov=ice_stream --cov-report=term-missing -v
```

#### Manual Testing

Use the utilities in `tests/helpers.py` as a reference for interacting with the Azurite emulator.

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
│   └── ice_stream/
│       ├── __init__.py
│       ├── blocks.py
│       └── mock_data_generator.py
├── tests/
│   ├── __init__.py
│   └── test_azurite_service.py
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
docker build -t ice-stream-dev .
docker run -it -v $(pwd):/app ice-stream-dev /bin/bash
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



# TODO

[OK] **Setup Azurite dev stack** – Docker Compose file & README for local emulator. 
[OK] **Setup Codex and Copilot coding agents** capable emulating azurite/CI/testing etc. 
[OK] **Repo scaffolding with pip, venv folder structure** – create `src/`, `tests/`, `docs/` 
[OK] **Mocked data creator** Generate large files with a small seeded data amount. 

## Icechunk features: 

[OK] **Multi-dimensional append tests** – verify appends along first and second axes  
[OK] **File-size sanity checks** – assert on-disk chunk sizes stay within budget  
[OK] **Upload integrity validation** – compare local vs. remote hashes after push  
[OK] **Weekly write benchmark** – write a week of data in 15minute streams and get benchmarks:  1215second upload, 1.8 GB repo size. 
[OK] **Weekly read benchmark** – open one week of data and measure latency reading last 100 timestamps at the end: near instantaneous. Selectively load a days ch4 data 200ms. 
[OK] Size of repo, up for 24H minimal data, uploaded in 15minute increments. [Minimal Data just over 20MB]
[OK] Can we append variables to the same dimension later.
[OK] Can we append high_freq_timestamp data later. 
[OK] Can we append waveform data, which is timestamped, later. 
[!OK] Size of the uploaded data, once all the waveforms/high freq data is uploaded with 4H chunks, 24H single chunk. [Simulated data for a 3GB NetCDF was 1.2GB, check compression settings]


[OK] Create a few random files on the blob path. 
[OK] implement fast path finding that can search ${CLADS_BACKUP_UPLOAD_TARGET}/ for all icechunkl projects. 
[OK] methods to return iccechunk paths that are in the format:
<instrument>/<project>/inst-<instrument>-prj-<project>-<YYYY-MM-DDtHH-mm-SSz>l1b/
or 
<instrument>/<project>/inst-<instrument>-prj-<project>-<YYYY-MM-DDtHH-mm-SSz>l1bmin/

[OK] Start a Streaming class template using the old one. It's icechunk so remove the unnecessary functions.  In fact ask for variables/properties/methods only, no code. 

[] Clean the mock package template evolving to the Streaming class. 
[] Search these paths should be ordered for YYYY-MM-DDtHH-mm-SS so we open the latest one. 
[] Latest one is opened, extracting the timestamp. the Streaming Class keeps the dataset open and opens in append mode. 
[] If there is no access we exit 
[] Mock a backup for tests instead of clads.Backup that returns couple of mock zarr files and returns their paths in the format the backup module does. 
[] Class sorts them to dates and iterates streaming them. _stream.
[] _stream checks if is appendable. 
[] if is appendable we append retro dim first, then minimal timestamp data. These tasks har handled in _append()
[] if not appendable, we create new icechunk repo then call _append(). the repo name should follow <instrument>/<project>/<YYYY-MM-DDtHH-mm-SSz>-inst-<instrument>-prj-<project>-l1b/
[] 


[ ] **Monthly read scalability test** – load one month of data within memory limits  
[ ] File naming conventions 

[  ] **Azurite bandwidth metrics probe** – explore and log emulator network usage stats  
[ ] **Icechunk integration tests** – test writer, reader, promoter with Azurite  
[ ] **Icechunk schema validation** – ensure data schema is validated on write  
[ ] **Icechunk retention policy** – implement retention policy for old data  
[ ] **Icechunk deletion workflow** – script to delete old data and run garbage collection
[ ] **Crash-safety integration test** – kill writer mid-upload; ensure `main` is intact  
[ ] **Network-jitter harness** – `tc netem` (500 ms / 1 % loss) throughput ≥ 80 MB/min  
[ ] **Deletion workflow demo** – script to remove 1 h window, commit, expire, GC  
[ ] **Schema drift guard** – hook that blocks promotion on unexpected schema change  
[ ] **Chunk-shape optimiser** – benchmark different layouts vs. performance  
[ ] **Documentation: quick-start & run-book** – ops tasks, common commands  
[ ] **Server Test**: Scripts and instructions for validating server-side functionality and integration.
[ ] **Instrument Test**: Automated checks for hardware or service instrumentation, ensuring correct metrics and logging.
[ ] **Installer Package**: Tools and documentation for packaging and distributing the application, including setup scripts and dependency management.




# Open Questions: 



Does xarray connection figure out timeouts/connection issues?  

What do we do when we fail.. 
 
  
  magic number of retries? (e.g., 3 retries) 
  upload steps.  How do we know the step that failed. 
  need to know if no connection or if repo is missing. 

Whats the daily transfer size. 

Seperate branch / maybe even a seperate process for the waveforms/high volume data.
retro's appended first. (smallest mods first, minimal loss on connection issues)
we need to decide on timestamp starts, for both normal and high volume data? 

Just push all of the data. 


# Difficulties: 

   - Icechunk is git for data, except not really.. It can't merge (it's concept for merge is limited to parallel writes to different chunks)

   - Transactional features of icechunk makes some of the design decisions we have made unnecessary as well as difficult to follow.  
   
      Unneccessary:  
         - Local state file. (transactional) 
         - Day splits (petabytes of data in a single file
         - Seperate high volume data repo (append on two dimensions possible) 
      
      Difficult decisions. 
         - Continue with "local first" approach, or use the icechunk repo as the source of truth??
         - The decision where do we append / start needs to be resolved. Do we resolve everytime or keep the state file? 
         - If we don't do day splits, Is there risk on readers taking longer to open the data.
         - If we seperate the high volume data, opening them might be more complex with icechunk.
         - We had an issue if the instrument is stopped for a long time, streaming did not continue as it would kjeep findsing the last upload chunk and only add a day to it. #4610

      ## new approach ? 

      - get the last backup data to build the blob folder search path. <instrument/project>/[dates] 
      - use the last files timestamp to know the last timestamp uploaded. 
      - update the since_hint and until_hint parameters from the last timestamp uploaded.
      - limit the until_hint to 4hours (setting) per commit, to avoid large data transfer in one hit. 


Where do we upload ?? 

   Yes.... <instrument/project/date_for_fun>   whenever the <gas_id> <gas_version>changes.   So no change !
   decided against:    No.... <instrument/project/<gas_id>_<gas_version>/date_for_fun>

   



# Default Settings

default_streaming_settings = {
    'streaming_minutes': 30,
    'streaming_days_per_file': 1,
    # Add other default settings as needed
}





You’re designing an Icechunk upload/streamer.

It will accept a few inputs:

--since-hint (CLI): optional; if provided it may be used to skip forward (the system may choose to honor or ignore it).

-- Config settings (dict): used to locate the database/source that holds the data to be backed up.

-- 

-- Environment variable: provides the root folder for the cloud files (destination root in cloud storage).

