# Dockerfile for ice-stream development environment
# This provides a reproducible environment for AI coding agents and developers

FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy package configuration first for better caching
COPY pyproject.toml ./

# Upgrade pip
RUN pip install --upgrade pip

# Install build dependencies (with SSL retry handling)
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org build || \
    pip install build

# Copy source code
COPY src/ ./src/
COPY tests/ ./tests/
COPY README.md ./

# Install package in development mode
RUN pip install -e ".[dev]"

# Install optional development tools (with SSL retry handling)
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org ruff || \
    pip install ruff || echo "Ruff installation failed, continuing without it"

# Create a non-root user
RUN useradd -m -s /bin/bash developer && \
    chown -R developer:developer /app
USER developer

# Default command runs tests
CMD ["pytest", "--cov=ice_stream", "--cov-report=term-missing", "-v"]

# Build and run instructions:
# docker build -t ice-stream-dev .
# docker run -it ice-stream-dev
#
# For development with volume mount:
# docker run -it -v $(pwd):/app ice-stream-dev /bin/bash
