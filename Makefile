# Makefile for actions-package development
# Provides common automation commands for AI coding agents and developers

.PHONY: help install install-dev test test-cov lint build clean docker-build docker-test docker-dev setup

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install package in production mode
	pip install .

install-dev: ## Install package in development mode with dev dependencies
	pip install -e ".[dev]"

test: ## Run tests
	pytest

test-cov: ## Run tests with coverage report
	pytest --cov=actions_package --cov-report=term-missing -v

lint: ## Run linting with ruff
	ruff check src/ tests/

build: ## Build package distributions
	python -m build

clean: ## Clean build artifacts and cache
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

docker-build: ## Build Docker development image
	docker build -t actions-package-dev .

docker-test: ## Run tests in Docker container
	docker run --rm actions-package-dev

docker-dev: ## Start interactive Docker development environment
	docker run -it --rm -v $(PWD):/app actions-package-dev /bin/bash

setup: ## Complete development environment setup
	./dev-setup.sh

# Quick reference for AI coding agents:
# make install-dev  # Setup development environment
# make test-cov     # Run tests with coverage
# make lint         # Check code style
# make build        # Build package
