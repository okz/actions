#!/bin/bash

# Development Environment Setup Script
# This script automates the setup process for the ice-stream development environment

set -e

echo "🚀 Setting up ice-stream development environment..."

# Check Python version
echo "📋 Checking Python version..."
python_version=$(python --version 2>&1 | cut -d' ' -f2)
required_version="3.12"

if ! python -c "import sys; exit(0 if sys.version_info >= (3, 12) else 1)"; then
    echo "❌ Error: Python 3.12 or higher is required. Current version: $python_version"
    echo "Please install Python 3.12+ and try again."
    exit 1
fi

echo "✅ Python version: $python_version"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "🐍 Creating virtual environment..."
    python -m venv venv
    echo "✅ Virtual environment created"
else
    echo "📁 Virtual environment already exists"
fi

# Activate virtual environment
echo "🔄 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "📦 Upgrading pip..."
pip install --upgrade pip

# Install package in development mode
echo "🔧 Installing package in development mode..."
pip install -e ".[dev]"

# Install optional development tools
echo "🛠️  Installing optional development tools..."
pip install ruff build

# Verify installation
echo "✅ Verifying installation..."
python -c "import ice_stream; print('Package import successful:', ice_stream.__version__)"

# Run tests to verify everything works
echo "🧪 Running tests to verify setup..."
pytest --cov=ice_stream --cov-report=term-missing -v

echo ""
echo "🎉 Development environment setup complete!"
echo ""
echo "📝 Next steps:"
echo "1. Activate the virtual environment: source venv/bin/activate"
echo "2. Run tests: pytest"
echo "3. Start coding in src/ice_stream/"
echo "4. Add tests in tests/"
echo ""
echo "📚 For more information, see DEVELOPMENT.md"
