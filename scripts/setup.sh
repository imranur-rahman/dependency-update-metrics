#!/bin/bash

# Dependency Metrics - Setup Script

echo "========================================"
echo "Dependency Metrics - Setup"
echo "========================================"

# Check Python version
echo ""
echo "Checking Python version..."
python3 --version

if [ $? -ne 0 ]; then
    echo "❌ Python 3 is not installed. Please install Python 3.8 or higher."
    exit 1
fi

# Check npm (optional but recommended for npm ecosystem)
echo ""
echo "Checking npm installation..."
npm --version

if [ $? -ne 0 ]; then
    echo "⚠️  npm is not installed. You'll need it for npm ecosystem analysis."
    echo "   Install Node.js from https://nodejs.org/"
else
    echo "✅ npm is installed"
fi

# Install dependencies
echo ""
echo "Installing dependencies..."
pip3 install -r requirements.txt

if [ $? -ne 0 ]; then
    echo "❌ Failed to install dependencies"
    exit 1
fi

# Install package in development mode
echo ""
echo "Installing package in development mode..."
pip3 install -e .

if [ $? -ne 0 ]; then
    echo "❌ Failed to install package"
    exit 1
fi

# Verify installation
echo ""
echo "Verifying installation..."
dependency-metrics --help

if [ $? -ne 0 ]; then
    echo "❌ Installation verification failed"
    exit 1
fi

echo ""
echo "========================================"
echo "✅ Setup completed successfully!"
echo "========================================"
echo ""
echo "Try running an example:"
echo "  dependency-metrics --ecosystem npm --package express"
echo ""
echo "For more examples, see:"
echo "  python3 examples/usage_examples.py"
echo ""
echo "For detailed usage, see USAGE.md"
echo "========================================"
