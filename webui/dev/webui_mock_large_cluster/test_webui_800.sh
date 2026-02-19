#!/bin/bash
# Quick start script for testing WebUI with 800 mocked instances

set -e

echo "================================================"
echo "WebUI Mock Cluster Test - Quick Start"
echo "================================================"

# Check if picodata is running
if ! curl -s http://localhost:8080/api/v1/config > /dev/null 2>&1; then
    echo ""
    echo "⚠️  Picodata is not running on port 8080!"
    echo ""
    echo "Please start it first with:"
    echo "  ./target/debug/picodata run --http-listen 0.0.0.0:8080"
    echo ""
    exit 1
fi

echo "✓ Picodata backend detected on port 8080"
echo ""

# Check dependencies
if ! python3 -c "import flask, requests" 2>/dev/null; then
    echo "Installing dependencies..."
    pip install -q -r test_webui_requirements.txt || {
        echo "Failed to install dependencies"
        exit 1
    }
    echo "✓ Dependencies installed"
fi

echo "Starting mock server with:"
echo "  - 800 instances total"
echo "  - Every 5th instance offline (160 offline, 640 online)"
echo "  - Running on http://localhost:8888"
echo ""
echo "================================================"
echo ""

exec python3 test_webui_mock_cluster.py \
    --backend http://localhost:8080 \
    --port 8888 \
    --instances 800 \
    --failure-rate 5
