#!/bin/bash
# MOCC Test Runner Script

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "========================================="
echo "Running MOCC Tests"
echo "========================================="

# Build the test if needed
cd "$PROJECT_ROOT"
if [ ! -f "build/test_mocc" ]; then
    echo "Building test_mocc..."
    make -j4 2>/dev/null || {
        echo "Build may have warnings, continuing..."
    }
fi

# Run the MOCC unit tests
echo ""
echo "Running MOCC unit tests..."
./build/test_mocc

echo ""
echo "========================================="
echo "MOCC Tests Completed"
echo "========================================="




