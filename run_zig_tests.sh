#!/bin/bash
# Script to run Zig tests with visible output

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================="
echo "Running Zig Unit Tests"
echo "=========================================="
echo ""

echo "1. Running basic unit tests (test_utils.zig)..."
zig test src/test_utils.zig
echo ""

echo "2. Running memory safety tests (memory_safety_test.zig)..."
zig test src/bm25/memory_safety_test.zig -lc
echo ""

echo "=========================================="
echo "All Zig tests completed successfully!"
echo "=========================================="

