#!/usr/bin/env python3
"""
Basic test script for DuckDBX.

This script demonstrates:
1. Starting a DuckDB container
2. Running a simple query
3. Stopping the container

Usage:
    python examples/basic_test.py
"""

import sys
import os

# Add parent directory to path to import duckdbx
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from duckdbx import DuckDBX


def test_context_manager():
    """Test using DuckDBX as a context manager."""
    print("Testing context manager pattern...")
    
    with DuckDBX() as db:
        print(f"✓ Container started: {db.is_running()}")
        
        # Run a simple query
        result = db.query("SELECT 1 as test_value, 'Hello DuckDBX' as message")
        print(f"✓ Query result: {result}")
        
        # Run another query
        result = db.query("SELECT 2 + 2 as sum")
        print(f"✓ Query result: {result}")
    
    print("✓ Context manager cleanup completed")


def test_manual_lifecycle():
    """Test manual lifecycle management."""
    print("\nTesting manual lifecycle management...")
    
    db = DuckDBX()
    
    try:
        db.start()
        print(f"✓ Container started: {db.is_running()}")
        
        # Run a query
        result = db.query("SELECT 'Manual lifecycle test' as test")
        print(f"✓ Query result: {result}")
        
    finally:
        db.stop()
        print("✓ Manual cleanup completed")


if __name__ == "__main__":
    print("=" * 60)
    print("DuckDBX Basic Test")
    print("=" * 60)
    
    try:
        test_context_manager()
        test_manual_lifecycle()
        
        print("\n" + "=" * 60)
        print("✓ All tests passed!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

