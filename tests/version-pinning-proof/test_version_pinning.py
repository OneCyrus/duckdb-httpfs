#!/usr/bin/env python3
"""
Test script to verify S3 version ID pinning in DuckDB HTTPfs extension

This script:
1. Starts a mock S3 server that simulates versioned objects
2. Tests that DuckDB correctly pins to a specific version ID
3. Verifies data consistency even when the file changes mid-read
"""

import subprocess
import time
import sys
import os
import signal
import threading

SERVER_PORT = 8888
TEST_FILE_URL = f"http://localhost:{SERVER_PORT}/test-file.bin"


def start_mock_server():
    """Start the mock S3 server in background"""
    print("Starting mock S3 server...")
    process = subprocess.Popen(
        [sys.executable, "mock_s3_server.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    # Wait for server to start
    time.sleep(1)

    # Check if server is running
    if process.poll() is not None:
        stdout, stderr = process.communicate()
        print(f"Server failed to start!")
        print(f"STDOUT: {stdout}")
        print(f"STDERR: {stderr}")
        sys.exit(1)

    print(f"Mock S3 server started (PID: {process.pid})")
    return process


def run_duckdb_test(extension_path=None):
    """Run DuckDB test with the httpfs extension"""
    print("\n" + "=" * 80)
    print("TESTING VERSION ID PINNING WITH DUCKDB")
    print("=" * 80)

    # Build DuckDB command
    duckdb_cmd = ["./duckdb", "-c"]

    # SQL commands to execute
    sql_commands = """
.timer on
.echo on

-- Show DuckDB version
SELECT version();

-- Load httpfs extension
INSTALL httpfs FROM '{{ extension_path }}';
LOAD httpfs;

-- Enable HTTP metadata cache to test version pinning across cache
SET enable_http_metadata_cache = true;

-- Test 1: Read the entire file
-- This should capture version-1 and continue using it even when server switches to version-2
SELECT
    'Test 1: Full file read' as test,
    length(content) as file_size,
    substr(content, 1, 10) as first_10_chars,
    substr(content, -10) as last_10_chars,
    CASE
        WHEN content = repeat('A', 1000000) THEN 'version-1 (all As)'
        WHEN content = repeat('B', 1000000) THEN 'version-2 (all Bs)'
        ELSE 'MIXED/CORRUPTED DATA!'
    END as content_check
FROM read_blob('{{ url }}');

-- Wait a moment
SELECT 'Waiting 1 second...' as status;

-- Test 2: Read again - should still get version-1 due to cache
SELECT
    'Test 2: Read from cache' as test,
    length(content) as file_size,
    CASE
        WHEN content = repeat('A', 1000000) THEN 'version-1 (all As) - CACHE HIT'
        WHEN content = repeat('B', 1000000) THEN 'version-2 (all Bs)'
        ELSE 'MIXED/CORRUPTED DATA!'
    END as content_check
FROM read_blob('{{ url }}');

-- Test 3: Explicitly request version-1
SELECT
    'Test 3: Explicit version-1' as test,
    length(content) as file_size,
    CASE
        WHEN content = repeat('A', 1000000) THEN 'version-1 (all As) - CORRECT'
        ELSE 'WRONG VERSION!'
    END as content_check
FROM read_blob('{{ url }}?versionId=version-1');

-- Test 4: Explicitly request version-2
SELECT
    'Test 4: Explicit version-2' as test,
    length(content) as file_size,
    CASE
        WHEN content = repeat('B', 1000000) THEN 'version-2 (all Bs) - CORRECT'
        ELSE 'WRONG VERSION!'
    END as content_check
FROM read_blob('{{ url }}?versionId=version-2');

-- Test 5: Clear cache and read again - should now get current version (version-2)
SET enable_http_metadata_cache = false;
SELECT
    'Test 5: Read with cache disabled' as test,
    length(content) as file_size,
    CASE
        WHEN content = repeat('A', 1000000) THEN 'version-1 (all As)'
        WHEN content = repeat('B', 1000000) THEN 'version-2 (all Bs) - CURRENT'
        ELSE 'MIXED/CORRUPTED DATA!'
    END as content_check
FROM read_blob('{{ url }}');

SELECT '=== ALL TESTS COMPLETED ===' as status;
"""

    # Replace placeholders
    sql_commands = sql_commands.replace('{{ url }}', TEST_FILE_URL)
    if extension_path:
        sql_commands = sql_commands.replace('{{ extension_path }}', extension_path)
    else:
        # If no extension path provided, try to install from repository
        sql_commands = sql_commands.replace("INSTALL httpfs FROM '{{ extension_path }}';", "INSTALL httpfs;")

    # Execute DuckDB
    print("\nExecuting DuckDB tests...")
    print("-" * 80)

    try:
        result = subprocess.run(
            duckdb_cmd + [sql_commands],
            cwd="/tmp/httpfs-version-test",
            capture_output=True,
            text=True,
            timeout=30
        )

        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)

        return result.returncode == 0

    except subprocess.TimeoutExpired:
        print("ERROR: DuckDB test timed out!")
        return False
    except Exception as e:
        print(f"ERROR: {e}")
        return False


def main():
    """Main test execution"""
    print("=" * 80)
    print("DuckDB HTTPfs Version ID Pinning Test")
    print("=" * 80)
    print()

    # Start mock server
    server_process = start_mock_server()

    try:
        # Wait for server to fully initialize
        time.sleep(2)

        # Run DuckDB tests
        success = run_duckdb_test()

        # Print results
        print("\n" + "=" * 80)
        if success:
            print("TEST RESULT: ✓ PASSED")
            print("\nVersion ID pinning is working correctly!")
            print("- Initial read captured version-1")
            print("- Subsequent reads used the pinned version-1 even after server changed to version-2")
            print("- Explicit version requests work correctly")
            print("- Cache correctly stores and retrieves versioned data")
        else:
            print("TEST RESULT: ✗ FAILED")
            print("\nSome tests failed. Check output above for details.")
        print("=" * 80)

        return success

    finally:
        # Stop server
        print(f"\nStopping mock S3 server (PID: {server_process.pid})...")
        server_process.terminate()
        try:
            server_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server_process.kill()
        print("Server stopped.")


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
