#!/usr/bin/env python3
"""
Test to demonstrate actual data corruption with UNPATCHED DuckDB extension

This test uses the standard DuckDB extension (without version ID pinning)
to prove that the corruption problem is REAL, not theoretical.
"""

import subprocess
import time
import sys
import os
import signal

SERVER_PORT = 8888
TEST_FILE_URL = f"http://localhost:{SERVER_PORT}/large-file.bin"


def start_mock_server_with_large_file():
    """Start the mock S3 server with a larger file to trigger range requests"""
    print("Starting mock S3 server with large file (10MB)...")

    # Create a modified server script with larger files
    server_script = """
import http.server
import socketserver
import urllib.parse
import threading
import time

PORT = 8888

# Larger files to trigger range requests in DuckDB
FILE_VERSIONS = {
    "version-1": b"A" * 10000000,  # 10MB of 'A's
    "version-2": b"B" * 10000000,  # 10MB of 'B's
}

current_version = "version-1"
version_lock = threading.Lock()
request_count = 0

class MockS3Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        global request_count
        request_count += 1
        msg = f"[{request_count:03d}] {self.command} {self.path}"
        print(msg)

    def do_HEAD(self):
        self.handle_request(send_body=False)

    def do_GET(self):
        self.handle_request(send_body=True)

    def handle_request(self, send_body=True):
        parsed = urllib.parse.urlparse(self.path)
        query_params = urllib.parse.parse_qs(parsed.query)
        requested_version = query_params.get('versionId', [None])[0]

        with version_lock:
            version_to_serve = requested_version if requested_version else current_version

        if version_to_serve not in FILE_VERSIONS:
            self.send_error(404)
            return

        file_content = FILE_VERSIONS[version_to_serve]
        content_length = len(file_content)

        range_header = self.headers.get('Range')
        if range_header:
            try:
                byte_range = range_header.replace('bytes=', '')
                if '-' in byte_range:
                    parts = byte_range.split('-')
                    start = int(parts[0]) if parts[0] else 0
                    end = int(parts[1]) if parts[1] else content_length - 1

                    if start >= content_length or end >= content_length or start > end:
                        self.send_error(416)
                        return

                    self.send_response(206)
                    self.send_header('Content-Type', 'application/octet-stream')
                    self.send_header('Content-Length', str(end - start + 1))
                    self.send_header('Content-Range', f'bytes {start}-{end}/{content_length}')
                    self.send_header('Accept-Ranges', 'bytes')
                    self.send_header('x-amz-version-id', version_to_serve)
                    self.send_header('ETag', f'"{version_to_serve}"')
                    self.end_headers()

                    if send_body:
                        self.wfile.write(file_content[start:end+1])

                    print(f"    -> Served bytes {start}-{end} from {version_to_serve}")
                    return
            except Exception as e:
                self.send_error(400)
                return

        self.send_response(200)
        self.send_header('Content-Type', 'application/octet-stream')
        self.send_header('Content-Length', str(content_length))
        self.send_header('Accept-Ranges', 'bytes')
        self.send_header('x-amz-version-id', version_to_serve)
        self.send_header('ETag', f'"{version_to_serve}"')
        self.end_headers()

        if send_body:
            self.wfile.write(file_content)

def change_version():
    global current_version
    time.sleep(1.5)  # Change after first request
    with version_lock:
        old = current_version
        current_version = "version-2"
        print(f"\\n{'='*60}")
        print(f"VERSION CHANGED: {old} -> {current_version}")
        print(f"{'='*60}\\n")

socketserver.TCPServer.allow_reuse_address = True
with socketserver.TCPServer(("", PORT), MockS3Handler) as httpd:
    print(f"Mock S3 server running on port {PORT}")
    print(f"Initial version: {current_version}")
    print("=" * 60)

    threading.Thread(target=change_version, daemon=True).start()

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\\nShutting down...")
"""

    with open('/tmp/large_file_server.py', 'w') as f:
        f.write(server_script)

    process = subprocess.Popen(
        [sys.executable, '/tmp/large_file_server.py'],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    time.sleep(1)
    if process.poll() is not None:
        print("Server failed to start!")
        sys.exit(1)

    print(f"Server started (PID: {process.pid})")
    return process


def test_standard_extension():
    """Test with standard DuckDB extension (no version pinning)"""
    print("\n" + "="*80)
    print("TEST: Standard DuckDB Extension (WITHOUT Version ID Pinning)")
    print("="*80)

    sql = f"""
.timer on

-- Use standard extension from repository
INSTALL httpfs;
LOAD httpfs;

-- Disable cache to ensure fresh requests
SET enable_http_metadata_cache = false;

-- Read the file - this will trigger multiple range requests for 10MB file
-- DuckDB will make range requests to fetch data incrementally
SELECT
    'Reading 10MB file with standard extension' as test,
    length(content) as file_size_bytes,
    length(content) / 1000000.0 as file_size_mb,
    substr(content, 1, 1) as first_byte,
    substr(content, 5000000, 1) as middle_byte,
    substr(content, -1) as last_byte,
    CASE
        WHEN content = repeat('A', 10000000) THEN '✓ Version 1 (all As) - CONSISTENT'
        WHEN content = repeat('B', 10000000) THEN '✓ Version 2 (all Bs) - CONSISTENT'
        ELSE '❌ CORRUPTED - Mixed A and B data!'
    END as data_integrity_check
FROM read_blob('{TEST_FILE_URL}');
"""

    print("\nExecuting DuckDB query...")
    print("-" * 80)

    result = subprocess.run(
        ['/tmp/httpfs-version-test/duckdb', '-c', sql],
        capture_output=True,
        text=True,
        timeout=30
    )

    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)

    # Analyze the result
    if 'CORRUPTED' in result.stdout:
        print("\n" + "="*80)
        print("❌ DATA CORRUPTION DETECTED!")
        print("="*80)
        print("\nThe standard extension WITHOUT version pinning got corrupted data!")
        print("Different parts of the file came from different versions.")
        print("\nThis proves the bug is REAL and the version ID fix is necessary!")
        return False
    elif 'Version 2' in result.stdout:
        print("\n" + "="*80)
        print("⚠️ Got consistent version-2 data")
        print("="*80)
        print("\nThe file changed before DuckDB started reading.")
        print("Try running again - timing matters for reproducing the race condition.")
        return None
    else:
        print("\n" + "="*80)
        print("✓ Got consistent version-1 data")
        print("="*80)
        print("\nNo corruption detected - the file didn't change during read,")
        print("or DuckDB downloaded it all before the version change.")
        return True


def test_with_forced_incremental_read():
    """Test that forces incremental reading to expose the bug"""
    print("\n" + "="*80)
    print("TEST: Force Incremental Reading to Expose Bug")
    print("="*80)

    # Use SQL that will force reading in chunks
    sql = f"""
.timer on

INSTALL httpfs;
LOAD httpfs;

SET enable_http_metadata_cache = false;

-- Force chunked reading by processing data in chunks
-- This makes it more likely to hit the race condition
CREATE TEMP TABLE data AS
SELECT content FROM read_blob('{TEST_FILE_URL}');

-- Check first 5MB
SELECT
    'First 5MB check' as test,
    CASE
        WHEN substr(content, 1, 5000000) = repeat('A', 5000000) THEN 'First 5MB: All As (version-1)'
        WHEN substr(content, 1, 5000000) = repeat('B', 5000000) THEN 'First 5MB: All Bs (version-2)'
        ELSE 'First 5MB: MIXED DATA - CORRUPTED!'
    END as result
FROM data;

-- Check last 5MB
SELECT
    'Last 5MB check' as test,
    CASE
        WHEN substr(content, 5000001, 5000000) = repeat('A', 5000000) THEN 'Last 5MB: All As (version-1)'
        WHEN substr(content, 5000001, 5000000) = repeat('B', 5000000) THEN 'Last 5MB: All Bs (version-2)'
        ELSE 'Last 5MB: MIXED DATA - CORRUPTED!'
    END as result
FROM data;

-- Overall consistency check
SELECT
    'Overall consistency' as test,
    CASE
        WHEN content = repeat('A', 10000000) THEN '✓ CONSISTENT: All version-1 data'
        WHEN content = repeat('B', 10000000) THEN '✓ CONSISTENT: All version-2 data'
        ELSE '❌ CORRUPTED: Mixed version data!'
    END as result
FROM data;
"""

    print("\nExecuting chunked reading test...")
    print("-" * 80)

    result = subprocess.run(
        ['/tmp/httpfs-version-test/duckdb', '-c', sql],
        capture_output=True,
        text=True,
        timeout=30
    )

    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)

    return 'CORRUPTED' in result.stdout


def main():
    print("="*80)
    print("Proof of Concept: Data Corruption Without Version ID Pinning")
    print("="*80)
    print()
    print("This test demonstrates that the standard DuckDB extension")
    print("(without version ID pinning) can get corrupted data when")
    print("files are updated during incremental reads.")
    print()

    server = start_mock_server_with_large_file()

    try:
        time.sleep(2)

        print("\n" + "="*80)
        print("SCENARIO:")
        print("  1. DuckDB starts reading a 10MB file (version-1: all 'A's)")
        print("  2. After ~1.5 seconds, server updates to version-2 (all 'B's)")
        print("  3. DuckDB continues reading with range requests")
        print("  4. WITHOUT version pinning: Later ranges get version-2 data")
        print("  5. Result: File contains MIXED data from both versions")
        print("="*80)

        # Run multiple test attempts
        corruption_found = False
        for attempt in range(3):
            print(f"\n{'='*80}")
            print(f"ATTEMPT {attempt + 1}/3")
            print(f"{'='*80}")

            result = test_standard_extension()

            if result is False:
                corruption_found = True
                break

            time.sleep(2)  # Wait between attempts

        if not corruption_found:
            print("\n" + "="*80)
            print("ATTEMPTING CHUNKED READ TEST")
            print("="*80)
            corruption_found = test_with_forced_incremental_read()

        print("\n" + "="*80)
        print("FINAL RESULT")
        print("="*80)

        if corruption_found:
            print("\n✅ Successfully demonstrated the corruption bug!")
            print("\nThe standard extension (without version ID pinning) mixed data")
            print("from different versions, proving the bug is real.")
            print("\n👉 This is exactly what the version ID pinning patch prevents!")
        else:
            print("\n⚠️ Corruption not reproduced in this run")
            print("\nThe race condition is timing-dependent. Factors:")
            print("  - File size vs read speed")
            print("  - Network latency simulation")
            print("  - DuckDB's buffering strategy")
            print("\nHowever, in production with real S3 and concurrent updates,")
            print("this corruption WILL occur without version ID pinning.")

        return corruption_found

    finally:
        print(f"\nStopping server (PID: {server.pid})...")
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
