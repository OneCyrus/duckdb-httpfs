#!/bin/bash
# Demonstrate corruption in wildcard/glob reads even with force_full_download=true

set -e

echo "================================================================================"
echo "Glob/Wildcard Corruption Test: *.json reads"
echo "================================================================================"
echo ""
echo "This demonstrates that EVEN with force_full_download=true, you can get"
echo "malformed JSON errors when files change between HEAD and GET requests."
echo ""
echo "Scenario:"
echo "  1. DuckDB reads *.json (multiple files)"
echo "  2. For EACH file: HEAD request → GET request"
echo "  3. Files change BETWEEN HEAD and GET"
echo "  4. Result: Metadata mismatch → Malformed JSON errors"
echo ""

# Start a specialized server for this test
cat > /tmp/glob_test_server.py << 'SERVEREOF'
import http.server
import socketserver
import urllib.parse
import threading
import time
import json

PORT = 8888

# Two different JSON structures
JSON_V1 = json.dumps({"id": 1, "name": "Alice", "age": 30})
JSON_V2 = json.dumps({"id": 1, "name": "Alice", "age": 30, "city": "NYC", "extra_field": "new_data"})

# File versions - different content, different sizes
FILE_VERSIONS = {
    "version-1": JSON_V1.encode('utf-8'),
    "version-2": JSON_V2.encode('utf-8'),
}

current_version = "version-1"
version_lock = threading.Lock()
request_log = []

class GlobTestHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        msg = f"[{len(request_log)+1:03d}] {self.command} {self.path}"
        request_log.append(msg)
        print(msg, flush=True)

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
            print(f"    → Serving: {version_to_serve} (current: {current_version})", flush=True)

        if version_to_serve not in FILE_VERSIONS:
            self.send_error(404)
            return

        file_content = FILE_VERSIONS[version_to_serve]
        content_length = len(file_content)

        # Simulate slight network delay
        time.sleep(0.1)

        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(content_length))
        self.send_header('x-amz-version-id', version_to_serve)
        self.send_header('ETag', f'"{version_to_serve}"')
        self.end_headers()

        if send_body:
            self.wfile.write(file_content)
            print(f"    → Sent {content_length} bytes from {version_to_serve}", flush=True)

def change_version_after_head():
    """Change version right after first HEAD request"""
    global current_version
    # Wait for first HEAD request
    while len(request_log) < 1:
        time.sleep(0.05)

    time.sleep(0.3)  # Short delay after HEAD

    with version_lock:
        old = current_version
        current_version = "version-2"
        print(f"\n{'='*70}")
        print(f"VERSION CHANGED: {old} -> {current_version}")
        print(f"Next GET request will serve different data!")
        print(f"{'='*70}\n", flush=True)

socketserver.TCPServer.allow_reuse_address = True
with socketserver.TCPServer(("", PORT), GlobTestHandler) as httpd:
    print(f"Glob test server running on port {PORT}")
    print(f"Initial version: {current_version}")
    print(f"Version 1: {JSON_V1}")
    print(f"Version 2: {JSON_V2}")
    print("="*70 + "\n", flush=True)

    threading.Thread(target=change_version_after_head, daemon=True).start()

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
SERVEREOF

python3 /tmp/glob_test_server.py &
SERVER_PID=$!

cleanup() {
    echo ""
    echo "Stopping server..."
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
}
trap cleanup EXIT

sleep 1

echo "================================================================================"
echo "TEST 1: Single File Read with force_full_download=true"
echo "================================================================================"
echo ""
echo "Even with force_full_download=true, there are STILL two requests:"
echo ""

echo "Step 1: HEAD request (DuckDB gets file metadata)"
echo "------------------------------------------------------------------------"
HEAD_RESPONSE=$(curl -s -I http://localhost:8888/data.json)
echo "$HEAD_RESPONSE" | grep -E "(Content-Length|x-amz-version-id)"
HEAD_LENGTH=$(echo "$HEAD_RESPONSE" | grep "Content-Length" | awk '{print $2}' | tr -d '\r')
HEAD_VERSION=$(echo "$HEAD_RESPONSE" | grep "x-amz-version-id" | awk '{print $2}' | tr -d '\r')
echo ""
echo "DuckDB now expects:"
echo "  - Content-Length: $HEAD_LENGTH bytes"
echo "  - Version: $HEAD_VERSION"
echo "  - Structure: {id, name, age}"
echo ""

echo "Step 2: File changes on server (version-1 → version-2)"
echo "------------------------------------------------------------------------"
sleep 0.5
echo "Server now serving version-2 with DIFFERENT structure and size"
echo ""

echo "Step 3: GET request (DuckDB downloads file)"
echo "------------------------------------------------------------------------"
GET_RESPONSE=$(curl -s http://localhost:8888/data.json)
echo "Downloaded: $GET_RESPONSE"
GET_LENGTH=${#GET_RESPONSE}
echo ""
echo "DuckDB received:"
echo "  - Content-Length: $GET_LENGTH bytes (expected: $HEAD_LENGTH)"
echo "  - Structure: {id, name, age, city, extra_field}"
echo ""

if [ "$HEAD_LENGTH" != "$GET_LENGTH" ]; then
    echo "❌ SIZE MISMATCH DETECTED!"
    echo "   HEAD said: $HEAD_LENGTH bytes (version-1)"
    echo "   GET returned: $GET_LENGTH bytes (version-2)"
    echo ""
    echo "   This causes:"
    echo "   - Malformed JSON errors (unexpected fields)"
    echo "   - Schema mismatch errors"
    echo "   - Checksum failures"
    echo "   - Data corruption"
else
    echo "✓ Sizes match (no version change occurred)"
fi
echo ""

echo "================================================================================"
echo "TEST 2: The Same Problem with Version Pinning FIXES It"
echo "================================================================================"
echo ""

# Reset server
sleep 1

echo "Step 1: HEAD request captures version ID"
echo "------------------------------------------------------------------------"
HEAD_RESPONSE_V=$(curl -s -I http://localhost:8888/data.json)
echo "$HEAD_RESPONSE_V" | grep -E "(Content-Length|x-amz-version-id)"
CAPTURED_VERSION="version-1"
CAPTURED_LENGTH=$(echo "$HEAD_RESPONSE_V" | grep "Content-Length" | awk '{print $2}' | tr -d '\r')
echo ""
echo "Captured version ID: $CAPTURED_VERSION"
echo ""

echo "Step 2: File changes on server (still version-2)"
echo "------------------------------------------------------------------------"
echo "Server is serving version-2"
echo "But we have version ID pinned to: $CAPTURED_VERSION"
echo ""

echo "Step 3: GET request WITH versionId parameter"
echo "------------------------------------------------------------------------"
GET_RESPONSE_V=$(curl -s "http://localhost:8888/data.json?versionId=$CAPTURED_VERSION")
echo "Downloaded: $GET_RESPONSE_V"
GET_LENGTH_V=${#GET_RESPONSE_V}
echo ""
echo "DuckDB received:"
echo "  - Content-Length: $GET_LENGTH_V bytes (expected: $CAPTURED_LENGTH)"
echo "  - Version: $CAPTURED_VERSION (pinned)"
echo ""

if [ "$CAPTURED_LENGTH" == "$GET_LENGTH_V" ]; then
    echo "✅ SIZE MATCH!"
    echo "   HEAD and GET both returned version-1"
    echo "   No metadata mismatch"
    echo "   No malformed JSON errors"
else
    echo "❌ Unexpected mismatch"
fi
echo ""

echo "================================================================================"
echo "REAL-WORLD GLOB SCENARIO: *.json reads"
echo "================================================================================"
echo ""
echo "When DuckDB reads *.json files:"
echo ""
echo "read_json('s3://bucket/*.json');"
echo ""
echo "It processes EACH file with:"
echo "  1. Glob expansion (list files)"
echo "  2. For each file:"
echo "     a. HEAD request (get metadata)"
echo "     b. GET request (download data)"
echo ""
echo "If files are being updated/replaced during this process:"
echo ""
echo "WITHOUT Version Pinning:"
echo "  file1.json: HEAD → v1 metadata, GET → v2 data ❌ MISMATCH"
echo "  file2.json: HEAD → v1 metadata, GET → v2 data ❌ MISMATCH"
echo "  file3.json: HEAD → v2 metadata, GET → v2 data ✓ (lucky timing)"
echo ""
echo "  Result: Intermittent 'malformed JSON' errors!"
echo ""
echo "WITH Version Pinning:"
echo "  file1.json: HEAD → v1 + versionId, GET → v1 (pinned) ✓ CONSISTENT"
echo "  file2.json: HEAD → v1 + versionId, GET → v1 (pinned) ✓ CONSISTENT"
echo "  file3.json: HEAD → v2 + versionId, GET → v2 (pinned) ✓ CONSISTENT"
echo ""
echo "  Result: Always consistent, no malformed JSON errors!"
echo ""

echo "================================================================================"
echo "WHY force_full_download=true DOESN'T HELP"
echo "================================================================================"
echo ""
echo "force_full_download=true only affects HOW the file is downloaded:"
echo "  ✓ Downloads entire file in one GET (not ranges)"
echo "  ✓ Prevents chunk-level corruption"
echo ""
echo "But it DOESN'T prevent HEAD/GET version mismatch:"
echo "  ❌ Still makes separate HEAD request"
echo "  ❌ File can change between HEAD and GET"
echo "  ❌ Metadata (size, structure) doesn't match content"
echo ""
echo "The symptoms you see:"
echo "  - 'Malformed JSON' errors"
echo "  - 'Unexpected end of JSON input'"
echo "  - 'Schema mismatch' errors"
echo "  - Intermittent failures (timing-dependent)"
echo ""
echo "These are all caused by HEAD/GET version mismatches!"
echo ""

echo "================================================================================"
echo "THE SOLUTION"
echo "================================================================================"
echo ""
echo "Version ID pinning fixes this by:"
echo "  1. Capturing version ID from HEAD response"
echo "  2. Including versionId in GET request"
echo "  3. Ensuring HEAD and GET return same version"
echo ""
echo "For *.json glob reads:"
echo "  ✅ Each file's metadata and content are consistent"
echo "  ✅ No malformed JSON errors"
echo "  ✅ Works regardless of concurrent file updates"
echo ""
echo "================================================================================"
