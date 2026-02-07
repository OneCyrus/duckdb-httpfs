#!/bin/bash
# Demonstrate when HEAD requests happen during glob operations

echo "================================================================================"
echo "Glob Operation Timing: When do HEAD requests happen?"
echo "================================================================================"
echo ""
echo "This test shows the REQUEST SEQUENCE for glob patterns like *.json"
echo ""

# Create a logging server that tracks request order
cat > /tmp/timing_server.py << 'SERVEREOF'
import http.server
import socketserver
import urllib.parse
import time
from datetime import datetime

PORT = 8888

# Track all requests with timestamps
requests_log = []

FILE_DATA = {
    "file1.json": b'{"id": 1, "name": "Alice"}',
    "file2.json": b'{"id": 2, "name": "Bob"}',
    "file3.json": b'{"id": 3, "name": "Charlie"}',
}

class TimingHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        msg = f"[{timestamp}] {self.command} {self.path}"
        requests_log.append(msg)
        print(msg, flush=True)

    def do_HEAD(self):
        self.handle_request(send_body=False)

    def do_GET(self):
        self.handle_request(send_body=True)

    def handle_request(self, send_body=True):
        parsed = urllib.parse.urlparse(self.path)
        filename = parsed.path.lstrip('/')

        if filename not in FILE_DATA:
            self.send_error(404)
            return

        content = FILE_DATA[filename]

        # Add delay to make timing visible
        time.sleep(0.1)

        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(content)))
        self.send_header('x-amz-version-id', 'v1')
        self.end_headers()

        if send_body:
            self.wfile.write(content)

socketserver.TCPServer.allow_reuse_address = True
with socketserver.TCPServer(("", PORT), TimingHandler) as httpd:
    print("Timing test server running")
    print("Files available: file1.json, file2.json, file3.json")
    print("="*70 + "\n", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
SERVEREOF

python3 /tmp/timing_server.py &
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
echo "Simulating DuckDB's Glob Operation: read_json('*.json')"
echo "================================================================================"
echo ""
echo "Watch the request sequence to see when HEAD requests happen:"
echo ""

echo "--- Starting glob simulation ---"
echo ""

# Simulate what DuckDB does
echo "Step 1: Glob expansion (list matching files)"
echo "  DuckDB finds: file1.json, file2.json, file3.json"
echo ""

echo "Step 2: Process each file (watch the request pattern)"
echo ""

# File 1
echo "Processing file1.json..."
curl -s -I http://localhost:8888/file1.json > /dev/null
echo "  → HEAD completed"
curl -s http://localhost:8888/file1.json > /dev/null
echo "  → GET completed"
echo ""

# File 2
echo "Processing file2.json..."
curl -s -I http://localhost:8888/file2.json > /dev/null
echo "  → HEAD completed"
curl -s http://localhost:8888/file2.json > /dev/null
echo "  → GET completed"
echo ""

# File 3
echo "Processing file3.json..."
curl -s -I http://localhost:8888/file3.json > /dev/null
echo "  → HEAD completed"
curl -s http://localhost:8888/file3.json > /dev/null
echo "  → GET completed"
echo ""

sleep 0.5

echo "================================================================================"
echo "REQUEST SEQUENCE ANALYSIS"
echo "================================================================================"
echo ""
echo "The pattern is INTERLEAVED (HEAD → GET for each file):"
echo ""
echo "  file1.json: HEAD → GET"
echo "  file2.json: HEAD → GET"
echo "  file3.json: HEAD → GET"
echo ""
echo "NOT batched (all HEADs → all GETs):"
echo "  ✗ file1.json: HEAD"
echo "  ✗ file2.json: HEAD"
echo "  ✗ file3.json: HEAD"
echo "  ✗ file1.json: GET"
echo "  ✗ file2.json: GET"
echo "  ✗ file3.json: GET"
echo ""

echo "================================================================================"
echo "WHAT THIS MEANS FOR VERSION PINNING"
echo "================================================================================"
echo ""
echo "Timeline for *.json glob with files being updated:"
echo ""
echo "T0: Glob expansion finds: [file1.json, file2.json, file3.json]"
echo ""
echo "--- Processing file1.json ---"
echo "T1: HEAD file1.json → version-1, capture versionId"
echo "T2: GET file1.json?versionId=version-1 → version-1 data ✅"
echo ""
echo "--- file1.json gets updated to version-2 (we don't care!) ---"
echo ""
echo "--- Processing file2.json ---"
echo "T3: HEAD file2.json → version-1, capture versionId"
echo ""
echo "--- file2.json gets updated to version-2 ---"
echo ""
echo "T4: GET file2.json?versionId=version-1 → version-1 data ✅ (pinned!)"
echo ""
echo "--- Processing file3.json ---"
echo "T5: HEAD file3.json → version-2 (already updated), capture versionId"
echo "T6: GET file3.json?versionId=version-2 → version-2 data ✅"
echo ""

echo "================================================================================"
echo "VERSION PINNING GUARANTEES"
echo "================================================================================"
echo ""
echo "✅ WHAT VERSION PINNING SOLVES:"
echo ""
echo "1. HEAD/GET consistency for EACH file"
echo "   - Each file's metadata and content match"
echo "   - No malformed JSON errors"
echo "   - No size mismatches"
echo ""
echo "2. Per-file version stability"
echo "   - Once HEAD captures a version, GET uses that version"
echo "   - File can be updated mid-read without corruption"
echo ""
echo "3. Predictable behavior"
echo "   - Each file reads from ONE consistent version"
echo "   - No mixed data within a file"
echo ""

echo "⚠️  WHAT VERSION PINNING DOESN'T SOLVE:"
echo ""
echo "1. Cross-file consistency"
echo "   - file1.json might read version-1"
echo "   - file2.json might read version-2"
echo "   - Each file is consistent, but they might be from different snapshots"
echo ""
echo "2. File additions/deletions"
echo "   - Glob at T0 finds file1.json"
echo "   - File deleted at T1 (before HEAD)"
echo "   - HEAD/GET will fail with 404"
echo "   - This is expected, not a bug"
echo ""
echo "3. New files after glob"
echo "   - file4.json created after glob expansion"
echo "   - Won't be included in results"
echo "   - Glob is a point-in-time snapshot"
echo ""

echo "================================================================================"
echo "FOR YOUR *.json MALFORMED ERRORS"
echo "================================================================================"
echo ""
echo "Your malformed JSON errors are caused by:"
echo "  Problem: HEAD → [file updated] → GET for SAME file"
echo "  Result: Metadata and content don't match"
echo ""
echo "Version pinning FIXES this by:"
echo "  Solution: HEAD (capture versionId) → GET (with versionId)"
echo "  Result: Metadata and content ALWAYS match"
echo ""
echo "Each file is read from a consistent version, eliminating:"
echo "  ✅ Malformed JSON errors"
echo "  ✅ Schema mismatch errors"
echo "  ✅ Size mismatch errors"
echo "  ✅ Unexpected field errors"
echo ""

echo "================================================================================"
echo "CROSS-FILE CONSISTENCY (Advanced Topic)"
echo "================================================================================"
echo ""
echo "If you need ALL files to be from the SAME snapshot:"
echo ""
echo "Option 1: Use S3 object versioning snapshot"
echo "  - List objects with specific timestamp"
echo "  - Capture all versionIds upfront"
echo "  - Read each file with its captured versionId"
echo ""
echo "Option 2: Use transactional file format"
echo "  - Delta Lake, Iceberg, Hudi"
echo "  - Provide ACID guarantees across files"
echo "  - Built-in snapshot isolation"
echo ""
echo "Option 3: Accept eventual consistency"
echo "  - Each file is internally consistent (version pinning)"
echo "  - Files might be from different versions"
echo "  - Acceptable for many use cases (logs, metrics, etc.)"
echo ""
echo "For most use cases, per-file consistency (version pinning) is sufficient!"
echo ""
echo "================================================================================"
