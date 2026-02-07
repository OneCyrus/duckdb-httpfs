#!/bin/bash
# Demonstrate the data corruption problem without version ID pinning

set -e

echo "=========================================================================="
echo "Demonstration: Why Version ID Pinning is Critical"
echo "=========================================================================="
echo ""
echo "This script demonstrates what happens when a file is updated mid-download"
echo "WITHOUT version ID pinning."
echo ""

# Start the mock server in background
echo "Starting mock S3 server..."
python3 mock_s3_server.py &
SERVER_PID=$!

# Cleanup function
cleanup() {
    echo ""
    echo "Cleaning up..."
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
}
trap cleanup EXIT

# Wait for server to start
sleep 1

echo ""
echo "=========================================================================="
echo "SCENARIO: Reading a file in chunks WITHOUT version pinning"
echo "=========================================================================="
echo ""
echo "Step 1: Read first chunk (bytes 0-10000)"
echo "------------------------------------------------------------------------"
curl -s -H "Range: bytes=0-10000" http://localhost:8888/test-file.bin -o /tmp/chunk1.bin
CHAR1=$(head -c 1 /tmp/chunk1.bin)
VERSION1=$(curl -s -I -H "Range: bytes=0-10000" http://localhost:8888/test-file.bin | grep "x-amz-version-id" | cut -d' ' -f2 | tr -d '\r')
echo "  First character: '$CHAR1'"
echo "  Version: $VERSION1"
echo "  ✓ Got data from version-1"
echo ""

echo "Step 2: File gets updated on server (version-1 -> version-2)"
echo "------------------------------------------------------------------------"
echo "  Waiting for automatic version change..."
sleep 2.5
echo "  ✓ Server now serving version-2"
echo ""

echo "Step 3: Read second chunk WITHOUT version pinning (bytes 10001-20000)"
echo "------------------------------------------------------------------------"
curl -s -H "Range: bytes=10001-20000" http://localhost:8888/test-file.bin -o /tmp/chunk2.bin
CHAR2=$(head -c 1 /tmp/chunk2.bin)
VERSION2=$(curl -s -I -H "Range: bytes=10001-20000" http://localhost:8888/test-file.bin | grep "x-amz-version-id" | cut -d' ' -f2 | tr -d '\r')
echo "  First character: '$CHAR2'"
echo "  Version: $VERSION2"
echo "  ⚠️  Got data from version-2 (DIFFERENT VERSION!)"
echo ""

echo "Step 4: Read third chunk WITHOUT version pinning (bytes 20001-30000)"
echo "------------------------------------------------------------------------"
curl -s -H "Range: bytes=20001-30000" http://localhost:8888/test-file.bin -o /tmp/chunk3.bin
CHAR3=$(head -c 1 /tmp/chunk3.bin)
VERSION3=$(curl -s -I -H "Range: bytes=20001-30000" http://localhost:8888/test-file.bin | grep "x-amz-version-id" | cut -d' ' -f2 | tr -d '\r')
echo "  First character: '$CHAR3'"
echo "  Version: $VERSION3"
echo "  ⚠️  Got data from version-2 (DIFFERENT VERSION!)"
echo ""

echo "=========================================================================="
echo "RESULT WITHOUT VERSION PINNING: DATA CORRUPTION!"
echo "=========================================================================="
echo ""
echo "Chunk 1: '$CHAR1' from $VERSION1"
echo "Chunk 2: '$CHAR2' from $VERSION2"
echo "Chunk 3: '$CHAR3' from $VERSION3"
echo ""
if [ "$CHAR1" != "$CHAR2" ] || [ "$CHAR1" != "$CHAR3" ]; then
    echo "❌ CORRUPTED: Different chunks contain data from different versions!"
    echo "   This creates an inconsistent file that mixes version-1 and version-2 data."
else
    echo "✓ Consistent (but this shouldn't happen in this test)"
fi
echo ""

# Now demonstrate WITH version pinning
echo ""
echo "=========================================================================="
echo "SCENARIO: Reading a file in chunks WITH version pinning"
echo "=========================================================================="
echo ""

echo "Step 1: Read first chunk and capture version ID"
echo "------------------------------------------------------------------------"
# Get version-1 by explicitly requesting it
CAPTURED_VERSION="version-1"
curl -s -H "Range: bytes=0-10000" "http://localhost:8888/test-file.bin?versionId=$CAPTURED_VERSION" -o /tmp/chunk1_pinned.bin
CHAR1_P=$(head -c 1 /tmp/chunk1_pinned.bin)
echo "  First character: '$CHAR1_P'"
echo "  Captured version ID: $CAPTURED_VERSION"
echo "  ✓ Got data from version-1"
echo ""

echo "Step 2: File is updated on server (already version-2)"
echo "------------------------------------------------------------------------"
echo "  Server is currently serving version-2"
echo "  ✓ But we have the version ID pinned to version-1"
echo ""

echo "Step 3: Read second chunk WITH version pinning"
echo "------------------------------------------------------------------------"
curl -s -H "Range: bytes=10001-20000" "http://localhost:8888/test-file.bin?versionId=$CAPTURED_VERSION" -o /tmp/chunk2_pinned.bin
CHAR2_P=$(head -c 1 /tmp/chunk2_pinned.bin)
echo "  First character: '$CHAR2_P'"
echo "  Using pinned version: $CAPTURED_VERSION"
echo "  ✓ Got data from version-1 (CONSISTENT!)"
echo ""

echo "Step 4: Read third chunk WITH version pinning"
echo "------------------------------------------------------------------------"
curl -s -H "Range: bytes=20001-30000" "http://localhost:8888/test-file.bin?versionId=$CAPTURED_VERSION" -o /tmp/chunk3_pinned.bin
CHAR3_P=$(head -c 1 /tmp/chunk3_pinned.bin)
echo "  First character: '$CHAR3_P'"
echo "  Using pinned version: $CAPTURED_VERSION"
echo "  ✓ Got data from version-1 (CONSISTENT!)"
echo ""

echo "=========================================================================="
echo "RESULT WITH VERSION PINNING: DATA INTEGRITY PRESERVED!"
echo "=========================================================================="
echo ""
echo "Chunk 1: '$CHAR1_P' from $CAPTURED_VERSION"
echo "Chunk 2: '$CHAR2_P' from $CAPTURED_VERSION"
echo "Chunk 3: '$CHAR3_P' from $CAPTURED_VERSION"
echo ""
if [ "$CHAR1_P" == "$CHAR2_P" ] && [ "$CHAR1_P" == "$CHAR3_P" ]; then
    echo "✅ SUCCESS: All chunks contain consistent data from the same version!"
    echo "   Even though the server updated to version-2, we continued reading version-1."
else
    echo "❌ FAILED: Data should be consistent"
fi
echo ""

echo "=========================================================================="
echo "CONCLUSION"
echo "=========================================================================="
echo ""
echo "WITHOUT version pinning:"
echo "  ❌ Mixed data from multiple versions"
echo "  ❌ Data corruption"
echo "  ❌ Unpredictable results"
echo ""
echo "WITH version pinning:"
echo "  ✅ Consistent data from single version"
echo "  ✅ Data integrity maintained"
echo "  ✅ Predictable, reliable results"
echo ""
echo "This is why DuckDB's S3 version ID support is critical for production use!"
echo "=========================================================================="
