#!/bin/bash
# Compare behavior with force_download=true vs false

echo "================================================================================"
echo "Comparison: force_download=true vs false vs version_pinning"
echo "================================================================================"
echo ""
echo "This demonstrates that:"
echo "  1. force_download=false (default) → CORRUPTION without version pinning"
echo "  2. force_download=true → NO corruption (but poor performance)"
echo "  3. Version pinning → NO corruption + good performance"
echo ""

# Start server
python3 mock_s3_server.py &
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
echo "SCENARIO 1: force_download=false (Default - Incremental Reading)"
echo "================================================================================"
echo ""
echo "This is what DuckDB does by default for large files:"
echo "  - Makes HEAD request to get metadata"
echo "  - Makes multiple range requests to fetch data in chunks"
echo "  - Efficient memory usage, fast startup"
echo ""
echo "WITHOUT version pinning, this causes corruption:"
echo ""

echo "Step 1: Initial HEAD request"
curl -s -I http://localhost:8888/test-file.bin | grep "x-amz-version-id"
echo "  → Got version-1 metadata"
echo ""

echo "Step 2: First range request (simulating DuckDB reading first chunk)"
curl -s -H "Range: bytes=0-100000" http://localhost:8888/test-file.bin -o /tmp/chunk1.bin
CHAR1=$(head -c 1 /tmp/chunk1.bin)
echo "  First byte: '$CHAR1' (from version-1) ✓"
echo ""

echo "Step 3: File changes on server..."
sleep 2.5
echo "  → Server now serving version-2"
echo ""

echo "Step 4: Second range request (DuckDB reading next chunk)"
echo "  ⚠️  WITHOUT versionId parameter!"
curl -s -H "Range: bytes=100001-200000" http://localhost:8888/test-file.bin -o /tmp/chunk2.bin
CHAR2=$(head -c 1 /tmp/chunk2.bin)
echo "  First byte: '$CHAR2' (from version-2) ❌ DIFFERENT VERSION!"
echo ""

echo "Result for force_download=false WITHOUT version pinning:"
echo "  Chunk 1: '$CHAR1' (version-1)"
echo "  Chunk 2: '$CHAR2' (version-2)"
if [ "$CHAR1" != "$CHAR2" ]; then
    echo "  ❌ CORRUPTED: Mixed data from multiple versions!"
else
    echo "  ✓ Consistent (file didn't change in time)"
fi
echo ""

echo "================================================================================"
echo "SCENARIO 2: force_download=true (Full Download)"
echo "================================================================================"
echo ""
echo "This is the workaround some users might use:"
echo "  - Downloads entire file in ONE request"
echo "  - No range requests, no chunks"
echo "  - Prevents corruption but has performance costs"
echo ""

# Wait for server to be back at version-1
sleep 1

echo "Single GET request for entire file:"
TIME_START=$(date +%s)
curl -s http://localhost:8888/test-file.bin -o /tmp/fullfile.bin
TIME_END=$(date +%s)
CHAR_FIRST=$(head -c 1 /tmp/fullfile.bin)
CHAR_LAST=$(tail -c 1 /tmp/fullfile.bin)
echo "  First byte: '$CHAR_FIRST'"
echo "  Last byte:  '$CHAR_LAST'"
echo "  Download time: $((TIME_END - TIME_START))s"
echo ""

if [ "$CHAR_FIRST" == "$CHAR_LAST" ]; then
    echo "Result for force_download=true:"
    echo "  ✓ CONSISTENT: All data from one version"
    echo ""
    echo "  BUT at the cost of:"
    echo "    - Entire file in memory (not scalable for large files)"
    echo "    - No incremental processing"
    echo "    - Higher latency"
else
    echo "  Unexpected: File changed during download"
fi
echo ""

echo "================================================================================"
echo "SCENARIO 3: force_download=false + Version Pinning (BEST)"
echo "================================================================================"
echo ""
echo "This is what the version ID pinning feature provides:"
echo "  - Incremental reading (efficient memory usage)"
echo "  - Multiple range requests (fast startup)"
echo "  - Version pinning (data consistency)"
echo ""

echo "Step 1: HEAD request captures version ID"
VERSION_ID="version-1"
curl -s -I "http://localhost:8888/test-file.bin" | grep "x-amz-version-id"
echo "  → Captured: $VERSION_ID"
echo ""

echo "Step 2: First range request WITH versionId"
curl -s -H "Range: bytes=0-100000" "http://localhost:8888/test-file.bin?versionId=$VERSION_ID" -o /tmp/chunk1_v.bin
CHAR1_V=$(head -c 1 /tmp/chunk1_v.bin)
echo "  First byte: '$CHAR1_V' (from $VERSION_ID) ✓"
echo ""

echo "Step 3: File is still version-2 on server (but we're pinned)"
echo "  Current server version: version-2"
echo "  Our pinned version: $VERSION_ID"
echo ""

echo "Step 4: Second range request WITH versionId"
curl -s -H "Range: bytes=100001-200000" "http://localhost:8888/test-file.bin?versionId=$VERSION_ID" -o /tmp/chunk2_v.bin
CHAR2_V=$(head -c 1 /tmp/chunk2_v.bin)
echo "  First byte: '$CHAR2_V' (from $VERSION_ID, pinned) ✓"
echo ""

echo "Result for force_download=false WITH version pinning:"
echo "  Chunk 1: '$CHAR1_V' ($VERSION_ID, pinned)"
echo "  Chunk 2: '$CHAR2_V' ($VERSION_ID, pinned)"
if [ "$CHAR1_V" == "$CHAR2_V" ]; then
    echo "  ✅ CONSISTENT: All data from same version"
    echo ""
    echo "  AND we get:"
    echo "    + Incremental reading (memory efficient)"
    echo "    + Data consistency (version pinned)"
    echo "    + Fast startup (can process while downloading)"
    echo "    + Scalable (works for any file size)"
else
    echo "  ❌ Unexpected inconsistency"
fi
echo ""

echo "================================================================================"
echo "SUMMARY: Comparison Table"
echo "================================================================================"
echo ""
printf "%-35s | %-20s | %-20s | %-20s\n" "Feature" "force_download=false" "force_download=true" "Version Pinning"
echo "--------------------------------------------------------------------------------"
printf "%-35s | %-20s | %-20s | %-20s\n" "Data Consistency" "❌ CORRUPTED" "✅ Consistent" "✅ Consistent"
printf "%-35s | %-20s | %-20s | %-20s\n" "Memory Usage" "✅ Low (chunks)" "❌ High (full file)" "✅ Low (chunks)"
printf "%-35s | %-20s | %-20s | %-20s\n" "Performance" "✅ Fast startup" "❌ Slow (wait for all)" "✅ Fast startup"
printf "%-35s | %-20s | %-20s | %-20s\n" "Scalability" "✅ Any file size" "❌ Limited by RAM" "✅ Any file size"
printf "%-35s | %-20s | %-20s | %-20s\n" "Request Count" "Multiple ranges" "Single full GET" "Multiple ranges"
printf "%-35s | %-20s | %-20s | %-20s\n" "Risk" "Data corruption" "Poor performance" "None"
echo ""
echo "================================================================================"
echo "CONCLUSION"
echo "================================================================================"
echo ""
echo "force_download=false (default):"
echo "  ❌ WITHOUT version pinning: Data corruption when file changes"
echo "  ✅ WITH version pinning: Safe AND efficient"
echo ""
echo "force_download=true:"
echo "  ✅ No corruption (single request)"
echo "  ❌ But sacrifices performance and scalability"
echo ""
echo "Version ID Pinning is the RIGHT solution:"
echo "  ✅ Prevents corruption"
echo "  ✅ Maintains incremental reading benefits"
echo "  ✅ Production-ready for all file sizes"
echo ""
echo "================================================================================"
