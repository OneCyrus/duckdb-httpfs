#!/usr/bin/env python3
"""
Mock S3 Server to test version ID pinning in DuckDB HTTPfs extension

This server simulates S3 behavior with versioned objects:
1. Returns x-amz-version-id header in responses
2. Serves different content based on versionId query parameter
3. Can change the "current" version mid-download to test version pinning
"""

import http.server
import socketserver
import urllib.parse
import threading
import time
import sys
import os
from datetime import datetime

PORT = 8888

# File versions - we'll have two versions of the same file
FILE_VERSIONS = {
    "version-1": b"A" * 1000000,  # 1MB of 'A's - version 1
    "version-2": b"B" * 1000000,  # 1MB of 'B's - version 2 (modified)
}

# Current version (can be changed during test)
current_version = "version-1"
version_lock = threading.Lock()

# Track requests for debugging
request_log = []


class MockS3Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        """Log requests to our custom log"""
        message = f"{self.command} {self.path} - {format % args}"
        request_log.append(message)
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] {message}")

    def do_HEAD(self):
        """Handle HEAD requests"""
        self.handle_request(send_body=False)

    def do_GET(self):
        """Handle GET requests"""
        self.handle_request(send_body=True)

    def handle_request(self, send_body=True):
        """Handle both HEAD and GET requests"""
        # Parse URL and query parameters
        parsed = urllib.parse.urlparse(self.path)
        query_params = urllib.parse.parse_qs(parsed.query)

        # Extract versionId if provided
        requested_version = query_params.get('versionId', [None])[0]

        # Determine which version to serve
        with version_lock:
            if requested_version:
                # If versionId is specified, serve that specific version
                version_to_serve = requested_version
                print(f"  -> Serving pinned version: {version_to_serve}")
            else:
                # Otherwise serve the current version
                version_to_serve = current_version
                print(f"  -> Serving current version: {version_to_serve}")

        # Get the file content for this version
        if version_to_serve not in FILE_VERSIONS:
            self.send_error(404, f"Version {version_to_serve} not found")
            return

        file_content = FILE_VERSIONS[version_to_serve]
        content_length = len(file_content)

        # Parse Range header if present
        range_header = self.headers.get('Range')
        if range_header:
            # Parse byte range: "bytes=start-end"
            try:
                byte_range = range_header.replace('bytes=', '')
                if '-' in byte_range:
                    parts = byte_range.split('-')
                    start = int(parts[0]) if parts[0] else 0
                    end = int(parts[1]) if parts[1] else content_length - 1

                    # Validate range
                    if start >= content_length or end >= content_length or start > end:
                        self.send_error(416, "Range Not Satisfiable")
                        return

                    # Send partial content
                    self.send_response(206)  # Partial Content
                    self.send_header('Content-Type', 'application/octet-stream')
                    self.send_header('Content-Length', str(end - start + 1))
                    self.send_header('Content-Range', f'bytes {start}-{end}/{content_length}')
                    self.send_header('Accept-Ranges', 'bytes')
                    self.send_header('x-amz-version-id', version_to_serve)
                    self.send_header('ETag', f'"{version_to_serve}"')
                    self.end_headers()

                    if send_body:
                        self.wfile.write(file_content[start:end+1])

                    print(f"  -> Sent bytes {start}-{end} from {version_to_serve}")
                    return

            except Exception as e:
                print(f"  -> Error parsing range: {e}")
                self.send_error(400, f"Invalid range: {e}")
                return

        # Send full content
        self.send_response(200)
        self.send_header('Content-Type', 'application/octet-stream')
        self.send_header('Content-Length', str(content_length))
        self.send_header('Accept-Ranges', 'bytes')
        self.send_header('x-amz-version-id', version_to_serve)
        self.send_header('ETag', f'"{version_to_serve}"')
        self.end_headers()

        if send_body:
            self.wfile.write(file_content)

        print(f"  -> Sent full file from {version_to_serve}")


def change_current_version(new_version, delay=0):
    """Change the current version after a delay"""
    if delay > 0:
        print(f"\n[SCHEDULED] Will change current version to {new_version} in {delay} seconds...")
        time.sleep(delay)

    global current_version
    with version_lock:
        old_version = current_version
        current_version = new_version
        print(f"\n{'='*60}")
        print(f"[VERSION CHANGE] {old_version} -> {new_version}")
        print(f"{'='*60}\n")


def start_server():
    """Start the mock S3 server"""
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("", PORT), MockS3Handler) as httpd:
        print(f"Mock S3 Server running on port {PORT}")
        print(f"Test file available at: http://localhost:{PORT}/test-file.bin")
        print(f"Initial version: {current_version}")
        print("=" * 60)

        # Schedule version change in 2 seconds (enough time for initial HEAD request)
        version_change_thread = threading.Thread(
            target=change_current_version,
            args=("version-2", 2)
        )
        version_change_thread.daemon = True
        version_change_thread.start()

        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down server...")
            print(f"\nTotal requests: {len(request_log)}")


if __name__ == "__main__":
    start_server()
