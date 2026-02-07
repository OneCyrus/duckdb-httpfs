#!/bin/bash
# Correct installation procedure for the patched httpfs extension

set -e

echo "================================================================================"
echo "Installing Patched HTTPfs Extension for dbt-duckdb"
echo "================================================================================"
echo ""

# Step 1: Download the artifact
echo "Step 1: Download the artifact from GitHub Actions"
echo "------------------------------------------------------------------------"
echo "Artifact URL: https://github.com/OneCyrus/duckdb-httpfs/actions/runs/21759383469"
echo ""
echo "For Linux x64, download: httpfs-v1.4.4-extension-linux_amd64"
echo "(This requires GitHub authentication)"
echo ""
echo "You should have a file like: httpfs-v1.4.4-extension-linux_amd64.zip"
echo ""

# For this example, assume the artifact is downloaded
ARTIFACT_ZIP="httpfs-v1.4.4-extension-linux_amd64.zip"

if [ ! -f "$ARTIFACT_ZIP" ]; then
    echo "❌ ERROR: Artifact file not found: $ARTIFACT_ZIP"
    echo ""
    echo "Please download it first from:"
    echo "https://github.com/OneCyrus/duckdb-httpfs/actions/runs/21759383469"
    exit 1
fi

# Step 2: Unzip the artifact
echo "Step 2: Unzip the artifact"
echo "------------------------------------------------------------------------"
TEMP_DIR=$(mktemp -d)
unzip -q "$ARTIFACT_ZIP" -d "$TEMP_DIR"
echo "Extracted to: $TEMP_DIR"
echo ""

# Step 3: Find the actual extension file
echo "Step 3: Locate the extension binary"
echo "------------------------------------------------------------------------"
echo "Contents of artifact:"
ls -lh "$TEMP_DIR"
echo ""

# The actual extension should be named httpfs.duckdb_extension
EXTENSION_FILE=$(find "$TEMP_DIR" -name "*.duckdb_extension" | head -1)

if [ -z "$EXTENSION_FILE" ]; then
    echo "❌ ERROR: No .duckdb_extension file found in artifact!"
    echo ""
    echo "Artifact contents:"
    find "$TEMP_DIR" -type f
    exit 1
fi

echo "Found extension: $EXTENSION_FILE"
EXTENSION_NAME=$(basename "$EXTENSION_FILE")
echo "Extension filename: $EXTENSION_NAME"
echo ""

# Step 4: Install to DuckDB extensions directory
echo "Step 4: Install extension"
echo "------------------------------------------------------------------------"
DUCKDB_VERSION="v1.4.4"
PLATFORM="linux_amd64"
EXTENSION_DIR="$HOME/.duckdb/extensions/$DUCKDB_VERSION/$PLATFORM"

mkdir -p "$EXTENSION_DIR"

# Copy with the CORRECT name (should be httpfs.duckdb_extension)
cp "$EXTENSION_FILE" "$EXTENSION_DIR/"

echo "Installed to: $EXTENSION_DIR/$EXTENSION_NAME"
echo ""

# Verify
echo "Step 5: Verify installation"
echo "------------------------------------------------------------------------"
ls -lh "$EXTENSION_DIR/$EXTENSION_NAME"
echo ""

# Cleanup
rm -rf "$TEMP_DIR"

echo "================================================================================"
echo "✅ Installation Complete!"
echo "================================================================================"
echo ""
echo "The extension is installed at:"
echo "  $EXTENSION_DIR/$EXTENSION_NAME"
echo ""
echo "For dbt-duckdb, use ONE of these configurations:"
echo ""
echo "Option 1 - Load from extension directory (RECOMMENDED):"
echo "----------------------------------------"
echo "# profiles.yml"
echo "your_profile:"
echo "  outputs:"
echo "    prod:"
echo "      type: duckdb"
echo "      extensions:"
echo "        - httpfs  # Will auto-load from ~/.duckdb/extensions/"
echo ""
echo "Option 2 - Explicit path:"
echo "----------------------------------------"
echo "# profiles.yml"
echo "your_profile:"
echo "  outputs:"
echo "    prod:"
echo "      type: duckdb"
echo "      init_sql:"
echo "        - \"INSTALL httpfs FROM '$EXTENSION_DIR/$EXTENSION_NAME'\""
echo "        - \"LOAD httpfs\""
echo ""
echo "DO NOT use the artifact filename - use the actual .duckdb_extension file!"
echo "================================================================================"
