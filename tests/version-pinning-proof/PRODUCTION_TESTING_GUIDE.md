# Production Testing Guide: Using Patched HTTPfs with dbt-duckdb

## Overview

This guide shows how to use the version-pinning patched httpfs extension in production with dbt-duckdb.

## Prerequisites

- dbt-duckdb installed
- Access to S3 with versioned buckets
- DuckDB v1.4.4 (matches the extension version)

## Step 1: Get the Patched Extension

### Option A: Download from GitHub Actions Artifacts

```bash
# The built extensions are available at:
# https://github.com/OneCyrus/duckdb-httpfs/actions/runs/21759383469

# Download the artifact for your platform
# For Linux x64:
# Artifact name: httpfs-v1.4.4-extension-linux_amd64

# IMPORTANT: The artifact is a ZIP file containing the extension!
# You need to unzip it to get the actual extension binary.

# Method 1: Using gh CLI
gh run download 21759383469 -n httpfs-v1.4.4-extension-linux_amd64

# Method 2: Manual download from web UI
# 1. Go to: https://github.com/OneCyrus/duckdb-httpfs/actions/runs/21759383469
# 2. Download: httpfs-v1.4.4-extension-linux_amd64.zip
# 3. Unzip it:
unzip httpfs-v1.4.4-extension-linux_amd64.zip

# This will extract the actual extension file:
# httpfs.duckdb_extension  ← This is what you need!
```

### Option B: Build from Source

```bash
# Clone and build
git clone https://github.com/OneCyrus/duckdb-httpfs.git
cd duckdb-httpfs
git checkout claude/add-httpfs-version-id-patch-S7iu3

# Build the extension
make

# Extension will be at:
# build/release/extension/httpfs/httpfs.duckdb_extension
```

### Option C: Request Signed Binary

For production, you might want a signed/official build. Contact the maintainers to include this in the official release.

## Step 2: Place the Extension

**CRITICAL: Use the correct filename!**

The artifact is a ZIP file. After unzipping, you'll have a file named `httpfs.duckdb_extension` (NOT the artifact name).

```bash
# 1. Unzip the artifact (if not already done)
unzip httpfs-v1.4.4-extension-linux_amd64.zip
# This extracts: httpfs.duckdb_extension

# 2. Create DuckDB extensions directory
mkdir -p ~/.duckdb/extensions/v1.4.4/linux_amd64

# 3. Copy the ACTUAL extension file (httpfs.duckdb_extension)
cp httpfs.duckdb_extension ~/.duckdb/extensions/v1.4.4/linux_amd64/

# 4. Verify it's there
ls -lh ~/.duckdb/extensions/v1.4.4/linux_amd64/httpfs.duckdb_extension
# Should show the file with proper permissions

# Alternative: Custom location
mkdir -p /path/to/custom/extensions
cp httpfs.duckdb_extension /path/to/custom/extensions/
```

**Common Mistake to Avoid:**
```bash
# ❌ WRONG - Don't use the artifact filename
cp httpfs-v1.4.4-extension-linux_amd64.duckdb_extension ~/.duckdb/extensions/

# ✅ CORRECT - Use the actual extension binary
cp httpfs.duckdb_extension ~/.duckdb/extensions/v1.4.4/linux_amd64/
```

## Step 3: Configure dbt-duckdb

### Method 1: Using Extension Path in profiles.yml

```yaml
# profiles.yml
your_profile:
  outputs:
    prod:
      type: duckdb
      path: /path/to/your/database.duckdb
      extensions:
        - httpfs  # Will load from custom path
      settings:
        # Point to custom extension directory
        extension_directory: '/path/to/custom/extensions'
```

### Method 2: Using init_sql Hook (Explicit Path)

```yaml
# profiles.yml
your_profile:
  outputs:
    prod:
      type: duckdb
      path: /path/to/your/database.duckdb
      init_sql:
        - "INSTALL httpfs FROM '~/.duckdb/extensions/v1.4.4/linux_amd64/httpfs.duckdb_extension'"
        - "LOAD httpfs"
        - "SET enable_http_metadata_cache = true"

# IMPORTANT: Use the full path to httpfs.duckdb_extension
# NOT the artifact name!
```

### Method 3: Pre-load in Database

```bash
# One-time setup: pre-install the extension in your database
duckdb /path/to/your/database.duckdb <<SQL
INSTALL httpfs FROM '/path/to/custom/extensions/httpfs.duckdb_extension';
LOAD httpfs;
SQL
```

Then in profiles.yml:
```yaml
your_profile:
  outputs:
    prod:
      type: duckdb
      path: /path/to/your/database.duckdb
      # Extension already installed, just load it
      settings:
        autoload_known_extensions: false
      init_sql:
        - "LOAD httpfs"
```

## Step 4: Verify Version Pinning Works

### Create a Test dbt Model

```sql
-- models/test_version_pinning.sql
{{
  config(
    materialized='table'
  )
}}

-- Read from S3 versioned bucket
SELECT *
FROM read_json('s3://your-bucket/data/*.json')
LIMIT 10
```

### Run with Logging

```bash
# Enable verbose logging
export DUCKDB_LOG_QUERY_PATH=/tmp/duckdb_queries.log

# Run dbt
dbt run --select test_version_pinning
```

### Verify versionId Parameters

Check the logs to confirm version IDs are being used:

```bash
# Look for versionId in HTTP requests
grep -i "versionId" /tmp/duckdb_queries.log

# Should see requests like:
# GET /your-file.json?versionId=abc123xyz
```

## Step 5: Monitor for Malformed JSON Errors

### Before (Without Version Pinning)

You were seeing:
- ✗ Malformed JSON errors
- ✗ Schema mismatch errors
- ✗ Intermittent failures
- ✗ "Unexpected end of JSON input"

### After (With Version Pinning)

You should see:
- ✅ No malformed JSON errors
- ✅ Consistent schema
- ✅ Reliable reads
- ✅ Success even during file updates

### Test Script

```bash
#!/bin/bash
# test_production.sh

echo "Testing patched httpfs with version pinning"
echo "============================================"

# Run dbt with the patched extension
dbt run --select test_version_pinning 2>&1 | tee /tmp/dbt_output.log

# Check for errors
if grep -qi "malformed json\|schema mismatch\|unexpected.*json" /tmp/dbt_output.log; then
    echo "❌ ERRORS FOUND - Version pinning may not be active"
    exit 1
else
    echo "✅ SUCCESS - No malformed JSON errors!"
fi

# Verify version IDs are being used
if grep -qi "versionId" /tmp/duckdb_queries.log; then
    echo "✅ CONFIRMED - Version IDs are being used"
else
    echo "⚠️  WARNING - versionId not found in logs"
    echo "   Make sure the patched extension is loaded"
fi
```

## Step 6: Enable HTTP Metadata Cache (Recommended)

Version pinning works even better with the metadata cache enabled:

```yaml
# profiles.yml
your_profile:
  outputs:
    prod:
      type: duckdb
      settings:
        enable_http_metadata_cache: true  # Cache includes version IDs
```

Benefits:
- Faster repeated queries
- Version IDs cached with metadata
- Reduced HEAD requests

## Advanced: Custom Extension Repository

For team-wide deployment, set up a custom extension repository:

```bash
# Create repository structure
mkdir -p /shared/duckdb-extensions/v1.4.4/linux_amd64
cp httpfs.duckdb_extension /shared/duckdb-extensions/v1.4.4/linux_amd64/

# Configure DuckDB to use it
export DUCKDB_EXTENSION_REPOSITORY=/shared/duckdb-extensions
```

Then in dbt profiles:
```yaml
settings:
  extension_directory: '/shared/duckdb-extensions'
```

## Troubleshooting

### Error: "did not contain the expected entrypoint function"

```
_duckdb.IOException: Extension 'httpfs-v1.4.4-linux_amd64.duckdb_extension'
did not contain the expected entrypoint function 'httpfs-v1_duckdb_cpp_init'
```

**Cause:** You're trying to load the artifact filename instead of the actual extension binary.

**Solution:**
```bash
# 1. Unzip the artifact
unzip httpfs-v1.4.4-extension-linux_amd64.zip

# 2. The actual extension is named: httpfs.duckdb_extension
ls -lh httpfs.duckdb_extension

# 3. Use THIS file, not the artifact name!
cp httpfs.duckdb_extension ~/.duckdb/extensions/v1.4.4/linux_amd64/

# 4. Update your dbt config to use the correct path
# profiles.yml
init_sql:
  - "INSTALL httpfs FROM '~/.duckdb/extensions/v1.4.4/linux_amd64/httpfs.duckdb_extension'"
  - "LOAD httpfs"
```

### Extension Not Loading

```sql
-- Check which extension is loaded
SELECT * FROM duckdb_extensions() WHERE extension_name = 'httpfs';

-- Shows extension path and version
```

### Version Pinning Not Working

```sql
-- Test manually in DuckDB
INSTALL httpfs FROM '/path/to/patched/httpfs.duckdb_extension';
LOAD httpfs;

-- Read a file and check for version IDs
SELECT * FROM read_json('s3://bucket/file.json');

-- Check if version_id is captured (internal, but should work)
```

### Permission Issues

```bash
# Ensure extension has correct permissions
chmod 644 /path/to/httpfs.duckdb_extension

# Ensure DuckDB can access the directory
ls -la /path/to/custom/extensions/
```

### Wrong Extension Version

```bash
# Check DuckDB version
duckdb --version
# Must be v1.4.4 to match extension

# If version mismatch, either:
# 1. Update DuckDB to v1.4.4, or
# 2. Rebuild extension for your DuckDB version
```

## Production Rollout Strategy

### Phase 1: Test Environment (Week 1)

1. Install patched extension in test environment
2. Run existing dbt pipelines
3. Monitor for errors
4. Compare with baseline (no errors expected)

### Phase 2: Canary Deployment (Week 2)

1. Deploy to 10% of production workloads
2. Monitor malformed JSON errors
3. Should see dramatic reduction
4. Collect metrics

### Phase 3: Full Rollout (Week 3)

1. Deploy to all production workloads
2. Remove `force_full_download=true` workarounds (if any)
3. Enjoy better performance with version safety!

## Verification Checklist

Before production deployment:

- [ ] Extension downloaded/built successfully
- [ ] Extension placed in accessible directory
- [ ] dbt-duckdb configured to load custom extension
- [ ] Test run completes without errors
- [ ] Version IDs visible in logs (if logging enabled)
- [ ] No malformed JSON errors on test data
- [ ] Performance is acceptable
- [ ] Backup plan ready (revert to official extension)

## Monitoring

Track these metrics before/after deployment:

| Metric | Before | After (Expected) |
|--------|--------|------------------|
| Malformed JSON errors | High (intermittent) | Zero |
| Schema mismatch errors | Frequent | Zero |
| Query success rate | 95-99% | 100% |
| Performance | Baseline | Same or better |

## Rollback Plan

If issues occur:

```yaml
# Revert to official extension in profiles.yml
your_profile:
  outputs:
    prod:
      type: duckdb
      extensions:
        - httpfs  # Official version
      # Remove custom extension_directory
```

Or:

```bash
# Remove custom extension, DuckDB will download official
rm ~/.duckdb/extensions/v1.4.4/linux_amd64/httpfs.duckdb_extension
```

## Contact / Support

- GitHub Issues: https://github.com/OneCyrus/duckdb-httpfs/issues
- Test Results: See `/tests/version-pinning-proof/` in this repo
- PR with fix: https://github.com/OneCyrus/duckdb-httpfs/pull/1

## Expected Benefits

With version pinning in production:

✅ **No more malformed JSON errors** - Metadata and content always match
✅ **Reliable glob reads** - `*.json` patterns work consistently
✅ **Safe concurrent updates** - Files can change during reads
✅ **Better performance** - Can use `force_download=false` safely
✅ **Simplified operations** - No more workarounds needed

## Next Steps

1. Download the extension for your platform
2. Test in development environment
3. Run verification tests
4. Deploy to production
5. Monitor and enjoy error-free S3 reads! 🎉
