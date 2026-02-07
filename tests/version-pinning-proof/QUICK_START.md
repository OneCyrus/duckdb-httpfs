# Quick Start: Use Patched HTTPfs with dbt-duckdb

## TL;DR

```bash
# 1. Get the extension (choose platform)
# Linux x64: httpfs-v1.4.4-extension-linux_amd64
# Download from: https://github.com/OneCyrus/duckdb-httpfs/actions/runs/21759383469

# 2. Place it where DuckDB can find it
mkdir -p ~/.duckdb/extensions/v1.4.4/linux_amd64
cp httpfs.duckdb_extension ~/.duckdb/extensions/v1.4.4/linux_amd64/

# 3. Configure dbt (add to profiles.yml)
your_profile:
  outputs:
    prod:
      type: duckdb
      path: /path/to/db.duckdb
      init_sql:
        - "INSTALL httpfs FROM '~/.duckdb/extensions/v1.4.4/linux_amd64/httpfs.duckdb_extension'"
        - "LOAD httpfs"

# 4. Test it
dbt run --select your_model

# 5. Verify (should see NO malformed JSON errors)
```

## What You Get

✅ No more malformed JSON errors on `*.json` reads
✅ Consistent data even when files update during reads
✅ Reliable glob pattern operations
✅ Safe to use default `force_download=false`

## Verification

```sql
-- Test in DuckDB directly
INSTALL httpfs FROM '~/.duckdb/extensions/v1.4.4/linux_amd64/httpfs.duckdb_extension';
LOAD httpfs;

-- Read your S3 data
SELECT * FROM read_json('s3://your-bucket/data/*.json') LIMIT 10;

-- Should work WITHOUT errors even if files are being updated!
```

## Need Help?

See `PRODUCTION_TESTING_GUIDE.md` for detailed instructions.

## Rollback

If you need to revert:

```bash
# Remove custom extension
rm ~/.duckdb/extensions/v1.4.4/linux_amd64/httpfs.duckdb_extension

# DuckDB will auto-download official version on next use
```

## Why This Matters

**Before (without version pinning):**
```
Reading file1.json...
  HEAD → metadata from version-1
  [file updated]
  GET → data from version-2
  ❌ Error: Malformed JSON (size/schema mismatch)
```

**After (with version pinning):**
```
Reading file1.json...
  HEAD → metadata from version-1, capture versionId
  [file updated - don't care!]
  GET with versionId → data from version-1
  ✅ Success: Metadata and content match perfectly
```
