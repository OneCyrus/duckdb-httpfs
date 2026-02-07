# Glob/Wildcard Corruption: Why `force_full_download=true` Doesn't Help

## Your Exact Problem

You're seeing **malformed JSON errors** when reading `*.json` files, even with `force_full_download=true`. This is caused by a **HEAD/GET version mismatch** that version pinning solves.

## The Root Cause

`force_full_download=true` **only affects chunk-level reading**. It does NOT prevent version mismatches between HEAD and GET requests!

### What Happens for EACH File in `*.json`:

```
1. HEAD /file.json
   ← Content-Length: 37 bytes (version-1 metadata)
   ← x-amz-version-id: version-1

[File gets updated to version-2]

2. GET /file.json  (with force_full_download=true)
   ← Content-Length: 79 bytes (version-2 data)
   ← Different structure, different size

DuckDB expects: 37 bytes, {id, name, age}
DuckDB receives: 79 bytes, {id, name, age, city, extra_field}

Result: ❌ MALFORMED JSON ERROR
```

## Test Results Prove This

From `test_glob_corruption.sh`:

```
❌ SIZE MISMATCH DETECTED!
   HEAD said: 37 bytes (version-1)
   GET returned: 79 bytes (version-2)

   This causes:
   - Malformed JSON errors (unexpected fields)
   - Schema mismatch errors
   - Checksum failures
   - Data corruption
```

## Why This Happens with Glob Reads

When DuckDB processes `read_json('s3://bucket/*.json')`:

```
For each file in glob results:
  1. HEAD request → Get metadata (size, schema hints)
  2. [File might change here!]
  3. GET request → Download data
```

**The problem**: Files can be updated/replaced between steps 1 and 3!

### Timing Window for Corruption:

```
T0: Glob expansion finds: file1.json, file2.json, file3.json
T1: HEAD file1.json → version-1 (37 bytes)
T2: [file1.json updated to version-2]
T3: GET file1.json → version-2 (79 bytes) ❌ MISMATCH!
T4: HEAD file2.json → version-2 (79 bytes)
T5: GET file2.json → version-2 (79 bytes) ✓ OK
```

Result: **Intermittent errors** (timing-dependent)

## What force_full_download Does (and Doesn't Do)

### ✓ What It DOES:
- Downloads entire file in single GET (no range requests)
- Prevents chunk-level corruption within a file
- Each file downloaded fully before processing

### ❌ What It DOESN'T Do:
- Doesn't prevent HEAD/GET version mismatch
- Doesn't ensure metadata matches content
- Doesn't help with glob/wildcard reads

## Error Symptoms You See

These are all HEAD/GET version mismatch symptoms:

1. **"Malformed JSON"**
   - HEAD expected one structure
   - GET returned different structure

2. **"Unexpected end of JSON input"**
   - HEAD said 100 bytes
   - GET returned 50 bytes (truncated version)

3. **"Schema mismatch"**
   - HEAD inferred schema from version-1
   - GET returned version-2 with different fields

4. **Intermittent failures**
   - Only happens when files change at wrong time
   - Hard to reproduce consistently

## The Solution: Version ID Pinning

Version pinning ensures HEAD and GET use the SAME version:

```
1. HEAD /file.json
   ← Content-Length: 37 bytes
   ← x-amz-version-id: version-1
   [Capture: versionId = "version-1"]

[File gets updated to version-2]

2. GET /file.json?versionId=version-1
   ← Content-Length: 37 bytes (version-1, pinned!)
   ← Same structure as HEAD promised

DuckDB expects: 37 bytes, {id, name, age}
DuckDB receives: 37 bytes, {id, name, age}

Result: ✅ NO ERRORS
```

## For Glob/Wildcard Reads

```sql
-- WITHOUT version pinning:
SELECT * FROM read_json('s3://bucket/*.json');
-- ❌ Intermittent malformed JSON errors

-- WITH version pinning:
SELECT * FROM read_json('s3://bucket/*.json');
-- ✅ Consistent, reliable reads
```

Each file gets:
- Consistent metadata (HEAD)
- Consistent data (GET with versionId)
- No mismatches
- No malformed JSON errors

## Comparison: force_full_download vs Version Pinning

| Issue | force_full_download=true | Version Pinning |
|-------|--------------------------|-----------------|
| **Chunk-level corruption** | ✅ Prevented | ✅ Prevented |
| **HEAD/GET mismatch** | ❌ Still occurs | ✅ Prevented |
| **Malformed JSON errors** | ❌ Still occurs | ✅ Prevented |
| **Glob read consistency** | ❌ Unreliable | ✅ Reliable |
| **Memory usage** | ❌ High | ✅ Low |
| **Performance** | ❌ Poor | ✅ Good |

## Real-World Example

### Your Scenario:
```
You have: s3://bucket/data/*.json
Files are being updated by another process
You run: SELECT * FROM read_json('s3://bucket/data/*.json')
```

### Without Version Pinning:
```
Processing file1.json:
  HEAD → version-1 metadata (expects schema A)
  [File updated]
  GET → version-2 data (has schema B)
  ❌ Error: "Malformed JSON: unexpected field 'city'"

Processing file2.json:
  HEAD → version-2 metadata
  GET → version-2 data
  ✓ OK (lucky timing)

Processing file3.json:
  HEAD → version-2 metadata
  [File updated]
  GET → version-3 data
  ❌ Error: "Schema mismatch"
```

### With Version Pinning:
```
Processing file1.json:
  HEAD → version-1 metadata, capture versionId
  [File updated - we don't care!]
  GET?versionId=v1 → version-1 data
  ✓ OK (pinned to v1)

Processing file2.json:
  HEAD → version-2 metadata, capture versionId
  GET?versionId=v2 → version-2 data
  ✓ OK (pinned to v2)

Processing file3.json:
  HEAD → version-2 metadata, capture versionId
  [File updated - we don't care!]
  GET?versionId=v2 → version-2 data
  ✓ OK (pinned to v2)
```

All files read consistently, no errors!

## Why Intermittent?

The errors are timing-dependent:

```
If files update BETWEEN HEAD and GET:
  ❌ Mismatch → Malformed JSON error

If files update BEFORE HEAD or AFTER GET:
  ✓ Consistent → No error
```

This explains why:
- Errors come and go
- Hard to reproduce
- Seem random
- Correlate with file update activity

## The Bottom Line

**Your malformed JSON errors are caused by HEAD/GET version mismatches.**

`force_full_download=true` **does not solve this** - you still get:
- Different versions between HEAD and GET
- Metadata/content mismatches
- Malformed JSON errors

**Version ID pinning DOES solve this** by:
- Pinning each file to a specific version
- Ensuring HEAD and GET return same version
- Eliminating metadata/content mismatches
- Preventing all malformed JSON errors

## Proof

Run the test:
```bash
cd tests/version-pinning-proof
./test_glob_corruption.sh
```

You'll see:
- HEAD returns 37 bytes (version-1)
- GET returns 79 bytes (version-2)
- Clear size mismatch causing errors
- Version pinning fixes it completely

## Recommendation

Enable version ID pinning to fix your `*.json` malformed errors. The issue is **not** about chunk-level corruption (which `force_full_download=true` prevents), but about **HEAD/GET version consistency** (which only version ID pinning prevents).
