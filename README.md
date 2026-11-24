# canopy-cache-proxy

Independently deployable, loopback-only, SQLite-backed cache for Earth
Platform's canopy-height tiles, for MINECRAFT-574 / MINECRAFT-571.

Pinned source: https://github.com/pirut/earth-platform, tag `combined-73`
(= release "Earth Platform 0.1.0-alpha.73", commit `88b13c33954550c6ac660dceb743a4288a9aafac`).
Endpoint inventory below was read from that exact tag's `CanopySource.java`,
not `main`, and live-verified read-only against the actual upstream bucket.

## Status

`brooswit-intel/canopy-cache-proxy` now exists (as of this writing) but is
empty, and this agent's GitHub account (`booswrit`) has pull-only access
(`permissions.push: false`, confirmed via the GitHub API) — repo-only Write
is still pending Tayne's grant per the MINECRAFT-571 matrix. This tree is
ready to push verbatim once that lands; nothing has been pushed yet.

## Data source

Meta/Data for Good global canopy height v2 (DINOv3, ml3, 10m, p95):
`https://dataforgood-fb-data.s3.amazonaws.com/forests/v2/global/dinov3_global_chm_v2_ml3_10m_p95/chm/{tileid}.tif`.
Live-verified (read-only GET/HEAD, no credentials) against the real bucket:
plain S3 object storage, `Accept-Ranges: bytes`, real `206 Partial Content`
with a correct `Content-Range`, and `ETag`/`If-None-Match` → real `304`.
No bucket-level README/LICENSE object exists; the companion model repo
(`facebookresearch/HighResCanopyHeight`) licenses code/weights Apache-2.0 and
requests academic citation for the dataset — no term found that blocks
caching or a loopback redistribution proxy. Carries a citation note here,
not yet exercised as "live use."

## Run

```
python3 -m unittest test_proxy -v   # 17 tests, local mock upstream only
python3 proxy.py                    # serves on 127.0.0.1:8074 by default
```

## Contract

- Loopback-only bind (`127.0.0.1`, hardcoded in `run_server`).
- Fixed, allowlisted upstream (unchanged from the mod's own `CanopySource`
  default); override via `CANOPY_CACHE_UPSTREAM` — the per-service override
  point the ticket asks for, default untouched unless a caller sets it.
- Request path validated against `{tileid}.tif` (digits only) and refused
  otherwise — no arbitrary URL forwarding.
- SQLite file cache, keyed by tile path, restart-persistent, whole-object;
  Range requests served as 206 slices of the cached object, with a stable
  SHA-256 ETag supporting `If-None-Match` → 304.
- Cache lifetime is TTL -1 (John/MINECRAFT-571 comment 55740): a validated
  successful tile survives restart and any clock advance with zero extra
  upstream calls, but storage stays bounded (`max_bytes`, LRU eviction to
  90% on overflow) and is explicitly invalidatable (`TileCache.invalidate`).
- A transient upstream failure (5xx, connection error) is retried 3x with
  backoff and never cached; a non-200 response, a truncated transfer, or a
  200 whose body isn't actually a well-formed TIFF is never written to the
  cache under any circumstance.
- Concurrent cache misses for the same tile single-flight to exactly one
  upstream request.
- Dedicated upstream health-check endpoint `/__health` (John/MINECRAFT-571
  comment 55734/55737): TTL 0, never reads or writes the data cache, issues
  a bounded live minimal-Range probe against a fixed representative tile,
  and reports the real upstream state even while an ordinary warm tile keeps
  serving from SQLite during an outage.

## What this is not (yet)

- Not tested against the live S3 bucket for proxy traffic: per the ticket's
  no-live-use-before-terms-verification instruction, all proxy tests run
  against a local mock upstream serving the identical `{tileid}.tif`
  contract (structurally real TIFF signature, not an opaque byte blob); the
  bucket behavior cited above was checked directly but only with read-only
  GET/HEAD, never through this proxy.
- Not wired into the mod: `CanopySource`'s override hook and the
  MINECRAFT-580 collision-free integration lane own that, not duplicated
  here.
- No Zippy/in-game run yet, no merge/release.
