"""Loopback-only caching proxy for Earth Platform's canopy-height tiles.

Upstream contract (inventoried from Earth Platform 0.1.0-alpha.73 / tag combined-73,
geography/src/main/java/io/earthplatform/geography/globe/CanopySource.java, and
live-verified directly against the actual bucket):
  base: https://dataforgood-fb-data.s3.amazonaws.com/forests/v2/global/dinov3_global_chm_v2_ml3_10m_p95/chm/
  path: {tileid}.tif  (Meta/Data for Good global canopy height v2, DINOv3 ml3, 10m,
        p95; tileid is a fixed-width numeric tile key, e.g. 0013113.tif). Each
        object is a Cloud-Optimized GeoTIFF several MB in size; the bucket
        confirms Accept-Ranges: bytes, real 206 Partial Content with a correct
        Content-Range, and ETag-based conditional 304s (verified live, read-only,
        no credentials, as part of this ticket's terms/native-request check).

This proxy fetches the whole object once (matching the other COG-backed source
proxies in this Story) and then serves Range and conditional (ETag) requests
to the client out of that cached copy, so cache-key correctness for byte
ranges/conditionals is proven once and reused regardless of whether the mod's
own client ever issues a sub-range GET upstream.

Per John's cache-lifetime decision (MINECRAFT-571 comment 55740): a validated
successful data response has TTL -1 (no time-based expiration) - restart and a
large clock advance must not re-fetch it - but storage stays bounded (byte cap
+ LRU eviction) and explicitly invalidatable; only the dedicated health-check
endpoint is TTL 0 and never touches the data cache.

Default upstream is exactly the mod's existing default (no behavior change for
a caller that never overrides it). CANOPY_CACHE_UPSTREAM overrides it, e.g. for
tests pointed at a local stand-in - the "endpoint override with unchanged
defaults" the ticket asks for.
"""
from __future__ import annotations

import hashlib
import http.client
import os
import re
import sqlite3
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DEFAULT_UPSTREAM = (
    "https://dataforgood-fb-data.s3.amazonaws.com/"
    "forests/v2/global/dinov3_global_chm_v2_ml3_10m_p95/chm/"
)
# Live-verified representative tile, used only by the dedicated health-check
# endpoint's bounded upstream probe - never read from or written to the data cache.
DEFAULT_HEALTH_PATH = "0013113.tif"

_TILE_RE = re.compile(r"^[0-9]{1,10}\.tif$")


def _parse_tile_path(path: str) -> str | None:
    """Validates the request names a canopy tile and nothing else; returns the
    normalized upstream path, or None to refuse (no arbitrary forwarding)."""
    name = path.strip("/")
    if "/" in name or not _TILE_RE.match(name):
        return None
    return name


@dataclass
class Stats:
    upstream_requests: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    not_modified: int = 0
    bytes_downloaded: int = 0
    evictions: int = 0
    health_checks: int = 0
    health_failures: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def as_dict(self) -> dict:
        with self.lock:
            return {
                "upstream_requests": self.upstream_requests,
                "cache_hits": self.cache_hits,
                "cache_misses": self.cache_misses,
                "not_modified": self.not_modified,
                "bytes_downloaded": self.bytes_downloaded,
                "evictions": self.evictions,
                "health_checks": self.health_checks,
                "health_failures": self.health_failures,
            }


class TileCache:
    """SQLite-backed, restart-persistent, bounded, whole-object tile cache.

    Never stores a response unless the upstream fetch completed in full as a
    successful (200) response with a structurally valid TIFF body: a timeout,
    a dropped connection, a non-200 status, a truncated transfer, or a 200
    that isn't actually a TIFF is never written, so a transient failure or
    bad upstream body can never be served as cached data later. TTL -1 (no
    time-based expiration) per John's cache-lifetime decision - entries are
    only removed by explicit invalidation or LRU eviction under the byte cap.
    """

    def __init__(self, db_path: str, max_bytes: int):
        self.db_path = db_path
        self.max_bytes = max_bytes
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS tiles (
                 path TEXT PRIMARY KEY,
                 etag TEXT NOT NULL,
                 body BLOB NOT NULL,
                 size INTEGER NOT NULL,
                 created_at REAL NOT NULL,
                 last_used_at REAL NOT NULL
               )"""
        )
        self._conn.commit()
        self._db_lock = threading.Lock()

    def get(self, path: str) -> tuple[str, bytes] | None:
        with self._db_lock:
            row = self._conn.execute(
                "SELECT etag, body FROM tiles WHERE path = ?", (path,)
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                "UPDATE tiles SET last_used_at = ? WHERE path = ?", (time.time(), path)
            )
            self._conn.commit()
        return row[0], row[1]

    def put(self, path: str, body: bytes) -> str:
        etag = hashlib.sha256(body).hexdigest()
        now = time.time()
        with self._db_lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO tiles (path, etag, body, size, created_at, last_used_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (path, etag, body, len(body), now, now),
            )
            self._conn.commit()
        return etag

    def invalidate(self, path: str) -> bool:
        """Explicit invalidation, independent of TTL/eviction - required
        alongside TTL -1 so a dataset revision can still be forced to refetch."""
        with self._db_lock:
            cur = self._conn.execute("DELETE FROM tiles WHERE path = ?", (path,))
            self._conn.commit()
            return cur.rowcount > 0

    def total_bytes(self) -> int:
        with self._db_lock:
            row = self._conn.execute("SELECT COALESCE(SUM(size), 0) FROM tiles").fetchone()
            return row[0]

    def evict_to_fit(self, stats: Stats) -> None:
        if self.max_bytes <= 0:
            return
        target = self.max_bytes * 9 // 10
        with self._db_lock:
            total = self._conn.execute("SELECT COALESCE(SUM(size), 0) FROM tiles").fetchone()[0]
            if total <= self.max_bytes:
                return
            rows = self._conn.execute(
                "SELECT path, size FROM tiles ORDER BY last_used_at ASC"
            ).fetchall()
            removed = 0
            for path, size in rows:
                if total - removed <= target:
                    break
                self._conn.execute("DELETE FROM tiles WHERE path = ?", (path,))
                removed += size
                with stats.lock:
                    stats.evictions += 1
            self._conn.commit()

    def close(self) -> None:
        self._conn.close()


class UpstreamError(Exception):
    pass


def _is_well_formed_tiff(body: bytes) -> bool:
    """Structural sanity check: real TIFF byte-order signature (classic or
    BigTIFF, either endianness) plus a minimum plausible header length. This
    does not parse every IFD - it exists to catch the same class of problem
    the other source proxies guard against: an HTTP 200 whose body is
    actually an S3 error page or a truncated write, not real tile data."""
    if len(body) < 16:
        return False
    if body[:2] not in (b"II", b"MM"):
        return False
    magic = int.from_bytes(body[2:4], "little" if body[:2] == b"II" else "big")
    return magic in (42, 43)  # 42 = classic TIFF, 43 = BigTIFF


def fetch_upstream(upstream_base: str, tile_path: str, timeout: float = 15.0, retries: int = 3) -> bytes:
    """Whole-object GET of the tile from the allowlisted upstream: fixed
    retries with backoff, success is 200 only, the body is read to completion
    and checked against the declared Content-Length before being treated as
    valid (no partial body is ever returned), and the bytes must actually be
    a TIFF. A malformed response ("success" is a lie) is treated the same as
    any other failed attempt: retried, never cached."""
    parsed = urllib.parse.urlsplit(upstream_base)
    full_path = parsed.path.rstrip("/") + "/" + tile_path
    last_exc: Exception | None = None
    for attempt in range(retries):
        if attempt:
            time.sleep(0.05 * (2 ** attempt))
        conn_cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        conn = conn_cls(parsed.netloc, timeout=timeout)
        try:
            conn.request("GET", full_path, headers={"User-Agent": "earth-platform-canopy-cache/1"})
            resp = conn.getresponse()
            body = resp.read()
            if resp.status != 200:
                last_exc = UpstreamError(f"HTTP {resp.status} for {full_path}")
                continue
            declared = resp.getheader("Content-Length")
            if declared is not None and int(declared) != len(body):
                last_exc = UpstreamError(f"truncated transfer for {full_path}: declared {declared}, got {len(body)}")
                continue
            if not _is_well_formed_tiff(body):
                last_exc = UpstreamError(f"malformed TIFF (HTTP 200 but not a valid tile) for {full_path}")
                continue
            return body
        except OSError as net_err:
            last_exc = net_err
            continue
        finally:
            conn.close()
    raise UpstreamError(f"upstream fetch failed after {retries} attempts: {last_exc}")


def probe_upstream_health(upstream_base: str, health_path: str, timeout: float = 5.0) -> bool:
    """Dedicated health-check probe (John/MINECRAFT-571 comment 55734/55737):
    a bounded, minimum live Range request against the fixed upstream for a
    representative tile. Never reads or writes the data cache (TTL 0, no
    cache read/write), and never substitutes stale cached success for a real
    upstream failure."""
    parsed = urllib.parse.urlsplit(upstream_base)
    full_path = parsed.path.rstrip("/") + "/" + health_path
    conn_cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    conn = conn_cls(parsed.netloc, timeout=timeout)
    try:
        conn.request(
            "GET",
            full_path,
            headers={"User-Agent": "earth-platform-canopy-cache/1", "Range": "bytes=0-15"},
        )
        resp = conn.getresponse()
        resp.read()
        return resp.status in (200, 206)
    except OSError:
        return False
    finally:
        conn.close()


class _Flight(threading.Event):
    """An Event that also carries its own leader's result, so every waiter
    reads its own reference instead of racing the others over a shared dict."""
    result = None
    error: Exception | None = None


class _SingleFlight:
    def __init__(self):
        self._lock = threading.Lock()
        self._inflight: dict[str, _Flight] = {}

    def run(self, key: str, loader):
        with self._lock:
            flight = self._inflight.get(key)
            if flight is not None:
                is_leader = False
            else:
                flight = _Flight()
                self._inflight[key] = flight
                is_leader = True
        if is_leader:
            try:
                flight.result = loader()
            except Exception as exc:  # noqa: BLE001 - propagated to every waiter
                flight.error = exc
            finally:
                with self._lock:
                    self._inflight.pop(key, None)
                flight.set()
        else:
            flight.wait()
        if flight.error is not None:
            raise flight.error
        return flight.result


class CanopyCacheProxy:
    def __init__(
        self,
        db_path: str,
        upstream_base: str | None = None,
        max_bytes: int = 512 * 1024 * 1024,
        health_path: str | None = None,
    ):
        self.upstream_base = upstream_base or os.environ.get("CANOPY_CACHE_UPSTREAM", DEFAULT_UPSTREAM)
        self.health_path = health_path or os.environ.get("CANOPY_CACHE_HEALTH_PATH", DEFAULT_HEALTH_PATH)
        self.cache = TileCache(db_path, max_bytes)
        self.stats = Stats()
        self._single_flight = _SingleFlight()

    def serve_tile(self, request_path: str, if_none_match: str | None, range_header: str | None):
        """Returns (status, headers_dict, body_bytes) for one client request."""
        tile_path = _parse_tile_path(request_path)
        if tile_path is None:
            return 404, {}, b""

        cached = self.cache.get(tile_path)
        if cached is None:
            def load():
                body = fetch_upstream(self.upstream_base, tile_path)
                with self.stats.lock:
                    self.stats.upstream_requests += 1
                    self.stats.bytes_downloaded += len(body)
                etag = self.cache.put(tile_path, body)
                self.cache.evict_to_fit(self.stats)
                return etag, body

            with self.stats.lock:
                self.stats.cache_misses += 1
            try:
                etag, body = self._single_flight.run(tile_path, load)
            except UpstreamError:
                return 502, {}, b""
        else:
            with self.stats.lock:
                self.stats.cache_hits += 1
            etag, body = cached

        if if_none_match is not None and if_none_match == etag:
            with self.stats.lock:
                self.stats.not_modified += 1
            return 304, {"ETag": etag}, b""

        if range_header and range_header.startswith("bytes="):
            start_s, _, end_s = range_header[len("bytes="):].partition("-")
            start = int(start_s) if start_s else 0
            end = int(end_s) if end_s else len(body) - 1
            end = min(end, len(body) - 1)
            if start > end or start >= len(body):
                return 416, {"ETag": etag, "Content-Range": f"bytes */{len(body)}"}, b""
            chunk = body[start:end + 1]
            headers = {
                "ETag": etag,
                "Content-Range": f"bytes {start}-{end}/{len(body)}",
                "Content-Type": "image/tiff",
            }
            return 206, headers, chunk

        return 200, {"ETag": etag, "Content-Type": "image/tiff"}, body

    def check_health(self) -> bool:
        with self.stats.lock:
            self.stats.health_checks += 1
        ok = probe_upstream_health(self.upstream_base, self.health_path)
        if not ok:
            with self.stats.lock:
                self.stats.health_failures += 1
        return ok

    def close(self):
        self.cache.close()


def make_handler(proxy: CanopyCacheProxy):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):  # quiet; stats endpoint covers verification
            pass

        def do_HEAD(self):
            """Earth Platform's EarthStatus.reach() probes the configured
            proxy's health endpoint with a plain HTTP HEAD (MINECRAFT-571
            comments 55789/55808/55816) - status only, no body. Only
            /__health answers HEAD; anything else 404s rather than
            silently falling back to tile-serving semantics for a method
            no client actually uses against a tile path."""
            if self.path == "/__health":
                ok = proxy.check_health()
                self.send_response(200 if ok else 503)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self):
            import json

            if self.path == "/__stats":
                payload = json.dumps(proxy.stats.as_dict()).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            if self.path == "/__health":
                ok = proxy.check_health()
                payload = json.dumps({"upstream_ok": ok}).encode()
                self.send_response(200 if ok else 503)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            status, headers, body = proxy.serve_tile(
                self.path,
                self.headers.get("If-None-Match"),
                self.headers.get("Range"),
            )
            self.send_response(status)
            for key, value in headers.items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if body:
                self.wfile.write(body)

    return Handler


def run_server(db_path: str, port: int, upstream_base: str | None = None, max_bytes: int = 512 * 1024 * 1024):
    proxy = CanopyCacheProxy(db_path, upstream_base, max_bytes)
    server = ThreadingHTTPServer(("127.0.0.1", port), make_handler(proxy))
    return server, proxy


if __name__ == "__main__":
    port = int(os.environ.get("CANOPY_CACHE_PORT", "8074"))
    db = os.environ.get("CANOPY_CACHE_DB", "canopy-cache.sqlite3")
    server, proxy = run_server(db, port)
    print(f"canopy cache proxy on http://127.0.0.1:{port} -> upstream {proxy.upstream_base}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        proxy.close()
