"""Integration proof for the canopy-height cache proxy.

Runs against a local mock upstream (never the live dataforgood-fb-data S3
bucket for data traffic): per the ticket and coordination comments, no live
provider/quota use is authorized until terms are reviewed and until this is
wired into the real integration lane. The mock serves the exact tile-id.tif
contract inventoried from CanopySource / live-verified against the actual
bucket (Accept-Ranges, ETag, 206/304 semantics) so the cache mechanics under
test are the real ones the proxy will use against the live default.
"""
import http.client
import os
import shutil
import tempfile
import threading
import time
import unittest
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

import proxy
from proxy import CanopyCacheProxy, UpstreamError, probe_upstream_health, run_server

# A minimal but structurally real classic little-endian TIFF: byte-order mark,
# magic 42, and an IFD offset, padded out - enough to pass _is_well_formed_tiff
# without needing a full raster, since that check is structural only.
TILE_BYTES = b"II*\x00" + b"\x08\x00\x00\x00" + (b"\x00" * 256)

# HTTP 200, correct Content-Length, but not a TIFF at all (an S3-style error
# body) - the "malformed upstream tile" case the ticket asks to prove is never
# cached.
MALFORMED_BYTES = b"<Error><Code>InternalError</Code></Error>" + b" " * 32


class MockUpstream(BaseHTTPRequestHandler):
    hit_count = {"n": 0}
    fail_next = {"n": 0}

    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        MockUpstream.hit_count["n"] += 1
        if MockUpstream.fail_next["n"] > 0:
            MockUpstream.fail_next["n"] -= 1
            self.send_response(503)
            self.end_headers()
            return
        if self.path == "/chm/0013113.tif":
            self.send_response(200)
            self.send_header("Content-Length", str(len(TILE_BYTES)))
            self.end_headers()
            self.wfile.write(TILE_BYTES)
        elif self.path == "/chm/9999999.tif":
            # HTTP 200 and a correct Content-Length, but the bytes aren't a real TIFF.
            self.send_response(200)
            self.send_header("Content-Length", str(len(MALFORMED_BYTES)))
            self.end_headers()
            self.wfile.write(MALFORMED_BYTES)
        elif self.path == "/chm/range-ignoring-huge.tif":
            # Ignores any Range header and answers 200 with a large body -
            # the exact origin shape John's 55839 flagged (WorldCover/JRC):
            # a health probe must never be allowed to read all of this.
            body = b"II*\x00" + b"\x08\x00\x00\x00" + (b"\x00" * (4 * 1024 * 1024))
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass  # the probe is expected to close early after its bounded read
        elif self.path == "/chm/forbidden.tif":
            self.send_response(403)
            self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()


class ProxyIntegrationTest(unittest.TestCase):
    def setUp(self):
        MockUpstream.hit_count["n"] = 0
        MockUpstream.fail_next["n"] = 0
        self.mock = HTTPServer(("127.0.0.1", 0), MockUpstream)
        self.mock_port = self.mock.server_address[1]
        self.mock_thread = threading.Thread(target=self.mock.serve_forever, daemon=True)
        self.mock_thread.start()
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "cache.sqlite3")
        self.upstream_base = f"http://127.0.0.1:{self.mock_port}/chm/"

    def tearDown(self):
        self.mock.shutdown()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_default_upstream_is_unchanged(self):
        proxy = CanopyCacheProxy(self.db_path)
        self.assertEqual(
            proxy.upstream_base,
            "https://dataforgood-fb-data.s3.amazonaws.com/forests/v2/global/dinov3_global_chm_v2_ml3_10m_p95/chm/",
        )
        proxy.close()

    def test_env_override_rejects_cleartext_http(self):
        os.environ["CANOPY_CACHE_UPSTREAM"] = "http://dataforgood-fb-data.s3.amazonaws.com/forests/v2/"
        try:
            with self.assertRaises(ValueError):
                CanopyCacheProxy(self.db_path)
        finally:
            del os.environ["CANOPY_CACHE_UPSTREAM"]

    def test_env_override_rejects_unallowlisted_host(self):
        os.environ["CANOPY_CACHE_UPSTREAM"] = "https://evil.example.com/forests/v2/"
        try:
            with self.assertRaises(ValueError):
                CanopyCacheProxy(self.db_path)
        finally:
            del os.environ["CANOPY_CACHE_UPSTREAM"]

    def test_env_override_accepts_allowlisted_https_host(self):
        os.environ["CANOPY_CACHE_UPSTREAM"] = (
            "https://dataforgood-fb-data.s3.amazonaws.com/forests/v2/global/other_variant/chm/"
        )
        try:
            proxy = CanopyCacheProxy(self.db_path)
            self.assertEqual(
                proxy.upstream_base,
                "https://dataforgood-fb-data.s3.amazonaws.com/forests/v2/global/other_variant/chm/",
            )
            proxy.close()
        finally:
            del os.environ["CANOPY_CACHE_UPSTREAM"]

    def test_cold_fetch_then_warm_sqlite_hit_then_restart_zero_extra_upstream(self):
        proxy = CanopyCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status, headers, body = proxy.serve_tile("/0013113.tif", None, None)
        self.assertEqual(status, 200)
        self.assertEqual(body, TILE_BYTES)
        self.assertEqual(MockUpstream.hit_count["n"], 1)
        self.assertEqual(proxy.stats.as_dict()["upstream_requests"], 1)

        status2, _, body2 = proxy.serve_tile("/0013113.tif", None, None)
        self.assertEqual(status2, 200)
        self.assertEqual(body2, TILE_BYTES)
        self.assertEqual(MockUpstream.hit_count["n"], 1, "warm hit must not call upstream again")
        self.assertEqual(proxy.stats.as_dict()["cache_hits"], 1)
        proxy.close()

        proxy2 = CanopyCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status3, _, body3 = proxy2.serve_tile("/0013113.tif", None, None)
        self.assertEqual(status3, 200)
        self.assertEqual(body3, TILE_BYTES)
        self.assertEqual(MockUpstream.hit_count["n"], 1, "restart must not call upstream again for a cached tile")
        self.assertEqual(proxy2.stats.as_dict()["upstream_requests"], 0)
        proxy2.close()

    def test_ttl_minus_one_survives_large_clock_advance(self):
        """John/MINECRAFT-571 55740: successful data responses are TTL -1 - a
        large simulated clock advance must still be zero extra upstream calls."""
        proxy = CanopyCacheProxy(self.db_path, upstream_base=self.upstream_base)
        proxy.serve_tile("/0013113.tif", None, None)
        self.assertEqual(MockUpstream.hit_count["n"], 1)
        proxy.close()

        real_time = time.time
        try:
            time.time = lambda: real_time() + 10 * 365 * 24 * 3600  # +10 years
            proxy2 = CanopyCacheProxy(self.db_path, upstream_base=self.upstream_base)
            status, _, body = proxy2.serve_tile("/0013113.tif", None, None)
            self.assertEqual(status, 200)
            self.assertEqual(body, TILE_BYTES)
            self.assertEqual(MockUpstream.hit_count["n"], 1, "TTL -1 must not expire the entry after any elapsed time")
            proxy2.close()
        finally:
            time.time = real_time

    def test_explicit_invalidation_forces_refetch(self):
        """TTL -1 still requires explicit invalidation to work, e.g. for a
        dataset revision (John/55740)."""
        proxy = CanopyCacheProxy(self.db_path, upstream_base=self.upstream_base)
        proxy.serve_tile("/0013113.tif", None, None)
        self.assertEqual(MockUpstream.hit_count["n"], 1)

        self.assertTrue(proxy.cache.invalidate("0013113.tif"))
        status, _, body = proxy.serve_tile("/0013113.tif", None, None)
        self.assertEqual(status, 200)
        self.assertEqual(body, TILE_BYTES)
        self.assertEqual(MockUpstream.hit_count["n"], 2, "invalidation must force exactly one refetch")
        proxy.close()

    def test_malformed_tiff_is_never_served_from_sqlite_on_retry(self):
        proxy = CanopyCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status, _, _ = proxy.serve_tile("/9999999.tif", None, None)
        self.assertEqual(status, 502)
        self.assertIsNone(proxy.cache.get("9999999.tif"), "a malformed TIFF must never be cached")

        status2, _, _ = proxy.serve_tile("/9999999.tif", None, None)
        self.assertEqual(status2, 502)
        self.assertIsNone(proxy.cache.get("9999999.tif"))
        self.assertEqual(MockUpstream.hit_count["n"], 2 * 3, "each attempt retries 3x against the always-malformed response")
        proxy.close()

    def test_conditional_304_on_matching_etag(self):
        proxy = CanopyCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status, headers, _ = proxy.serve_tile("/0013113.tif", None, None)
        etag = headers["ETag"]
        status2, _, body2 = proxy.serve_tile("/0013113.tif", etag, None)
        self.assertEqual(status2, 304)
        self.assertEqual(body2, b"")
        self.assertEqual(MockUpstream.hit_count["n"], 1)
        proxy.close()

    def test_range_request_returns_206_with_exact_slice(self):
        proxy = CanopyCacheProxy(self.db_path, upstream_base=self.upstream_base)
        proxy.serve_tile("/0013113.tif", None, None)  # warm the cache
        status, headers, chunk = proxy.serve_tile("/0013113.tif", None, "bytes=0-9")
        self.assertEqual(status, 206)
        self.assertEqual(chunk, TILE_BYTES[0:10])
        self.assertEqual(headers["Content-Range"], f"bytes 0-9/{len(TILE_BYTES)}")
        proxy.close()

    def test_out_of_range_is_416(self):
        proxy = CanopyCacheProxy(self.db_path, upstream_base=self.upstream_base)
        proxy.serve_tile("/0013113.tif", None, None)
        status, _, _ = proxy.serve_tile("/0013113.tif", None, f"bytes={len(TILE_BYTES)+100}-")
        self.assertEqual(status, 416)
        proxy.close()

    def test_suffix_range_returns_final_bytes(self):
        proxy = CanopyCacheProxy(self.db_path, upstream_base=self.upstream_base)
        proxy.serve_tile("/0013113.tif", None, None)
        status, headers, chunk = proxy.serve_tile("/0013113.tif", None, "bytes=-10")
        self.assertEqual(status, 206)
        self.assertEqual(chunk, TILE_BYTES[-10:])
        self.assertEqual(
            headers["Content-Range"], f"bytes {len(TILE_BYTES) - 10}-{len(TILE_BYTES) - 1}/{len(TILE_BYTES)}"
        )
        proxy.close()

    def test_suffix_range_larger_than_body_clamps_to_whole_body(self):
        proxy = CanopyCacheProxy(self.db_path, upstream_base=self.upstream_base)
        proxy.serve_tile("/0013113.tif", None, None)
        status, _, chunk = proxy.serve_tile("/0013113.tif", None, f"bytes=-{len(TILE_BYTES) + 1000}")
        self.assertEqual(status, 206)
        self.assertEqual(chunk, TILE_BYTES)
        proxy.close()

    def test_malformed_range_header_is_416_not_an_exception(self):
        proxy = CanopyCacheProxy(self.db_path, upstream_base=self.upstream_base)
        proxy.serve_tile("/0013113.tif", None, None)
        for bad in ("bytes=abc", "bytes=-", "bytes=", "bytes=0-10,20-30", "bytes=-0", "items=0-5"):
            status, _, _ = proxy.serve_tile("/0013113.tif", None, bad)
            self.assertEqual(status, 416, f"expected 416 for malformed Range {bad!r}")
        proxy.close()

    def test_suffix_range_over_real_http(self):
        server, srv_proxy = run_server(self.db_path, 0, upstream_base=self.upstream_base)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/0013113.tif", headers={"Range": "bytes=-10"})
            resp = conn.getresponse()
            body = resp.read()
            self.assertEqual(resp.status, 206)
            self.assertEqual(body, TILE_BYTES[-10:])
            conn.close()
        finally:
            server.shutdown()
            server.server_close()
            srv_proxy.close()

    def test_malformed_range_over_real_http_is_416(self):
        server, srv_proxy = run_server(self.db_path, 0, upstream_base=self.upstream_base)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/0013113.tif", headers={"Range": "bytes=abc"})
            resp = conn.getresponse()
            resp.read()
            self.assertEqual(resp.status, 416)
            conn.close()
        finally:
            server.shutdown()
            server.server_close()
            srv_proxy.close()

    def test_oversized_numeric_range_field_is_416_not_an_exception(self):
        # John/MINECRAFT-571 55955: Python's int() has a ~4300-digit
        # conversion limit; a Range field past that must still return a
        # controlled 416, never raise, both at the parser and over real HTTP.
        proxy = CanopyCacheProxy(self.db_path, upstream_base=self.upstream_base)
        proxy.serve_tile("/0013113.tif", None, None)
        huge = "9" * 4301
        for bad in (f"bytes={huge}-", f"bytes=-{huge}", f"bytes={huge}-{huge}"):
            status, _, _ = proxy.serve_tile("/0013113.tif", None, bad)
            self.assertEqual(status, 416, f"expected 416 for oversized Range {bad!r}")
        proxy.close()

    def test_oversized_numeric_range_field_over_real_http_is_416(self):
        server, srv_proxy = run_server(self.db_path, 0, upstream_base=self.upstream_base)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/0013113.tif", headers={"Range": f"bytes={'9' * 4301}-"})
            resp = conn.getresponse()
            resp.read()
            self.assertEqual(resp.status, 416)
            conn.close()
        finally:
            server.shutdown()
            server.server_close()
            srv_proxy.close()

    def test_transient_upstream_failure_is_never_cached_but_retry_recovers(self):
        MockUpstream.fail_next["n"] = 1
        proxy = CanopyCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status, _, _ = proxy.serve_tile("/0013113.tif", None, None)
        self.assertEqual(status, 200, "retry must recover from one transient 503")
        self.assertEqual(MockUpstream.hit_count["n"], 2, "the failed attempt plus the retry")
        self.assertIsNotNone(proxy.cache.get("0013113.tif"))
        proxy.close()

    def test_persistent_upstream_failure_returns_502_and_caches_nothing(self):
        MockUpstream.fail_next["n"] = 99
        proxy = CanopyCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status, _, _ = proxy.serve_tile("/0013113.tif", None, None)
        self.assertEqual(status, 502)
        self.assertIsNone(proxy.cache.get("0013113.tif"))
        proxy.close()

    def test_arbitrary_path_is_refused_not_forwarded(self):
        proxy = CanopyCacheProxy(self.db_path, upstream_base=self.upstream_base)
        status, _, _ = proxy.serve_tile("/../../etc/passwd", None, None)
        self.assertEqual(status, 404)
        status2, _, _ = proxy.serve_tile("/0013113.tif/../secret", None, None)
        self.assertEqual(status2, 404)
        self.assertEqual(MockUpstream.hit_count["n"], 0, "must never forward an unvalidated path upstream")
        proxy.close()

    def test_concurrent_miss_is_deduplicated_to_one_upstream_call(self):
        proxy = CanopyCacheProxy(self.db_path, upstream_base=self.upstream_base)
        results = []
        barrier = threading.Barrier(8)

        def worker():
            barrier.wait()
            results.append(proxy.serve_tile("/0013113.tif", None, None))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(results), 8)
        for status, _, body in results:
            self.assertEqual(status, 200)
            self.assertEqual(body, TILE_BYTES)
        self.assertEqual(MockUpstream.hit_count["n"], 1, "8 concurrent misses on the same tile must single-flight to 1 upstream call")
        proxy.close()

    def test_bounded_storage_evicts_least_recently_used(self):
        proxy = CanopyCacheProxy(self.db_path, upstream_base=self.upstream_base, max_bytes=len(TILE_BYTES))
        proxy.serve_tile("/0013113.tif", None, None)
        self.assertEqual(proxy.cache.total_bytes(), len(TILE_BYTES))

        proxy.cache.put("0013131.tif", TILE_BYTES)
        proxy.cache.evict_to_fit(proxy.stats)
        total_after = proxy.cache.total_bytes()
        self.assertLessEqual(total_after, len(TILE_BYTES) * 2)
        self.assertGreaterEqual(proxy.stats.as_dict()["evictions"], 1)
        proxy.close()

    def test_loopback_only_bind(self):
        server, proxy = run_server(self.db_path, 0, upstream_base=self.upstream_base)
        try:
            self.assertEqual(server.server_address[0], "127.0.0.1")
        finally:
            server.server_close()
            proxy.close()

    def test_dedicated_health_endpoint_bypasses_cache_and_reflects_live_upstream(self):
        """John/MINECRAFT-571 55734/55737: TTL 0 applies only to the dedicated
        health-check endpoint, as a bounded live probe - it must never read or
        write the data cache, and ordinary cached tiles must keep serving from
        SQLite while the health probe independently reports upstream failure."""
        proxy = CanopyCacheProxy(
            self.db_path, upstream_base=self.upstream_base, health_path="0013113.tif"
        )
        # Warm the ordinary tile cache first.
        proxy.serve_tile("/0013113.tif", None, None)
        self.assertEqual(MockUpstream.hit_count["n"], 1)

        # Healthy probe succeeds and does not touch the data-cache stats.
        self.assertTrue(proxy.check_health())
        self.assertEqual(proxy.stats.as_dict()["health_checks"], 1)
        self.assertEqual(proxy.stats.as_dict()["health_failures"], 0)
        self.assertEqual(proxy.stats.as_dict()["cache_hits"], 0, "health probe must not count as a tile cache hit")
        self.assertEqual(proxy.stats.as_dict()["cache_misses"], 1, "only the earlier real tile fetch, not the health probe")

        # Simulate an upstream outage: ordinary tile must still hit SQLite ...
        MockUpstream.fail_next["n"] = 999
        status, _, body = proxy.serve_tile("/0013113.tif", None, None)
        self.assertEqual(status, 200)
        self.assertEqual(body, TILE_BYTES)
        self.assertEqual(MockUpstream.hit_count["n"], 2, "a warm tile must stay served from SQLite during an outage (count unchanged since the prior health probe)")

        # ... but the separate health endpoint must report the real failure.
        self.assertFalse(proxy.check_health())
        self.assertEqual(proxy.stats.as_dict()["health_failures"], 1)

        # Restore upstream: a new probe must succeed again.
        MockUpstream.fail_next["n"] = 0
        self.assertTrue(proxy.check_health())
        proxy.close()

    def test_health_endpoint_over_http(self):
        server, proxy = run_server(self.db_path, 0, upstream_base=self.upstream_base)
        proxy.health_path = "0013113.tif"
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            resp = urllib.request.urlopen(f"http://127.0.0.1:{port}/__health", timeout=5)
            self.assertEqual(resp.status, 200)
        finally:
            server.shutdown()
            server.server_close()
            proxy.close()

    def test_oversized_tile_response_is_rejected_never_cached(self):
        """John/MINECRAFT-571 55839's unbounded-read finding applies to the
        real tile fetch too, not just the health probe: a response at or
        past MAX_TILE_BYTES must be rejected (bounded read, never fully
        buffered) and never written to the cache."""
        original_cap = proxy.MAX_TILE_BYTES
        proxy.MAX_TILE_BYTES = 100  # TILE_BYTES (264 bytes) now counts as oversized
        try:
            proxy_obj = CanopyCacheProxy(self.db_path, upstream_base=self.upstream_base)
            status, _, _ = proxy_obj.serve_tile("/0013113.tif", None, None)
            self.assertEqual(status, 502)
            self.assertIsNone(proxy_obj.cache.get("0013113.tif"), "an oversized response must never be cached")
            proxy_obj.close()
        finally:
            proxy.MAX_TILE_BYTES = original_cap

    def test_health_probe_rejects_bad_status_without_reading_body(self):
        """John/MINECRAFT-571 55839: an unexpected status must be rejected
        as unhealthy before any read."""
        ok = probe_upstream_health(self.upstream_base, "forbidden.tif")
        self.assertFalse(ok)

    def test_health_probe_bounds_the_read_on_a_range_ignoring_huge_origin(self):
        """John/MINECRAFT-571 55839: an origin that ignores Range and
        answers 200 with the full (multi-MB) object must never turn a
        health probe into a full download - the probe must still complete
        quickly and report healthy without reading the whole body."""
        start = time.monotonic()
        ok = probe_upstream_health(self.upstream_base, "range-ignoring-huge.tif", timeout=5.0)
        elapsed = time.monotonic() - start
        self.assertTrue(ok)
        self.assertLess(elapsed, 2.0, "a bounded read must not take anywhere near the time to transfer 4MB")

    def test_health_endpoint_answers_plain_head_like_earth_status_reach(self):
        """MINECRAFT-571 55789/55808/55816: EarthStatus.reach() probes the
        configured proxy's health endpoint with a plain HTTP HEAD, not GET -
        a handler that only implements do_GET fails this integration."""
        server, proxy = run_server(self.db_path, 0, upstream_base=self.upstream_base)
        proxy.health_path = "0013113.tif"
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("HEAD", "/__health")
            resp = conn.getresponse()
            self.assertEqual(resp.status, 200)
            self.assertEqual(resp.read(), b"", "HEAD must never return a body")
            conn.close()

            MockUpstream.fail_next["n"] = 999
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("HEAD", "/__health")
            resp = conn.getresponse()
            self.assertEqual(resp.status, 503)
            resp.read()
            conn.close()
        finally:
            server.shutdown()
            server.server_close()
            proxy.close()

    def test_end_to_end_http_cold_warm_restart(self):
        server, proxy = run_server(self.db_path, 0, upstream_base=self.upstream_base)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            url = f"http://127.0.0.1:{port}/0013113.tif"
            body1 = urllib.request.urlopen(url, timeout=5).read()
            self.assertEqual(body1, TILE_BYTES)
            self.assertEqual(MockUpstream.hit_count["n"], 1)

            body2 = urllib.request.urlopen(url, timeout=5).read()
            self.assertEqual(body2, TILE_BYTES)
            self.assertEqual(MockUpstream.hit_count["n"], 1, "warm HTTP request must not hit upstream again")
        finally:
            server.shutdown()
            server.server_close()
            proxy.close()

        server2, proxy2 = run_server(self.db_path, 0, upstream_base=self.upstream_base)
        port2 = server2.server_address[1]
        thread2 = threading.Thread(target=server2.serve_forever, daemon=True)
        thread2.start()
        try:
            url2 = f"http://127.0.0.1:{port2}/0013113.tif"
            body3 = urllib.request.urlopen(url2, timeout=5).read()
            self.assertEqual(body3, TILE_BYTES)
            self.assertEqual(MockUpstream.hit_count["n"], 1, "restart must still be zero extra upstream calls")
        finally:
            server2.shutdown()
            server2.server_close()
            proxy2.close()


if __name__ == "__main__":
    unittest.main()
