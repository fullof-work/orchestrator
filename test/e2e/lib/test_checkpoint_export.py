"""Exercise the measurement client against the real public HTTP shape over UDS."""
import base64
from contextlib import contextmanager
import http.server
import importlib.util
import json
from pathlib import Path
import socketserver
import tempfile
import threading
import time
import unittest


SOURCE = Path(__file__).resolve().parents[2] / "perf/checkpoint_export.py"
spec = importlib.util.spec_from_file_location("checkpoint_export", SOURCE)
client = importlib.util.module_from_spec(spec)
spec.loader.exec_module(client)


def publication():
    root = "manifest://" + "a" * 64
    return {"result": "e2b-sbx-" + base64.urlsafe_b64encode(root.encode()).decode().rstrip("="),
            "sandboxRef": root, "removedRefs": []}


@contextmanager
def endpoint(status=200, body=None, trickle=False):
    requests = []
    payload = json.dumps(publication()).encode() if body is None else body

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            requests.append({"method": self.command, "path": self.path,
                             "key": self.headers.get("X-API-KEY"),
                             "body": json.loads(self.rfile.read(int(self.headers["Content-Length"])))})
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            try:
                if trickle:
                    for value in payload:
                        self.wfile.write(bytes([value]))
                        self.wfile.flush()
                        time.sleep(0.01)
                else:
                    self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *_):
            pass

    with tempfile.TemporaryDirectory(prefix="checkpoint-export-") as directory:
        path = str(Path(directory) / "s")
        with socketserver.UnixStreamServer(path, Handler) as server:
            server.timeout = 1
            thread = threading.Thread(target=server.handle_request)
            thread.start()
            try:
                yield path, requests
            finally:
                thread.join(2)
                if thread.is_alive():
                    raise AssertionError("UDS fixture did not finish")


class CheckpointExport(unittest.TestCase):
    def test_public_export_preserves_exact_request_and_validates_response(self):
        with endpoint() as (path, requests):
            result = client.export(path, "sandbox-1", 1, "fixture-secret")
        self.assertEqual(result, publication())
        self.assertEqual(requests, [{"method": "POST", "path": "/sandboxes/sandbox-1/export",
                                    "key": "fixture-secret", "body": {"toTemplate": True, "keepSource": True}}])

    def test_non_200_response_is_not_retried_or_echoed(self):
        with endpoint(503, b"fixture-secret server response") as (path, requests):
            with self.assertRaisesRegex(ValueError, "^export API returned HTTP 503$"):
                client.export(path, "sandbox-1", 1, "fixture-secret")
        self.assertEqual(len(requests), 1)

    def test_total_deadline_stops_a_trickling_body_without_retry(self):
        with endpoint(trickle=True) as (path, requests):
            started = time.monotonic()
            with self.assertRaises(TimeoutError):
                client.export(path, "sandbox-1", 0.08, "fixture-secret")
            self.assertLess(time.monotonic() - started, 0.5)
        self.assertEqual(len(requests), 1)

    def test_invalid_json_schema_roots_and_template_are_rejected(self):
        valid = publication()
        invalid = [b"{} {}", b'{"result":"x","result":"y"}', b"null",
                   json.dumps({**valid, "extra": True}).encode(),
                   json.dumps({**valid, "sandboxRef": "file://local.sandbox"}).encode(),
                   json.dumps({**valid, "result": "e2b-sbx-bad"}).encode(),
                   json.dumps({**valid, "removedRefs": None}).encode(),
                   json.dumps({**valid, "removedRefs": ["same", "same"]}).encode(),
                   json.dumps({**valid, "removedRefs": [valid["sandboxRef"]]}).encode()]
        for raw in invalid:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                client.validate_response(raw)

    def test_invalid_timeout_id_or_key_fails_before_connect(self):
        for timeout, sid, key in ((0, "id", "secret"), (-1, "id", "secret"),
                                  (float("nan"), "id", "secret"), (float("inf"), "id", "secret"),
                                  (1, "../other", "secret"), (1, "id", ""), (1, "id", "secret\n")):
            with self.subTest(timeout=timeout, sid=sid), self.assertRaises(ValueError):
                client.export("/does/not/exist", sid, timeout, key)


if __name__ == "__main__":
    unittest.main()
