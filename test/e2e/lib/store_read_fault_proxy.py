#!/usr/bin/env python3
"""Test-owned plaintext HTTP/2 proxy: close exact selected Manifest Get streams."""
import json
import pathlib
import signal
import socket
import sys
import threading
import time

port, target, control, evidence = sys.argv[1:]
stop = threading.Event()
lock = threading.Lock()
connections = set()
workers = []
receipt = {"count": 0, "first_ns": 0, "last_ns": 0}


def exact(conn, size):
    data = bytearray()
    while len(data) < size:
        part = conn.recv(size - len(data))
        if not part:
            raise EOFError()
        data.extend(part)
    return bytes(data)


def fault(payload):
    config = json.loads(pathlib.Path(control).read_text())
    if config["mode"] != "block":
        return False
    # GetRequest: partition=MANIFEST (field 1), 32-byte key (field 2).
    signature = b"\x08\x02\x12\x20" + bytes.fromhex(config["key"])
    if signature not in payload:
        return False
    with lock:
        now = time.monotonic_ns()
        receipt.update(count=receipt["count"] + 1, last_ns=now)
        if not receipt["first_ns"]:
            receipt["first_ns"] = now
        path = pathlib.Path(evidence)
        temporary = path.with_suffix(".next")
        temporary.write_text(json.dumps(receipt))
        temporary.replace(path)
    return True


def serve(client):
    backend = None
    reverse = None
    try:
        backend = socket.create_connection(("127.0.0.1", int(target)), timeout=10)
        backend.settimeout(None)
        with lock:
            connections.add(backend)

        def response():
            try:
                while data := backend.recv(65536):
                    client.sendall(data)
            except OSError:
                pass
            finally:
                try:
                    client.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

        reverse = threading.Thread(target=response)
        reverse.start()
        preface = exact(client, 24)
        if preface != b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n":
            raise ValueError("expected plaintext HTTP/2 preface")
        backend.sendall(preface)
        while not stop.is_set():
            header = exact(client, 9)
            size = int.from_bytes(header[:3], "big")
            if size > 1 << 20:
                raise ValueError("oversized test frame")
            payload = exact(client, size)
            if header[3] == 0 and fault(payload):
                return
            backend.sendall(header + payload)
    except (OSError, EOFError, ValueError):
        pass
    finally:
        for conn in (client, backend):
            if conn is not None:
                try:
                    conn.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                conn.close()
                with lock:
                    connections.discard(conn)
        if reverse is not None:
            reverse.join()


listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", int(port)))
listener.listen()
listener.settimeout(.2)
signal.signal(signal.SIGTERM, lambda *_: stop.set())
signal.signal(signal.SIGINT, lambda *_: stop.set())
try:
    while not stop.is_set():
        try:
            client, _ = listener.accept()
        except socket.timeout:
            continue
        with lock:
            connections.add(client)
        retained = []
        for worker in workers:
            if worker.is_alive():
                retained.append(worker)
            else:
                worker.join()
        worker = threading.Thread(target=serve, args=(client,))
        workers = retained + [worker]
        worker.start()
finally:
    listener.close()
    with lock:
        for conn in connections:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
    for worker in workers:
        worker.join()
