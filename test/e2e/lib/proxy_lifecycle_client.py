#!/usr/bin/env python3
"""Observe real Proxy images, service actions and unretried Guest business."""
import base64
import hashlib
import http.client
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

import websocket_probe as ws


def digest(path):
    with open(path, 'rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


class UnixHTTP(http.client.HTTPConnection):
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.host)


class Probe:
    def __init__(self, config):
        self.config = config
        self.stats = config['stats']
        self.pid = self.main_pid()
        self.initial_pid = self.pid
        self.stop = threading.Event()
        self.errors = []
        self.requests = 0
        self.peaks = dict(processes=0, rss_kib=0, fds=0)
        self.out = open(config['output'], 'x', buffering=1)
        self.output_lock = threading.Lock()
        self.cgroup = Path('/sys/fs/cgroup' + subprocess.check_output(
            ['systemctl', 'show', '--value', '-p', 'ControlGroup', config['unit']], text=True).strip())
        self.load = threading.Thread(target=self.traffic)
        self.monitor = threading.Thread(target=self.resources)

    def log(self, **fields):
        with self.output_lock:
            self.out.write(json.dumps(dict(time=time.time(), **fields)) + '\n')

    def main_pid(self):
        return int(subprocess.check_output(['systemctl', 'show', '--value', '-p', 'MainPID', self.config['unit']], text=True))

    def management(self, path='/status', method='GET', endpoint=None):
        connection = UnixHTTP(endpoint or self.stats, timeout=2)
        try:
            connection.request(method, path)
            response = connection.getresponse()
            body = response.read()
            return response.status, dict(response.getheaders()), body
        finally:
            connection.close()

    def wait_status(self, predicate, endpoint=None, timeout=35):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            try:
                code, _, body = self.management(endpoint=endpoint)
                assert code == 200
                last = json.loads(body)
                assert len(last['instances']) <= 2, last
                assert last['coordinator_pid'] == self.pid, last
                self.log(kind='status', status=last)
                if predicate(last):
                    return last
            except (OSError, http.client.HTTPException):
                pass  # Only management may have the documented exec gap.
            time.sleep(.02)
        raise AssertionError(f'lifecycle state timeout: {last}')

    def check_ready(self):
        code, headers, body = self.management('/readyz')
        status = json.loads(body)
        assert code == 200 and status['ready'] and status['pid'] == self.pid, status
        assert headers['Cache-Control'] == 'no-store'
        assert headers['Content-Type'] == 'application/json'
        assert status['workers'] == {'ready': 2, 'expected': 2}
        for path in ('/readyz', '/status'):
            assert self.management(path, 'POST')[0] == 405

    def connection(self, port=8002):
        connection = http.client.HTTPConnection('127.0.0.1', self.config['port'], timeout=30)
        headers = {'Host': f"{port}-{self.config['sid']}.{self.config['domain']}",
                   'X-Access-Token': self.config['token'], 'Connection': 'close'}
        return connection, headers

    def get(self, path):
        connection, headers = self.connection()
        try:
            connection.request('GET', path, headers=headers)
            response = connection.getresponse()
            return response.status, response.read()
        finally:
            connection.close()

    def traffic(self):
        while not self.stop.is_set():
            started = time.time()
            try:
                code, body = self.get('/health')
                assert code == 200 and body == b'healthy\n', (code, body)
                self.requests += 1
                self.log(kind='business', start=started, status=code)
            except Exception as error:
                self.errors.append(str(error))
                self.log(kind='business', start=started, error=str(error))
            self.stop.wait(.005)

    def resources(self):
        while not self.stop.is_set():
            try:
                pids = (self.cgroup / 'cgroup.procs').read_text().split()
            except FileNotFoundError:
                pids = []
            rss, fds = 0, 0
            for pid in pids:
                try:
                    rss += int(next(line.split()[1] for line in Path('/proc', pid, 'status').read_text().splitlines() if line.startswith('VmRSS:')))
                    fds += len(list(Path('/proc', pid, 'fd').iterdir()))
                except (FileNotFoundError, ProcessLookupError, StopIteration):
                    pass
            for name, value in dict(processes=len(pids), rss_kib=rss, fds=fds).items():
                self.peaks[name] = max(value, self.peaks[name])
            self.log(kind='resources', pids=pids, rss_kib=rss, fds=fds)
            self.stop.wait(.02)

    def finite(self, key):
        # One Guest serves every entry/source combination. Never reuse a released
        # request key from a previous mode or accepted operation.
        key = f'{self.initial_pid}-{time.monotonic_ns()}-{key}'
        connection, headers = self.connection()
        connection.request('GET', '/finite?id=' + key, headers=headers)
        response = connection.getresponse()
        assert response.status == 200 and response.read(6) == b'begin\n'

        def complete():
            assert self.get('/release?id=' + key)[0] == 204
            body = response.read()
            connection.close()
            assert body == b'mmds-ok\ncomplete\n', body
            self.log(kind='finite_complete', request=key, mmds=True)
        return complete

    def websocket(self):
        connection = socket.create_connection(('127.0.0.1', self.config['port']), timeout=15)
        key = base64.b64encode(os.urandom(16)).decode()
        request = (f"GET /ws HTTP/1.1\r\nHost: 8001-{self.config['sid']}.{self.config['domain']}\r\n"
                   f"X-Access-Token: {self.config['token']}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n"
                   f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: test\r\n\r\n")
        connection.sendall(request.encode())
        reader = connection.makefile('rb')
        assert reader.readline().split()[1] == b'101'
        http.client.parse_headers(reader)
        assert ws.read_frame(reader) == (1, b'ready')
        assert ws.read_frame(reader) == (2, b'\x00\xff\x7f')

        def complete():
            connection.sendall(ws.frame(9, b'during-drain', True))
            assert ws.read_frame(reader) == (10, b'during-drain')
            connection.sendall(ws.frame(8, b'\x03\xe8', True))
            connection.shutdown(socket.SHUT_WR)
            assert ws.read_frame(reader) == (8, b'\x03\xe8') and reader.read() == b''
            reader.close()
            connection.close()
            self.log(kind='websocket_complete', half_close=True)
        return complete

    def tunnel(self):
        connection = socket.create_connection(('127.0.0.1', self.config['port']), timeout=15)
        request = (f"CONNECT guest HTTP/1.1\r\nHost: guest\r\nE2b-Sandbox-Id: {self.config['sid']}\r\n"
                   f"E2b-Sandbox-Port: 8002\r\nX-Access-Token: {self.config['token']}\r\n\r\n")
        connection.sendall(request.encode())
        response = http.client.HTTPResponse(connection)
        response.begin()
        assert response.status == 200
        response.close()

        def complete():
            connection.sendall(b'GET /health HTTP/1.1\r\nHost: guest\r\nConnection: close\r\n\r\n')
            response = http.client.HTTPResponse(connection)
            response.begin()
            assert response.status == 200 and response.read() == b'healthy\n'
            response.close()
            connection.close()
            self.log(kind='connect_complete')
        return complete

    def trigger(self, index):
        if self.config['entry'] == 'app' and index == 0:
            code, _, body = self.management('/test/restart', 'POST')
            assert code == 200, (code, body)
        else:
            os.kill(self.pid, signal.SIGUSR1 if self.config['entry'] == 'app' else signal.SIGUSR2)

    def check_aggregate(self):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            connection = http.client.HTTPConnection('127.0.0.1', int(self.config['api_port']), timeout=2)
            try:
                connection.request('GET', f"/sandboxes/{self.config['sid']}/stats/traffic",
                                   headers={'Host': 'api.' + self.config['domain'], 'X-API-KEY': self.config['api_key']})
                response = connection.getresponse()
                body = json.loads(response.read())
                if response.status == 200 and body['inflight']['connected'] >= 6:
                    assert 'idleSince' not in body
                    self.log(kind='both_instances_busy', traffic=body)
                    return
            finally:
                connection.close()
            time.sleep(.02)
        raise AssertionError('conductor did not count both instances with three held requests each')

    def deploy(self, source):
        destination = Path(self.config['deployment'])
        temporary = destination.with_suffix('.next')
        shutil.copyfile(source, temporary)
        temporary.chmod(0o700)
        temporary.replace(destination)

    def operation(self, index):
        config_path = Path(self.config['proxy_config'])
        original_config = config_path.read_bytes()
        self.deploy(self.config['target'])
        target_digest = digest(self.config['target'])
        completions_a = [self.finite(f'{index}-a'), self.websocket(), self.tunnel()]
        previous = self.wait_status(lambda s: s['ready'])['inplace_restart']
        self.trigger(index)
        prepared = self.wait_status(lambda s: s['inplace_restart'] and
            (not previous or s['inplace_restart']['id'] != previous['id']) and
            s['inplace_restart']['phase'] == 'draining_a' and s['workers']['ready'] == 0)
        operation = prepared['inplace_restart']['id']
        candidate = prepared['instances'][1]
        assert candidate['ready'] and candidate['pid'] != self.pid
        assert digest(f"/proc/{candidate['pid']}/exe") == target_digest
        assert self.management('/readyz')[0] == 503
        completions_b = [self.finite(f'{index}-b'), self.websocket(), self.tunnel()]
        self.check_aggregate()
        if index == 0:
            # An accepted target/input must survive later deployment changes.
            self.deploy(self.config['initial'])
            config_path.write_text('invalid: [unclosed\n')
        elif index == 1:
            Path(self.config['deployment']).chmod(0o600)  # Real execve EACCES.
        else:
            Path(self.stats).with_name('fail-successor').touch(exist_ok=False)
        for complete in reversed(completions_a):
            complete()
        if index == 1:
            recovery = self.wait_status(lambda s: s['inplace_restart']['id'] == operation and
                s['inplace_restart']['attempts'] >= 4 and s['inplace_restart'].get('next_attempt'), candidate['stats_socket'])
            assert recovery['ready'] and recovery['service_ready']
            assert 'permission denied' in recovery['inplace_restart']['last_error']
            Path(self.config['deployment']).chmod(0o700)
        elif index == 2:
            recovery = self.wait_status(lambda s: s['inplace_restart']['id'] == operation and
                s['inplace_restart']['attempts'] >= 4 and s['inplace_restart'].get('next_attempt'), candidate['stats_socket'])
            assert recovery['ready'] and recovery['service_ready']
            assert 'test-only successor initialization failure' in recovery['inplace_restart']['last_error']
            Path(self.stats).with_name('fail-successor').unlink()
        successor = self.wait_status(lambda s: s['ready'] and s['inplace_restart']['id'] == operation and
            s['inplace_restart']['phase'] == 'draining_b')
        assert successor['pid'] == self.initial_pid == self.main_pid()
        assert digest(f'/proc/{self.pid}/exe') == target_digest
        for complete in reversed(completions_b):
            complete()
        result = self.wait_status(lambda s: s['inplace_restart']['id'] == operation and s['inplace_restart'].get('result'))
        assert result['inplace_restart']['result'] == 'completed', result
        assert result['inplace_restart']['forced_close_count'] == 0 and len(result['instances']) == 1
        assert not Path(f"/proc/{candidate['pid']}").exists(), 'B not reaped'
        assert not list(Path(self.stats).parent.glob('.proxy-*')), 'B resources remain'
        config_path.write_bytes(original_config)
        self.deploy(self.config['target'])
        self.check_ready()
        self.log(kind='operation_complete', id=operation, target_sha256=target_digest,
                 main_pid=self.pid, result=result['inplace_restart'])

    def run(self):
        self.wait_status(lambda s: s['ready'])
        self.check_ready()
        self.log(kind='start', entry=self.config['entry'], source=self.config['source'],
                 initial_sha256=digest(f'/proc/{self.pid}/exe'), target_sha256=digest(self.config['target']))
        assert digest(f'/proc/{self.pid}/exe') != digest(self.config['target'])
        self.load.start()
        self.monitor.start()
        try:
            for index in range(3 if self.config['entry'] == 'app' else 2):
                self.operation(index)
            if self.config['source'] in ('datafd', 'bothfd'):
                previous = self.pid
                subprocess.run(['systemctl', 'restart', self.config['unit']], check=True)
                self.pid = self.main_pid()
                assert self.pid != previous
                self.wait_status(lambda s: s['ready'])
                self.check_ready()
                self.log(kind='ordinary_service_restart', previous=previous, current=self.pid)
        finally:
            self.stop.set()
            self.load.join()
            self.monitor.join()
            self.log(kind='business_summary', requests=self.requests, failures=self.errors, peaks=self.peaks)
            self.out.close()
        assert self.requests > 0 and not self.errors, self.errors
        assert self.peaks['processes'] <= 6, self.peaks


if __name__ == '__main__':
    Probe(json.loads(Path(sys.argv[1]).read_text())).run()
