"""Loopback Responses tool-name gateway for Qwen + Codex namespace MCP tools."""
from __future__ import annotations

import http.client
import http.server
import json
import threading
from urllib.parse import urlparse


class ToolNameGateway:
    def __init__(self, listen_port: int, upstream_port: int, timeout: float = 3600.0):
        self.listen_port = int(listen_port)
        self.upstream_port = int(upstream_port)
        self.timeout = timeout
        self._names: dict[str, tuple[str, str]] = {}
        self._server = None
        self._thread = None

    def _rewrite_value(self, value):
        if isinstance(value, dict):
            kind = value.get('type')
            if kind == 'function_call' and isinstance(value.get('name'), str):
                mapped = self._names.get(value['name'])
                if mapped:
                    value['namespace'], value['name'] = mapped
            elif kind == 'response.function_call_arguments.done' and isinstance(value.get('name'), str):
                mapped = self._names.get(value['name'])
                if mapped:
                    value['name'] = mapped[1]
            for item in value.values():
                self._rewrite_value(item)
        elif isinstance(value, list):
            for item in value:
                self._rewrite_value(item)

    def _flatten_request(self, payload):
        tools = payload.get('tools')
        if not isinstance(tools, list):
            return payload
        flattened = []
        for spec in tools:
            if not isinstance(spec, dict) or spec.get('type') != 'namespace':
                flattened.append(spec)
                continue
            namespace = spec.get('name')
            if not isinstance(namespace, str) or not namespace:
                continue
            for child in spec.get('tools') or []:
                if not isinstance(child, dict) or child.get('type') != 'function':
                    continue
                child_name = child.get('name')
                if not isinstance(child_name, str) or not child_name:
                    continue
                flat_name = f'{namespace}__{child_name}'
                converted = {k: v for k, v in child.items() if k != 'type'}
                converted['type'] = 'function'
                converted['name'] = flat_name
                flattened.append(converted)
                self._names[flat_name] = (namespace, child_name)
        payload['tools'] = flattened
        return payload

    def _transform_response(self, payload):
        self._rewrite_value(payload)
        return payload

    def start(self):
        gateway = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, fmt, *args):
                return

            def _upstream_headers(self):
                skip = {'host', 'content-length', 'connection', 'transfer-encoding'}
                return {k: v for k, v in self.headers.items() if k.lower() not in skip}

            def _send_simple(self, status, body, content_type='application/json'):
                if isinstance(body, str):
                    body = body.encode()
                self.send_response(status)
                self.send_header('Content-Type', content_type)
                self.send_header('Content-Length', str(len(body)))
                self.send_header('Connection', 'close')
                self.end_headers()
                if self.command != 'HEAD':
                    self.wfile.write(body)

            def _write_chunk(self, data):
                if not data:
                    return
                self.wfile.write(f'{len(data):X}\r\n'.encode('ascii'))
                self.wfile.write(data)
                self.wfile.write(b'\r\n')
                self.wfile.flush()

            def _stream_sse(self, response):
                self.send_response(response.status)
                for key, value in response.getheaders():
                    if key.lower() in {'content-length', 'transfer-encoding', 'connection'}:
                        continue
                    self.send_header(key, value)
                self.send_header('Transfer-Encoding', 'chunked')
                self.send_header('Connection', 'close')
                self.end_headers()
                try:
                    for raw in response:
                        line = raw
                        if line.startswith(b'data:'):
                            payload = line[5:].strip()
                            if payload and payload != b'[DONE]':
                                try:
                                    event = json.loads(payload)
                                except ValueError:
                                    event = None
                                if event is not None:
                                    event = gateway._transform_response(event)
                                    line = b'data: ' + json.dumps(event, ensure_ascii=False, separators=(',', ':')).encode('utf-8') + b'\n'
                        self._write_chunk(line)
                    self.wfile.write(bytes([48, 13, 10, 13, 10]))
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    return

            def _handle(self):
                parsed = urlparse(self.path)
                path = parsed.path
                length = int(self.headers.get('Content-Length') or 0)
                body = self.rfile.read(length) if length else b''
                if self.command == 'POST' and path == '/v1/responses' and body:
                    try:
                        payload = json.loads(body)
                    except ValueError:
                        payload = None
                    if isinstance(payload, dict):
                        payload = gateway._flatten_request(payload)
                        body = json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
                conn = http.client.HTTPConnection('127.0.0.1', gateway.upstream_port, timeout=gateway.timeout)
                try:
                    headers = self._upstream_headers()
                    headers['Host'] = f'127.0.0.1:{gateway.upstream_port}'
                    conn.request(self.command, self.path, body=body, headers=headers)
                    response = conn.getresponse()
                    content_type = response.getheader('Content-Type', '')
                    if self.command == 'POST' and path == '/v1/responses' and 'text/event-stream' in content_type.lower():
                        self._stream_sse(response)
                        return
                    data = response.read()
                    if self.command == 'POST' and path == '/v1/responses' and data:
                        try:
                            payload = json.loads(data)
                        except ValueError:
                            payload = None
                        if isinstance(payload, dict):
                            payload = gateway._transform_response(payload)
                            data = json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
                    self.send_response(response.status)
                    for key, value in response.getheaders():
                        if key.lower() in {'content-length', 'transfer-encoding', 'connection'}:
                            continue
                        self.send_header(key, value)
                    self.send_header('Content-Length', str(len(data)))
                    self.send_header('Connection', 'close')
                    self.end_headers()
                    if self.command != 'HEAD':
                        self.wfile.write(data)
                except Exception as exc:
                    self._send_simple(502, json.dumps({'error': {'message': f'tool gateway upstream error: {exc}'}}))
                finally:
                    conn.close()

            def do_GET(self):
                self._handle()

            def do_POST(self):
                self._handle()

            def do_HEAD(self):
                self._handle()

        self._server = http.server.ThreadingHTTPServer(('127.0.0.1', self.listen_port), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, name='exa-tool-gateway', daemon=True)
        self._thread.start()

    def stop(self):
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
