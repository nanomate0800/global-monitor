#!/usr/bin/env python3
"""
server.py — Global Monitor local server
Usage: python server.py
Opens at http://localhost:8765
"""
import http.server, socketserver, webbrowser, os
from pathlib import Path

PORT = 8765
APP_DIR = Path(__file__).parent / 'app'  # serve app/ (index.html + static/data/)

class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(APP_DIR), **kwargs)
    def log_message(self, fmt, *args):
        if args[1] not in ('200', '304'):
            super().log_message(fmt, *args)

if __name__ == '__main__':
    with socketserver.TCPServer(('', PORT), Handler) as httpd:
        url = f'http://localhost:{PORT}'
        print(f'\n  Global Monitor running at {url}')
        print(f'  Press Ctrl+C to stop\n')
        webbrowser.open(url)
        httpd.serve_forever()