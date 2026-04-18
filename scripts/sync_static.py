"""Sync the freshly-built app to ./static/ for Streamlit Cloud deployment.

Run this after `python build_data.py`. It:
  1. Re-copies app/index.html into static/index.html with fetch-path rewrites
     (`static/data/...` -> `data/...`) so the app works under Streamlit's
     static-serving URL pattern.
  2. Mirrors app/static/data/ -> static/data/ (overwriting any stale copies).
  3. Reports the resulting size + file count.
"""
import os
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
APP_HTML = ROOT / 'app' / 'index.html'
APP_DATA = ROOT / 'app' / 'static' / 'data'
STATIC_DIR = ROOT / 'static'
STATIC_HTML = STATIC_DIR / 'index.html'
STATIC_DATA = STATIC_DIR / 'data'

def main():
    if not APP_HTML.exists():
        raise SystemExit(f"app/index.html missing: {APP_HTML}")
    if not APP_DATA.exists():
        raise SystemExit(f"app/static/data missing: {APP_DATA}")
    STATIC_DIR.mkdir(parents=True, exist_ok=True)

    # Copy HTML with fetch-path rewrites (static/data/ -> data/)
    html = APP_HTML.read_text(encoding='utf-8')
    html = html.replace("fetch('static/data/", "fetch('data/")
    html = html.replace("fetch(`static/data/", "fetch(`data/")
    STATIC_HTML.write_text(html, encoding='utf-8')
    print(f"[sync] static/index.html ({len(html):,} bytes)")

    # Mirror data directory
    if STATIC_DATA.exists():
        shutil.rmtree(STATIC_DATA)
    shutil.copytree(APP_DATA, STATIC_DATA)

    total = 0
    n = 0
    for root, _, files in os.walk(STATIC_DATA):
        for f in files:
            total += (Path(root) / f).stat().st_size
            n += 1
    print(f"[sync] static/data/  {total/1024/1024:.1f} MB across {n} files")

if __name__ == '__main__':
    main()
