import os
import sys
import json
import argparse
from typing import Dict, Any, List, Optional
import time
import csv
import requests

def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"Accept": "application/json"})
    return s

def build_headers() -> Dict[str, str]:
    public_key = os.getenv("REFTAB_PUBLIC_KEY", "")
    secret_key = os.getenv("REFTAB_SECRET_KEY", "")
    base = {"X-Public-Key": public_key, "X-Secret-Key": secret_key}
    extra = os.getenv("REFTAB_HEADERS", "")
    if extra:
        try:
            base.update(json.loads(extra))
        except Exception:
            pass
    return base

def join_url(base: str, path: str) -> str:
    return base.rstrip("/") + "/" + path.lstrip("/")

def get_paginated(session: requests.Session, base_url: str, path: str, params: Dict[str, Any], headers: Dict[str, str], limit: int, sleep_s: float) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    offset = 0
    while True:
        qp = dict(params or {})
        qp["limit"] = limit
        qp["offset"] = offset
        url = join_url(base_url, path)
        resp = session.get(url, params=qp, headers=headers, timeout=60)
        if resp.status_code >= 500:
            time.sleep(sleep_s)
            continue
        resp.raise_for_status()
        data = resp.json() or []
        if isinstance(data, dict) and "results" in data:
            batch = data.get("results") or []
        else:
            batch = data if isinstance(data, list) else []
        if not batch:
            break
        out.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
        if sleep_s > 0:
            time.sleep(sleep_s)
    return out

def fetch_assets(session: requests.Session, base_url: str, headers: Dict[str, str], limit: int, sleep_s: float, query: Optional[str]) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if query:
        params["q"] = query
    return get_paginated(session, base_url, "assets", params, headers, limit, sleep_s)

def fetch_locations(session: requests.Session, base_url: str, headers: Dict[str, str], limit: int, sleep_s: float, query: Optional[str]) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if query:
        params["q"] = query
    return get_paginated(session, base_url, "locations", params, headers, limit, sleep_s)

def write_csv(rows: List[Dict[str, Any]], path: str) -> None:
    if not rows:
        with open(path, "w", newline="", encoding="utf-8") as f:
            f.write("")
        return
    keys = sorted({k for r in rows for k in r.keys()})
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: json.dumps(v) if isinstance(v, (dict, list)) else v for k, v in r.items()})

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--resource", choices=["assets", "locations"], default="assets")
    p.add_argument("--query", default=None)
    p.add_argument("--limit", type=int, default=200)
    p.add_argument("--sleep", type=float, default=0.2)
    p.add_argument("--out", default=None)
    p.add_argument("--stdout", action="store_true")
    return p.parse_args()

def main() -> None:
    base_url = os.getenv("REFTAB_BASE_URL", "")
    if not base_url:
        print(json.dumps({"ok": False, "error": "Missing REFTAB_BASE_URL"}))
        sys.exit(1)
    headers = build_headers()
    if not headers.get("X-Public-Key") or not headers.get("X-Secret-Key"):
        print(json.dumps({"ok": False, "error": "Missing REFTAB_PUBLIC_KEY or REFTAB_SECRET_KEY"}))
        sys.exit(1)
    args = parse_args()
    s = build_session()
    try:
        if args.resource == "assets":
            rows = fetch_assets(s, base_url, headers, args.limit, args.sleep, args.query)
        else:
            rows = fetch_locations(s, base_url, headers, args.limit, args.sleep, args.query)
    except requests.HTTPError as e:
        print(json.dumps({"ok": False, "error": f"HTTP {e.response.status_code if e.response else ''}"}))
        sys.exit(2)
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}))
        sys.exit(2)
    if args.out:
        write_csv(rows, args.out)
    if args.stdout:
        print(json.dumps(rows, ensure_ascii=False))
    else:
        print(json.dumps({"ok": True, "resource": args.resource, "count": len(rows), "out": args.out or ""}))

if __name__ == "__main__":
    main()
