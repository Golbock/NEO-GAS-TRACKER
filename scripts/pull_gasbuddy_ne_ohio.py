#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import csv, datetime as dt, json, time, sys, os
from pathlib import Path
from typing import List, Dict
import requests

GB_ENDPOINT = "https://www.gasbuddy.com/graphql"  # endpoint used by the site
FUEL_PRODUCT = 1     # 1 = Regular grade
MAX_AGE_MIN = 0      # freshest prices
ZIP_CSV = Path(__file__).parent / "zips_ne_oh.csv"
OUTFILE = Path(__file__).parents[1] / "public" / "stations.csv"

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/plain, */*",
    "User-Agent": os.getenv("GB_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                                              "Chrome/122 Safari/537.36"),
}
# (Optional) If you hit Cloudflare 403s, add a temporary cookie via repo Secret
CF_COOKIE = os.getenv("GB_CF_COOKIE")  # e.g., "cf_clearance=...."
if CF_COOKIE:
    HEADERS["Cookie"] = CF_COOKIE

QUERY = """
query LocationBySearchTerm($fuel: Int, $maxAge: Int, $search: String) {
  locationBySearchTerm(search: $search) {
    stations(fuel: $fuel, maxAge: $maxAge) {
      results {
        id
        name
        address { line1 locality region postalCode }
        prices {
          cash { price postedTime }
          credit { price postedTime }
        }
      }
    }
  }
}
"""

def load_zips() -> List[Dict[str,str]]:
    rows = []
    with open(ZIP_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r.get("zip") and r.get("county_name"):
                rows.append(r)
    return rows

def lowest_regular_price(station: dict) -> float | None:
    prices = station.get("prices") or {}
    c = prices.get("cash") or {}
    r = prices.get("credit") or {}
    vals = []
    if c.get("price"): vals.append(float(c["price"]))
    if r.get("price"): vals.append(float(r["price"]))
    return min(vals) if vals else None

def fetch_zip(z: str) -> List[Dict[str,str]]:
    payload = {
        "operationName": "LocationBySearchTerm",
        "variables": { "fuel": FUEL_PRODUCT, "maxAge": MAX_AGE_MIN, "search": z },
        "query": QUERY
    }
    # retry/backoff a few times
    for attempt in range(4):
        r = requests.post(GB_ENDPOINT, headers=HEADERS, data=json.dumps(payload), timeout=30)
        if r.status_code == 200:
            data = r.json()
            stations = (data.get("data", {})
                           .get("locationBySearchTerm", {})
                           .get("stations", {})
                           .get("results", [])) or []
            out = []
            for s in stations:
                price = lowest_regular_price(s)
                if price is None:
                    continue
                addr = s.get("address") or {}
                out.append({
                    "date": dt.date.today().strftime("%Y-%m-%d"),
                    "station": s.get("name") or "",
                    "county": "",  # filled later from ZIP table
                    "city": addr.get("locality") or "",
                    "zip": addr.get("postalCode") or z,
                    "price": f"{price:.3f}",
                })
            return out
        time.sleep(1 + attempt)  # basic backoff
    raise RuntimeError(f"Failed ZIP {z}: HTTP {r.status_code} {r.text[:200]}")

def main():
    zips = load_zips()
    all_rows = []
    zip_to_county = {r["zip"]: r["county_name"] for r in zips}
    for i, zr in enumerate(zips, 1):
        z = zr["zip"]
        try:
            rows = fetch_zip(z)
            for r in rows:
                r["county"] = zip_to_county.get(r["zip"], zr["county_name"])
            all_rows.extend(rows)
        except Exception as e:
            print(f"[WARN] {z}: {e}", file=sys.stderr)
        time.sleep(0.7)  # polite throttle

    all_rows.sort(key=lambda r: (r["date"], float(r["price"])))
    OUTFILE.parent.mkdir(parents=True, exist_ok=True)
    with OUTFILE.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["date","station","county","city","zip","price"])
        w.writeheader()
        w.writerows(all_rows)

    print(f"Wrote {OUTFILE} with {len(all_rows)} rows")

if __name__ == "__main__":
    main()
