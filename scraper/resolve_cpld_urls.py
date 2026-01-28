#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
import json
import yaml
import requests
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
MODELS_CFG = yaml.safe_load((ROOT / "scraper" / "models.yaml").read_text(encoding="utf-8"))
MODELS = MODELS_CFG["models"]

API_BASE = "https://www.dell.com/support/driver/en-us/ips/api/driverlist/fetchdriversbyproduct"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DellCPLDResolver/1.0)",
    "X-Requested-With": "XMLHttpRequest",
}
DRIVERID_RE = re.compile(r"_([0-9A-Z]{5})_", re.IGNORECASE)

def parse_date(s: str) -> datetime:
    s = (s or "").strip()
    for fmt in ("%d %b %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s.split("T")[0], fmt)
        except Exception:
            continue
    return datetime.min

def extract_driverid(rec: dict) -> str | None:
    # Try common fields (varies by product/era)
    for key in ("DriverId", "DriverID", "ReleaseID", "DellDriverId", "UniqueDownloadId"):
        val = rec.get(key)
        if isinstance(val, str) and len(val) == 5:
            return val.upper()
    # Else parse from filename or URL (…_9N4DH_…)
    ffi = rec.get("FileFrmtInfo") or rec.get("fileFrmtInfo") or {}
    candidates = [
        ffi.get("FileName") or ffi.get("fileName") or "",
        ffi.get("HttpFileLocation") or ffi.get("httpFileLocation") or "",
    ]
    for c in candidates:
        m = DRIVERID_RE.search(c or "")
        if m:
            return m.group(1).upper()
    return None

def is_cpld(rec: dict) -> bool:
    blob = " ".join(str(rec.get(k) or "") for k in ("DriverName", "Category", "ComponentType")).upper()
    return "CPLD" in blob

def latest_cpld(productcode: str, oscode: str = "NAA") -> dict | None:
    params = {"productcode": productcode, "oscode": oscode, "initialload": "true", "_": str(int(time.time()*1000))}
    r = requests.get(API_BASE, headers=HEADERS, params=params, timeout=45)
    r.raise_for_status()
    payload = r.json()
    items = payload.get("DriverListData") or []
    cpld = [d for d in items if is_cpld(d)]
    if not cpld:
        return None
    cpld.sort(key=lambda d: (parse_date(d.get("ReleaseDate") or ""),
                             parse_date(d.get("LUPDDate") or ""),
                             str(d.get("DellVer") or "")), reverse=True)
    rec = cpld[0]
    driverid = extract_driverid(rec)
    if not driverid:
        return None
    url = f"https://www.dell.com/support/home/en-us/drivers/driversdetails?driverid={driverid.lower()}"
    return {
        "driverid": driverid,
        "url": url,
        "version": rec.get("DellVer") or rec.get("Version") or rec.get("releaseVersion"),
        "released": rec.get("ReleaseDate") or rec.get("LUPDDate"),
        "raw": rec,
    }

def main():
    overlay = {}
    detail_rows = []
    for entry in MODELS:
        name = entry["name"] if isinstance(entry, dict) else str(entry)
        productcode = entry.get("productcode") if isinstance(entry, dict) else None
        if not productcode:
            detail_rows.append({"model": name, "error": "missing_productcode"})
            continue
        try:
            res = latest_cpld(productcode)
            if not res:
                detail_rows.append({"model": name, "productcode": productcode, "error": "no_cpld_found"})
                continue
            overlay[name] = res["url"]
            detail_rows.append({
                "model": name,
                "productcode": productcode,
                "driverid": res["driverid"],
                "version": res["version"],
                "released": res["released"],
                "url": res["url"],
            })
        except Exception as e:
            detail_rows.append({"model": name, "productcode": productcode, "error": str(e)})
        time.sleep(0.4)  # be polite

    out_dir = ROOT / "scraper"
    out_dir.mkdir(parents=True, exist_ok=True)

    # YAML overlay your program can import
    (out_dir / "cpld_pages.auto.yaml").write_text(
        yaml.safe_dump({"CPLD_PAGES": overlay}, sort_keys=True, allow_unicode=True),
        encoding="utf-8"
    )

    # Optional detailed JSON report
    (out_dir / "cpld_pages.report.json").write_text(
        json.dumps({"generated_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "models": detail_rows}, indent=2),
        encoding="utf-8"
    )

if __name__ == "__main__":
    main()
