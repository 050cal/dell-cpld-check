#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import gzip
import io
import re
import time
import json
import yaml
import requests
from pathlib import Path
from datetime import datetime
from xml.etree import ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
MODELS_CFG = yaml.safe_load((ROOT / "scraper" / "models.yaml").read_text(encoding="utf-8"))
MODELS = MODELS_CFG["models"]

# ---- JSON endpoint Dell site uses ----
API_BASE = "https://www.dell.com/support/driver/en-us/ips/api/driverlist/fetchdriversbyproduct"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DellCPLDResolver/1.0)",
    "Accept": "application/json",
    "X-Requested-With": "XMLHttpRequest",
}
DRIVERID_RE = re.compile(r"_([0-9A-Z]{5})_", re.IGNORECASE)

# ---- Enterprise (DUP) catalog that includes CPLD ----
CATALOG_URL = "https://downloads.dell.com/catalog/Catalog.gz"  # Enterprise catalog
# Dell’s official KB references this family of catalogs for PowerEdge updates. [1](https://www.dell.com/support/kbdoc/en-us/000132986/dell-emc-catalog-links-for-poweredge-servers)

def _parse_date(s: str) -> datetime:
    s = (s or "").strip()
    for fmt in ("%d %b %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s.split("T")[0], fmt)
        except Exception:
            pass
    return datetime.min

def _extract_driverid_from_record(rec: dict) -> str | None:
    for key in ("DriverId", "DriverID", "ReleaseID", "DellDriverId", "UniqueDownloadId"):
        val = rec.get(key)
        if isinstance(val, str) and len(val) == 5:
            return val.upper()
    ffi = rec.get("FileFrmtInfo") or rec.get("fileFrmtInfo") or {}
    for c in (ffi.get("FileName") or "", ffi.get("HttpFileLocation") or ""):
        m = DRIVERID_RE.search(c or "")
        if m:
            return m.group(1).upper()
    return None

def _is_cpld_json(rec: dict) -> bool:
    blob = " ".join(str(rec.get(k) or "") for k in ("DriverName", "Category", "ComponentType")).upper()
    return "CPLD" in blob

def _resolve_latest_cpld_json(productcode: str, oscode: str = "NAA") -> dict | None:
    params = {"productcode": productcode, "oscode": oscode, "initialload": "true", "_": str(int(time.time()*1000))}
    r = requests.get(API_BASE, headers=HEADERS, params=params, timeout=45)
    r.raise_for_status()
    payload = r.json()
    items = payload.get("DriverListData") or []
    cpld = [d for d in items if _is_cpld_json(d)]
    if not cpld:
        return None
    cpld.sort(key=lambda d: (_parse_date(d.get("ReleaseDate") or ""),
                             _parse_date(d.get("LUPDDate") or ""),
                             str(d.get("DellVer") or "")), reverse=True)
    latest = cpld[0]
    driverid = _extract_driverid_from_record(latest)
    if not driverid:
        return None
    return {
        "driverid": driverid,
        "url": f"https://www.dell.com/support/home/en-us/drivers/driversdetails?driverid={driverid.lower()}",
        "version": latest.get("DellVer") or latest.get("Version") or latest.get("releaseVersion"),
        "released": latest.get("ReleaseDate") or latest.get("LUPDDate"),
        "source": "json",
        "raw": latest,
    }

# ---------------- Catalog fallback ----------------

def _download_catalog_text() -> str:
    r = requests.get(CATALOG_URL, timeout=60)
    r.raise_for_status()
    # Catalog.gz contains UTF-16 XML (Dell publishes Enterprise catalog this way)
    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as gz:
        raw = gz.read()
    try:
        return raw.decode("utf-16")
    except UnicodeError:
        return raw.decode("utf-8", errors="replace")

def _is_cpld_catalog(sc: ET.Element, ns: dict) -> bool:
    parts = [
        (sc.findtext(".//Display", namespaces=ns) or ""),
        (sc.findtext(".//Category", namespaces=ns) or ""),
        (sc.findtext(".//ComponentType", namespaces=ns) or ""),
    ]
    return "CPLD" in " ".join(parts).upper()

def _extract_driverid_from_filename(name: str) -> str | None:
    m = DRIVERID_RE.search(name or "")
    return m.group(1).upper() if m else None

def _catalog_latest_cpld_for_model(xml_text: str, model_display: str) -> dict | None:
    root = ET.fromstring(xml_text)
    ns = {"d": root.tag.split('}')[0].strip('{')} if root.tag.startswith("{") else {}

    # Find all CPLD SoftwareComponents that target this model by Display text match
    candidates = []
    for sc in root.findall(".//SoftwareComponent", ns):
        if not _is_cpld_catalog(sc, ns):
            continue

        # collect models this component applies to (Display labels)
        models = []
        for brand in sc.findall(".//TargetSystems/Brand", ns):
            for model in brand.findall(".//Model", ns):
                lbl = (model.findtext("./Display", namespaces=ns) or model.get("display") or "").strip()
                if lbl:
                    models.append(lbl)

        if model_display not in set(models):
            continue

        # gather version/date and filename
        name = (sc.findtext(".//Display", ns) or sc.findtext(".//Name", ns) or "").strip()
        version = (sc.findtext(".//DellVersion", ns) or "").strip()
        rdate = (sc.findtext(".//ReleaseDate", ns) or "").strip()

        # try common path nodes to get filename
        filename = ""
        for tag in ("Path", "PackagePath", "Location"):
            el = sc.find(f".//{tag}", ns)
            if el is None:
                continue
            val = (el.text or "").strip()
            if not val and (el.get("path") or el.get("Path")):
                val = (el.get("path") or el.get("Path")).strip()
            if val:
                filename = val.split("/")[-1]
                break

        did = _extract_driverid_from_filename(filename)
        if not did:
            continue

        candidates.append({
            "name": name,
            "version": version,
            "released": rdate,
            "driverid": did,
            "filename": filename,
            "models": models,
        })

    if not candidates:
        return None

    # newest by ReleaseDate then Version string
    def skey(x):
        return (_parse_date(x["released"]), x["version"])
    candidates.sort(key=skey, reverse=True)
    top = candidates[0]
    return {
        "driverid": top["driverid"],
        "url": f"https://www.dell.com/support/home/en-us/drivers/driversdetails?driverid={top['driverid'].lower()}",
        "version": top["version"],
        "released": top["released"],
        "source": "catalog",
        "raw": top,
    }

def main():
    # Load models and normalize to list of dicts with {name, productcode}
    models = []
    for m in MODELS:
        if isinstance(m, dict):
            name = m.get("name") or m.get("model")
            productcode = m.get("productcode")
        else:
            name = str(m)
            productcode = None
        models.append({"name": name, "productcode": productcode})

    overlay = {}
    details = []
    unresolved = []

    # First pass: JSON endpoint
    for entry in models:
        name, productcode = entry["name"], entry["productcode"]
        if not name:
            continue
        if not productcode:
            details.append({"model": name, "error": "missing_productcode"})
            unresolved.append(entry)
            continue
        try:
            res = _resolve_latest_cpld_json(productcode)
            if res:
                overlay[name] = res["url"]
                details.append({"model": name, "productcode": productcode, **{k: res[k] for k in ("driverid","version","released","url","source")}})
            else:
                details.append({"model": name, "productcode": productcode, "error": "no_cpld_from_json"})
                unresolved.append(entry)
        except Exception as e:
            details.append({"model": name, "productcode": productcode, "error": f"json_error: {e}"})
            unresolved.append(entry)
        time.sleep(0.4)

    # If anything unresolved, fall back to Enterprise catalog
    if unresolved:
        try:
            xml_text = _download_catalog_text()  # Enterprise catalog (includes CPLD) [1](https://www.dell.com/support/kbdoc/en-us/000132986/dell-emc-catalog-links-for-poweredge-servers)
        except Exception as e:
            for entry in unresolved:
                details.append({"model": entry["name"], "productcode": entry.get("productcode"), "error": f"catalog_download_error: {e}"})
            xml_text = ""

        if xml_text:
            for entry in unresolved:
                name = entry["name"]
                try:
                    res = _catalog_latest_cpld_for_model(xml_text, model_display=name)
                    if res:
                        overlay[name] = res["url"]
                        details.append({"model": name, "productcode": entry.get("productcode"), **{k: res[k] for k in ("driverid","version","released","url","source")}})
                    else:
                        details.append({"model": name, "productcode": entry.get("productcode"), "error": "no_cpld_from_catalog"})
                except Exception as e:
                    details.append({"model": name, "productcode": entry.get("productcode"), "error": f"catalog_parse_error: {e}"})

    # Write overlay + report
    out_dir = ROOT / "scraper"
    out_dir.mkdir(parents=True, exist_ok=True)

    (out_dir / "cpld_pages.auto.yaml").write_text(
        yaml.safe_dump({"CPLD_PAGES": overlay}, sort_keys=True, allow_unicode=True),
        encoding="utf-8"
    )
    (out_dir / "cpld_pages.report.json").write_text(
        json.dumps({"generated_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "models": details}, indent=2),
        encoding="utf-8"
    )

    # Fail clearly if truly nothing found
    if not overlay:
        raise SystemExit("Resolver found no CPLD URLs. Check models.yaml productcode slugs, network access, or catalog parsing.")

if __name__ == "__main__":
    main()
