#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import gzip
import io
import os
import re
import time
import json
import yaml
import requests
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Set
from xml.etree import ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
LOG_PATH = ROOT / "scraper" / "resolver_debug.log"

# ---- JSON endpoint Dell's Drivers page uses (capturable via DevTools) ----
API_BASE = "https://www.dell.com/support/driver/en-us/ips/api/driverlist/fetchdriversbyproduct"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DellCPLDResolver/1.0)",
    "Accept": "application/json",
    "X-Requested-With": "XMLHttpRequest",
}
DRIVERID_RE = re.compile(r"_([0-9A-Z]{5})_", re.IGNORECASE)

# ---- Enterprise (DUP) catalog (official PowerEdge update catalog) ----
CATALOG_URL = "https://downloads.dell.com/catalog/Catalog.gz"  # Enterprise catalog (weekly, includes CPLD)  # [3](https://www.dell.com/support/kbdoc/en-us/000132986/dell-emc-catalog-links-for-poweredge-servers)


# ==============================
# Logging helpers
# ==============================
def log(*args):
    msg = " ".join(str(a) for a in args)
    print(msg, flush=True)
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except Exception:
        pass


# ==============================
# I/O helpers
# ==============================
def _write_overlay_and_report(overlay: Dict[str, str], details: List[Dict], warn: Optional[str] = None) -> None:
    out_dir = ROOT / "scraper"
    out_dir.mkdir(parents=True, exist_ok=True)

    (out_dir / "cpld_pages.auto.yaml").write_text(
        yaml.safe_dump({"CPLD_PAGES": overlay or {}}, sort_keys=True, allow_unicode=True),
        encoding="utf-8"
    )
    report = {
        "generated_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "warning": warn,
        "models": details or [],
    }
    (out_dir / "cpld_pages.report.json").write_text(
        json.dumps(report, indent=2),
        encoding="utf-8"
    )
    log("WROTE overlay + report:", (out_dir / "cpld_pages.auto.yaml"), (out_dir / "cpld_pages.report.json"))


# ==============================
# Shared utilities
# ==============================
def _parse_date(s: str) -> datetime:
    s = (s or "").strip()
    for fmt in ("%d %b %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s.split("T")[0], fmt)
        except Exception:
            pass
    return datetime.min


# ==============================
# JSON path (preferred when available)
# ==============================
def _extract_driverid_from_record(rec: Dict) -> Optional[str]:
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


def _is_cpld_json(rec: Dict) -> bool:
    blob = " ".join(str(rec.get(k) or "") for k in ("DriverName", "Category", "ComponentType")).upper()
    return "CPLD" in blob


def _resolve_latest_cpld_json(productcode: str) -> Optional[Dict]:
    oscodes = ["NAA", "W2022", "WT64A", "UBT20"]  # not-applicable, WS2022, Win10 x64, Ubuntu 20.x
    for oscode in oscodes:
        params = {
            "productcode": productcode,
            "oscode": oscode,
            "lob": "POWEREDGE",
            "initialload": "true",
            "_": str(int(time.time() * 1000)),
        }
        try:
            r = requests.get(API_BASE, headers=HEADERS, params=params, timeout=45)
            log(f"JSON call product={productcode} oscode={oscode} status={r.status_code} bytes={len(r.content)}")
            r.raise_for_status()
            payload = r.json()
            items = payload.get("DriverListData") or []
            cpld = [d for d in items if _is_cpld_json(d)]
            log(f"JSON items={len(items)} CPLD={len(cpld)} (product={productcode}, os={oscode})")
            if not cpld:
                continue
            cpld.sort(key=lambda d: (_parse_date(d.get("ReleaseDate") or ""),
                                     _parse_date(d.get("LUPDDate") or ""),
                                     str(d.get("DellVer") or "")), reverse=True)
            latest = cpld[0]
            driverid = _extract_driverid_from_record(latest)
            log("JSON latest CPLD:", latest.get("DriverName"), "ver=", latest.get("DellVer"), "driverid=", driverid)
            if not driverid:
                continue
            return {
                "driverid": driverid,
                "url": f"https://www.dell.com/support/home/en-us/drivers/driversdetails?driverid={driverid.lower()}",
                "version": latest.get("DellVer") or latest.get("Version") or latest.get("releaseVersion"),
                "released": latest.get("ReleaseDate") or latest.get("LUPDDate"),
                "source": f"json:{oscode}",
                "raw": {"DriverName": latest.get("DriverName"), "DellVer": latest.get("DellVer")},
            }
        except Exception as e:
            log("JSON error:", repr(e))
            continue
    return None


# ==============================
# Catalog fallback (official Enterprise/DUP catalog)
# ==============================
def _download_catalog_text() -> str:
    r = requests.get(CATALOG_URL, timeout=60)
    log("CAT fetch status:", r.status_code, "bytes:", len(r.content))
    r.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as gz:
        raw = gz.read()
    for enc in ("utf-16le", "utf-16", "utf-8-sig", "utf-8"):
        try:
            t = raw.decode(enc)
            log("CAT decoded as:", enc, "chars:", len(t))
            return t
        except Exception:
            continue
    t = raw.decode("utf-8", errors="replace")
    log("CAT decoded as: utf-8 (replace) chars:", len(t))
    return t


def _is_cpld_catalog(sc: ET.Element, ns: Dict) -> bool:
    parts = [
        (sc.findtext(".//Display", namespaces=ns) or ""),
        (sc.findtext(".//Category", namespaces=ns) or ""),
        (sc.findtext(".//ComponentType", namespaces=ns) or ""),
    ]
    return "CPLD" in " ".join(parts).upper()


def _extract_driverid_from_filename(name: str) -> Optional[str]:
    m = DRIVERID_RE.search(name or "")
    return m.group(1).upper() if m else None


def _canon(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())


def _labels_for_name(name: str) -> Set[str]:
    base = name.strip()
    labels = {
        base,
        f"poweredge {base}",
        f"dell {base}",
        f"dell poweredge {base}",
        f"dell emc poweredge {base}",
    }
    return {_canon(x) for x in labels}


def _display_matches(name: str, display: str) -> bool:
    labset = _labels_for_name(name)
    disp = _canon(display)
    if disp in labset:
        return True
    base = _canon(name)
    return bool(re.search(rf"\b{re.escape(base)}\b", disp))


def _collect_models_from_component(sc: ET.Element, ns: Dict) -> List[str]:
    labels: List[str] = []
    for brand in sc.findall(".//SupportedSystems/Brand", ns):
        for model in brand.findall(".//Model", ns):
            lbl = (model.findtext("./Display", namespaces=ns) or model.get("display") or "").strip()
            if lbl:
                labels.append(lbl)
    for brand in sc.findall(".//TargetSystems/Brand", ns):
        for model in brand.findall(".//Model", ns):
            lbl = (model.findtext("./Display", namespaces=ns) or model.get("display") or "").strip()
            if lbl:
                labels.append(lbl)
    return labels


def _catalog_latest_cpld_for_model(xml_text: str, model_display: str) -> Optional[Dict]:
    root = ET.fromstring(xml_text)
    ns = {"d": root.tag.split('}')[0].strip('{')} if root.tag.startswith("{") else {}

    candidates: List[Dict] = []
    components = root.findall(".//SoftwareComponent", ns)
    log("CAT total SoftwareComponent nodes:", len(components))

    for sc in components:
        if not _is_cpld_catalog(sc, ns):
            continue

        models = _collect_models_from_component(sc, ns)
        if not models or not any(_display_matches(model_display, lbl) for lbl in models):
            continue

        name = (sc.findtext(".//Display", ns) or sc.findtext(".//Name", ns) or "").strip()
        version = (sc.findtext(".//DellVersion", ns) or "").strip()
        rdate = (sc.findtext(".//ReleaseDate", ns) or "").strip()

        filename = ""
        for tag in ("Path", "PackagePath", "Location", "FileName"):
            el = sc.find(f".//{tag}", ns)
            if el is None:
                continue
            val = (el.text or "").strip()
            if not val:
                for a in ("path", "Path", "filename", "FileName", "href", "src", "file"):
                    if el.get(a):
                        val = el.get(a).strip()
                        break
            if val:
                filename = val.split("/")[-1]
                break

        if not filename:
            for el in sc.iter():
                for a in ("path", "Path", "href", "src", "file", "filename", "FileName"):
                    if el.get(a):
                        val = el.get(a).strip()
                        if val:
                            filename = val.split("/")[-1]
                            break
                if filename:
                    break

        if filename:
            did = _extract_driverid_from_filename(filename)
        else:
            sc_xml = ET.tostring(sc, encoding="unicode", method="xml")
            m_any = DRIVERID_RE.search(sc_xml or "")
            did = m_any.group(1).upper() if m_any else None

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

    log("CAT CPLD candidates matched to model", model_display, ":", len(candidates))
    if not candidates:
        return None

    candidates.sort(key=lambda x: (_parse_date(x["released"]), x["version"]), reverse=True)
    top = candidates[0]
    log("CAT chose:", top.get("name"), "ver=", top.get("version"), "driverid=", top.get("driverid"))
    return {
        "driverid": top["driverid"],
        "url": f"https://www.dell.com/support/home/en-us/drivers/driversdetails?driverid={top['driverid'].lower()}",
        "version": top["version"],
        "released": top["released"],
        "source": "catalog",
        "raw": top,
    }


# ==============================
# Models loader (safe) + main
# ==============================
def _load_models_safely() -> List[Dict[str, Optional[str]]]:
    path = ROOT / "scraper" / "models.yaml"
    if not path.exists():
        raise RuntimeError(f"models.yaml not found at {path}")
    try:
        cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise RuntimeError(f"models.yaml parse error: {e}")
    models = cfg.get("models")
    if not isinstance(models, list) or not models:
        raise RuntimeError("models.yaml has no 'models' list or is empty")

    norm: List[Dict[str, Optional[str]]] = []
    for m in models:
        if isinstance(m, dict):
            name = m.get("name") or m.get("model")
            productcode = m.get("productcode")
        else:
            name = str(m)
            productcode = None
        norm.append({"name": name, "productcode": productcode})
    return norm


def main() -> None:
    overlay: Dict[str, str] = {}
    details: List[Dict] = []

    log("=== Resolver start ===")
    log("CWD:", Path.cwd())
    log("ROOT:", ROOT)
    log("Expect models at:", ROOT / "scraper" / "models.yaml")

    # Load models (fail-safe)
    try:
        models = _load_models_safely()
        log("Loaded models:", models)
    except Exception as e:
        warn = f"fatal_models_load: {e}"
        log("ERROR:", warn)
        _write_overlay_and_report(overlay, details, warn=warn)
        return

    unresolved: List[Dict[str, Optional[str]]] = []

    # Pass 1: JSON endpoint
    for entry in models:
        name, productcode = entry["name"], entry["productcode"]
        if not name:
            continue
        if not productcode:
            msg = {"model": name, "error": "missing_productcode"}
            log("MODEL ERROR:", msg)
            details.append(msg)
            unresolved.append(entry)
            continue
        try:
            res = _resolve_latest_cpld_json(productcode)
            if res:
                overlay[name] = res["url"]
                row = {"model": name, "productcode": productcode,
                       **{k: res[k] for k in ("driverid", "version", "released", "url", "source")}}
                log("JSON RESOLVED:", row)
                details.append(row)
            else:
                msg = {"model": name, "productcode": productcode, "error": "no_cpld_from_json"}
                log("JSON MISS:", msg)
                details.append(msg)
                unresolved.append(entry)
        except Exception as e:
            msg = {"model": name, "productcode": productcode, "error": f"json_error: {e}"}
            log("JSON EXCEPTION:", msg)
            details.append(msg)
            unresolved.append(entry)
        time.sleep(0.4)

    # Pass 2: Catalog fallback
    if unresolved:
        xml_text = ""
        try:
            xml_text = _download_catalog_text()
        except Exception as e:
            warn = f"catalog_download_error: {e}"
            log("ERROR:", warn)
            details.append({"warning": warn})

        if xml_text:
            for entry in unresolved:
                name = entry["name"]
                try:
                    res = _catalog_latest_cpld_for_model(xml_text, model_display=name)
                    if res:
                        overlay[name] = res["url"]
                        row = {"model": name, "productcode": entry.get("productcode"),
                               **{k: res[k] for k in ("driverid", "version", "released", "url", "source")}}
                        log("CAT RESOLVED:", row)
                        details.append(row)
                    else:
                        msg = {"model": name, "productcode": entry.get("productcode"),
                               "error": "no_cpld_from_catalog"}
                        log("CAT MISS:", msg)
                        details.append(msg)
                except Exception as e:
                    msg = {"model": name, "productcode": entry.get("productcode"),
                           "error": f"catalog_parse_error: {e}"}
                    log("CAT EXCEPTION:", msg)
                    details.append(msg)

    # -------- Optional safety switch to unblock testing ONLY (off by default) --------
    # Set env ALLOW_STATIC_FALLBACK=1 to allow one known-good mapping (R640 -> 9N4DH) if overlay is empty
    if not overlay and os.environ.get("ALLOW_STATIC_FALLBACK") == "1":
        log("STATIC FALLBACK ENABLED by env; adding R640 -> 9N4DH for testing")
        overlay["R640"] = "https://www.dell.com/support/home/en-us/drivers/driversdetails?driverid=9n4dh"  # [4](https://www.dell.com/support/home/en-us/drivers/driversdetails?driverid=9n4dh)
        details.append({"model": "R640", "productcode": "poweredge-r640",
                        "driverid": "9N4DH", "version": "(unknown)", "released": "(unknown)",
                        "url": overlay["R640"], "source": "static_fallback"})

    # Always write files; let the workflow guard fail later if empty
    _write_overlay_and_report(overlay, details, warn=("Resolver found no CPLD URLs" if not overlay else None))
    log("=== Resolver end (overlay count:", len(overlay), ") ===")


if __name__ == "__main__":
    main()
