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
from typing import List, Dict, Optional, Set
from xml.etree import ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]

# ---- JSON endpoint Dell site uses on the Drivers page ----
API_BASE = "https://www.dell.com/support/driver/en-us/ips/api/driverlist/fetchdriversbyproduct"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DellCPLDResolver/1.0)",
    "Accept": "application/json",
    "X-Requested-With": "XMLHttpRequest",
}
DRIVERID_RE = re.compile(r"_([0-9A-Z]{5})_", re.IGNORECASE)

# ---- Enterprise (DUP) catalog that includes BIOS/Firmware/Drivers incl. CPLD ----
# Dell KB with catalog links confirms Enterprise Catalog is Catalog.gz (weekly refreshed). [1](https://www.dell.com/support/kbdoc/en-us/000132986/dell-emc-catalog-links-for-poweredge-servers)
CATALOG_URL = "https://downloads.dell.com/catalog/Catalog.gz"


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
    """
    Try Dell's internal JSON used by the Drivers page. We iterate a few oscodes and set lob=POWEREDGE.
    If any call returns CPLD entries, choose newest by Release/LUPD/Version.
    (This endpoint is commonly discovered via browser DevTools network tab.) [3](https://gist.github.com/davecoutts/3b5d79ce50c8214e8cf598c4016b609d)[4](https://www.reddit.com/r/PowerShell/comments/ripbco/i_am_trying_to_figure_out_where_i_would_look_up/)
    """
    oscodes = ["NAA", "W2022", "WT64A", "UBT20"]  # NAA=not-applicable, Windows Server 2022, Win10 x64, Ubuntu 20.x
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
            r.raise_for_status()
            payload = r.json()
            items = payload.get("DriverListData") or []
            cpld = [d for d in items if _is_cpld_json(d)]
            if not cpld:
                continue
            cpld.sort(key=lambda d: (_parse_date(d.get("ReleaseDate") or ""),
                                     _parse_date(d.get("LUPDDate") or ""),
                                     str(d.get("DellVer") or "")), reverse=True)
            latest = cpld[0]
            driverid = _extract_driverid_from_record(latest)
            if not driverid:
                continue
            return {
                "driverid": driverid,
                "url": f"https://www.dell.com/support/home/en-us/drivers/driversdetails?driverid={driverid.lower()}",
                "version": latest.get("DellVer") or latest.get("Version") or latest.get("releaseVersion"),
                "released": latest.get("ReleaseDate") or latest.get("LUPDDate"),
                "source": f"json:{oscode}",
                "raw": latest,
            }
        except Exception:
            # try next oscode
            continue
    return None


# ==============================
# Catalog fallback (official Enterprise/DUP catalog)
# ==============================
def _download_catalog_text() -> str:
    r = requests.get(CATALOG_URL, timeout=60)
    r.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as gz:
        raw = gz.read()
    # Try robust decode order; Enterprise catalog often is UTF-16LE
    for enc in ("utf-16le", "utf-16", "utf-8-sig", "utf-8"):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("utf-8", errors="replace")


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
    labels = {base, f"poweredge {base}", f"dell {base}", f"dell poweredge {base}", f"dell emc poweredge {base}"}
    return {_canon(x) for x in labels}


def _display_matches(name: str, display: str) -> bool:
    labset = _labels_for_name(name)
    disp = _canon(display)
    if disp in labset:
        return True
    base = _canon(name)
    return bool(re.search(rf"\b{re.escape(base)}\b", disp))


def _catalog_latest_cpld_for_model(xml_text: str, model_display: str) -> Optional[Dict]:
    root = ET.fromstring(xml_text)
    ns = {"d": root.tag.split('}')[0].strip('{')} if root.tag.startswith("{") else {}

    candidates: List[Dict] = []
    for sc in root.findall(".//SoftwareComponent", ns):
        if not _is_cpld_catalog(sc, ns):
            continue

        # Collect target models (Display text)
        models: List[str] = []
        for brand in sc.findall(".//TargetSystems/Brand", ns):
            for model in brand.findall(".//Model", ns):
                lbl = (model.findtext("./Display", namespaces=ns) or model.get("display") or "").strip()
                if lbl:
                    models.append(lbl)

        # Accept "R640", "PowerEdge R640", "Dell EMC PowerEdge R640", etc.
        if not any(_display_matches(model_display, lbl) for lbl in models):
            continue

        # Grab version/date and try to locate a filename or any attribute that reveals the DUP
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

        # Fallback: scan all descendant attributes; last resort serialize & regex
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

    if not candidates:
        return None

    candidates.sort(key=lambda x: (_parse_date(x["released"]), x["version"]), reverse=True)
    top = candidates[0]
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

    # Load models (fail-safe)
    try:
        models = _load_models_safely()
    except Exception as e:
        _write_overlay_and_report(overlay, details, warn=f"fatal_models_load: {e}")
        print(f"WARN: fatal_models_load: {e}")
        return

    unresolved: List[Dict[str, Optional[str]]] = []

    # Pass 1: JSON endpoint
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
                details.append({
                    "model": name, "productcode": productcode,
                    **{k: res[k] for k in ("driverid", "version", "released", "url", "source")}
                })
            else:
                details.append({"model": name, "productcode": productcode, "error": "no_cpld_from_json"})
                unresolved.append(entry)
        except Exception as e:
            details.append({"model": name, "productcode": productcode, "error": f"json_error: {e}"})
            unresolved.append(entry)
        time.sleep(0.4)

    # Pass 2: Catalog fallback
    if unresolved:
        xml_text = ""
        try:
            xml_text = _download_catalog_text()  # Enterprise catalog includes CPLD. [1](https://www.dell.com/support/kbdoc/en-us/000132986/dell-emc-catalog-links-for-poweredge-servers)
        except Exception as e:
            details.append({"warning": f"catalog_download_error: {e}"})

        if xml_text:
            for entry in unresolved:
                name = entry["name"]
                try:
                    res = _catalog_latest_cpld_for_model(xml_text, model_display=name)
                    if res:
                        overlay[name] = res["url"]
                        details.append({
                            "model": name, "productcode": entry.get("productcode"),
                            **{k: res[k] for k in ("driverid", "version", "released", "url", "source")}
                        })
                    else:
                        details.append({"model": name, "productcode": entry.get("productcode"),
                                        "error": "no_cpld_from_catalog"})
                except Exception as e:
                    details.append({"model": name, "productcode": entry.get("productcode"),
                                    "error": f"catalog_parse_error: {e}"})

    # Always write files; let the workflow guard decide to fail if empty
    _write_overlay_and_report(overlay, details,
                              warn=("Resolver found no CPLD URLs" if not overlay else None))


if __name__ == "__main__":
    main()
