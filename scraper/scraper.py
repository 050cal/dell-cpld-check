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

# ---------------- helpers: IO ----------------
def _write_overlay_and_report(overlay: dict, details: list, warn: str | None = None):
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
        json.dumps(report, indent=2), encoding="utf-8"
    )

# ---------------- existing functions (JSON + Catalog) ----------------
# … keep the _parse_date / _resolve_latest_cpld_json / _download_catalog_text /
#    _catalog_latest_cpld_for_model you already have (the latest versions we added)

def _parse_date(s: str) -> datetime:
    s = (s or "").strip()
    for fmt in ("%d %b %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s.split("T")[0], fmt)
        except Exception:
            pass
    return datetime.min

# (keep all the JSON and catalog functions you have from the previous message unchanged)
# _extract_driverid_from_record, _is_cpld_json, _resolve_latest_cpld_json,
# _download_catalog_text (with utf-16le/utf-16/utf-8-sig fallbacks),
# _is_cpld_catalog, _extract_driverid_from_filename,
# _canon, _labels_for_name, _display_matches, _catalog_latest_cpld_for_model
# --------------------------------------------------------------------

def _load_models_safely() -> list[dict]:
    """Load models.yaml safely; raise RuntimeError with a helpful message on any issue."""
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
    # normalize to dicts {name, productcode}
    norm = []
    for m in models:
        if isinstance(m, dict):
            name = m.get("name") or m.get("model")
            productcode = m.get("productcode")
        else:
            name = str(m)
            productcode = None
        norm.append({"name": name, "productcode": productcode})
    return norm

def main():
    overlay: dict[str, str] = {}
    details: list[dict] = []
    fatal_warn = None

    try:
        models = _load_models_safely()
    except Exception as e:
        fatal_warn = f"fatal_models_load: {e}"
        _write_overlay_and_report(overlay, details, warn=fatal_warn)
        print(f"WARN: {fatal_warn}")
        return  # do not raise; workflow will print files and fail later

    unresolved = []

    # ---- JSON pass ----
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

    # ---- Catalog fallback ----
    if unresolved:
        xml_text = ""
        try:
            xml_text = _download_catalog_text()
        except Exception as e:
            warn = f"catalog_download_error: {e}"
            details.append({"warning": warn})
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

    # ---- Always write files, even if overlay is empty ----
    _write_overlay_and_report(overlay, details,
                              warn=("Resolver found no CPLD URLs" if not overlay else None))

if __name__ == "__main__":
    main()
