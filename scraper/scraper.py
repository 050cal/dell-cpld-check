import json, re, time, yaml
from pathlib import Path
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
MODELS = yaml.safe_load((ROOT/"scraper"/"models.yaml").read_text())["models"]

# Known driver detail URLs per model
CPLD_PAGES = {
    "R640":   "https://www.dell.com/support/home/en-us/drivers/driversdetails?driverid=9n4dh",
    "R740":   "https://www.dell.com/support/home/en-us/drivers/driversdetails?driverid=g65gh",
    "R740XD": "https://www.dell.com/support/home/en-us/drivers/driversdetails?driverid=g65gh",
    "R650":   "https://www.dell.com/support/home/en-us/drivers/driversdetails?driverid=fctdf",
    "R750":   "https://www.dell.com/support/home/en-us/drivers/driversdetails?driverid=fctdf",
    "C6525":  "https://www.dell.com/support/home/en-us/drivers/driversdetails?driverid=wv5d3",
    "PowerEdge T340":  "https://www.dell.com/support/home/en-us/drivers/driversdetails?driverid=r5kd4",
    "PowerEdge R450":  "https://www.dell.com/support/home/en-us/drivers/driversdetails?driverid=pryjm",
    "PowerEdge R350":  "https://www.dell.com/support/home/en-us/drivers/driversdetails?driverid=w1htn",
    "PowerEdge R440":    "https://www.dell.com/support/home/en-us/drivers/driversdetails?driverid=1gw62",
    "PowerEdge R550":    "https://www.dell.com/support/home/en-us/drivers/driversdetails?driverid=pryjm",
}


# NEW: overlay with auto-generated URLs (if present)
auto_map_path = ROOT / "scraper" / "cpld_pages.auto.yaml"
if auto_map_path.exists():
    try:
        auto_map = yaml.safe_load(auto_map_path.read_text(encoding="utf-8")) or {}
        if isinstance(auto_map, dict) and "CPLD_PAGES" in auto_map:
            CPLD_PAGES.update(auto_map["CPLD_PAGES"])
    except Exception:
        pass


HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DellCPLDChecker/1.0; +https://github.com/<you>/dell-cpld-check)"
}

def parse_version(html: str) -> str | None:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(separator="\n", strip=True)

    m = re.search(r"Version\s+([0-9]+(?:\.[0-9]+){1,2})", text, re.IGNORECASE)
    if m:
        return m.group(1)

    m2 = re.search(r"releaseVersion\"?:\s*\"([0-9]+(?:\.[0-9]+){1,2})", html, re.IGNORECASE)
    return m2.group(1) if m2 else None

def get_latest_for_model(model: str) -> dict:
    url = CPLD_PAGES.get(model)
    if not url:
        return {"model": model, "error": "no_cpld_url"}

    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    ver = parse_version(r.text)
    return {"model": model, "cpld": ver, "source": url}

# ... existing imports and setup ...

def main():
    out = {
        "generated_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "models": []
    }

    for m in MODELS:
        try:
            model_name = m["name"] if isinstance(m, dict) else m
            info = get_latest_for_model(model_name)
            info["model"] = model_name  # ensure model is a string in output
        except Exception as e:
            info = {"model": (m.get("name") if isinstance(m, dict) else m), "error": str(e)}
        out["models"].append(info)

    site = ROOT / "site"
    site.mkdir(parents=True, exist_ok=True)

    (site / "latest.json").write_text(
        json.dumps(out, indent=2),
        encoding="utf-8"
    )

if __name__ == "__main__":
    main()
