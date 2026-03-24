"""
scrape_iari.py
==============
Scrapes daily weather observations, 5-day IMD forecast, and monthly
rainfall totals from the IARI BMS daily-weather page.

Outputs
-------
data/iari_weather.csv     – append-only observation log (sorted by date)
data/forecast.json        – rolling 5-day IMD multimodel forecast
data/monthly_rainfall.csv – full monthly/annual rainfall table

Usage
-----
    python scraper/scrape_iari.py [--dry-run] [--verbose]

Run automatically via GitHub Actions (.github/workflows/scrape_weather.yml).
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Constants ──────────────────────────────────────────────────────────────────

URL              = "https://www.iari.res.in/bms/daily-weather/"
DATA_DIR         = Path(__file__).resolve().parent.parent / "data"
OBS_CSV          = DATA_DIR / "iari_weather.csv"
FORECAST_JSON    = DATA_DIR / "forecast.json"
RAINFALL_CSV     = DATA_DIR / "monthly_rainfall.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; IARI-weather-scraper/1.0; "
        "+https://github.com/your-username/iari-weather-dashboard)"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

OBS_COLUMNS = [
    "date", "tmax", "tmin", "rain", "wind",
    "wdir1", "wdir2", "cond1", "cond2",
    "rh1", "rh2", "bss", "evap",
]

MONTH_COLS = [
    "year",
    "jan", "feb", "mar", "apr", "may", "jun",
    "jul", "aug", "sep", "oct", "nov", "dec",
    "total",
]

TIMEOUT = 30  # seconds

log = logging.getLogger("iari-scraper")


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_float(value, default=None):
    """Safely convert a cell string to float, stripping IARI-specific artefacts."""
    if value is None:
        return default
    cleaned = str(value).strip().replace("*", "").replace("\xa0", "").replace(",", "")
    if cleaned in ("", "-", "—", "N/A"):
        return default
    try:
        return float(cleaned)
    except ValueError:
        return default


def safe_text(tag):
    """Return stripped text from a BS4 tag, or empty string if tag is None."""
    return tag.get_text(strip=True) if tag else ""


# ── Fetch ─────────────────────────────────────────────────────────────────────

def fetch_page():
    """Download the IARI daily-weather page and return a BeautifulSoup object."""
    log.info("Fetching %s", URL)
    try:
        resp = requests.get(URL, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        log.info("HTTP %s  —  %.1f kB received", resp.status_code, len(resp.content) / 1024)
        return BeautifulSoup(resp.text, "lxml")
    except requests.RequestException as exc:
        log.error("Fetch failed: %s", exc)
        raise


# ── Parsers ───────────────────────────────────────────────────────────────────

_DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")


def _find_obs_table(soup):
    """
    Locate the daily observation table using three independent strategies,
    tried in order. Returns the matching BS4 tag or None.

    Strategy A — date-format scan (primary, most reliable):
        Walk every table's body rows. If any row has ≥13 <td> cells and the
        first cell matches DD/MM/YYYY, this is the observation table.
        Works regardless of header structure, language, colspan, or rowspan.

    Strategy B — keyword scan across full table text (fallback):
        Look for a table whose combined text contains 'date' AND ('temp' OR
        'rainfall'), while excluding the forecast table (whose first row
        contains ISO-format dates like 2026-03-24).

    Strategy C — widest table (last resort):
        The observation table always has the most columns (13) on the IARI
        page. If nothing else matched, pick the widest table with ≥13 cols.
    """
    tables = soup.find_all("table")

    # ── A: date-format cell in body rows ──────────────────────────────────
    for tbl in tables:
        for row in tbl.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 13 and _DATE_RE.match(cells[0].get_text(strip=True)):
                log.debug("Obs table found via strategy A (date-format)")
                return tbl

    # ── B: keyword scan, excluding forecast table ──────────────────────────
    for tbl in tables:
        text = tbl.get_text(" ").lower()
        if "date" in text and ("temp" in text or "rainfall" in text):
            first_row_cells = [
                c.get_text(strip=True)
                for c in (tbl.find("tr") or []).find_all("td")
            ] if tbl.find("tr") else []
            is_forecast = any(
                re.match(r"\d{4}-\d{2}-\d{2}", c) for c in first_row_cells
            )
            if not is_forecast:
                log.debug("Obs table found via strategy B (keyword scan)")
                return tbl

    # ── C: widest table with ≥13 columns ──────────────────────────────────
    widest, max_cols = None, 0
    for tbl in tables:
        for row in tbl.find_all("tr"):
            n = len(row.find_all("td"))
            if n > max_cols:
                max_cols = n
                widest = tbl
    if widest and max_cols >= 13:
        log.debug("Obs table found via strategy C (widest table, %d cols)", max_cols)
        return widest

    return None


def parse_obs_table(soup):
    """
    Parse the 10-day daily observation table.

    Uses _find_obs_table() which tries three detection strategies so the
    scraper remains robust against IARI page layout changes (header text,
    colspan/rowspan, Hindi mixed headers, missing header row, etc.).

    Returns a list of dicts with keys matching OBS_COLUMNS.
    """
    obs_table = _find_obs_table(soup)

    if obs_table is None:
        raise ValueError(
            "Observation table not found — all three detection strategies failed. "
            "The page structure may have changed significantly."
        )

    records = []
    for row in obs_table.find_all("tr"):
        cells = [safe_text(c) for c in row.find_all("td")]
        if len(cells) < 13:
            continue

        # Only process rows whose first cell is a DD/MM/YYYY date
        # (skips any header rows automatically, regardless of structure)
        if not _DATE_RE.match(cells[0].strip()):
            continue

        try:
            dt = datetime.strptime(cells[0].strip(), "%d/%m/%Y")
            date_iso = dt.strftime("%Y-%m-%d")
        except ValueError:
            log.debug("Skipping row with unparseable date: %r", cells[0])
            continue

        records.append({
            "date":  date_iso,
            "tmax":  parse_float(cells[1]),
            "tmin":  parse_float(cells[2]),
            "rain":  parse_float(cells[3], default=0.0),
            "wind":  parse_float(cells[4]),
            "wdir1": cells[5],
            "wdir2": cells[6],
            "cond1": parse_float(cells[7]),
            "cond2": parse_float(cells[8]),
            "rh1":   parse_float(cells[9]),
            "rh2":   parse_float(cells[10]),
            "bss":   parse_float(cells[11]),
            "evap":  parse_float(cells[12]),
        })

    log.info("Parsed %d observation rows", len(records))
    return records


def parse_forecast_table(soup):
    """
    Parse the 5-day IMD multimodel ensemble forecast table.

    The table is row-oriented: each row is one weather parameter and each
    column (after the first) is a forecast date.  We pivot into a list of
    per-day dicts and capture the advisory string.

    Returns a dict:
        {
          "generated":      "2026-03-23T02:30:00Z",
          "advisory":       "Generally cloudy sky…",
          "weekly_rain_mm": "2.0 mm",
          "days": [ {date, tmax, tmin, rain, cloud, rh_max, rh_min, wind, wind_dir}, … ]
        }
    or None if the table cannot be found.
    """
    fc_table = None
    for tbl in soup.find_all("table"):
        text = tbl.get_text()
        if "Maximum Temperature" in text and "Wind" in text and "Rainfall" in text:
            fc_table = tbl
            break

    if fc_table is None:
        log.warning("Forecast table not found — skipping forecast output")
        return None

    rows = fc_table.find_all("tr")
    if not rows:
        return None

    # Header row: first cell = parameter label, rest = forecast dates
    header_cells = [safe_text(c) for c in rows[0].find_all("td")]
    dates = header_cells[1:]

    param_map   = {}
    advisory    = ""
    weekly_rain = None

    for row in rows[1:]:
        cells = [safe_text(c) for c in row.find_all("td")]
        if not cells:
            continue
        label = cells[0].lower()
        vals  = cells[1:]

        if "rainfall" in label and "weekly" not in label and "cumulative" not in label:
            param_map["rain"]     = vals
        elif "maximum temperature" in label:
            param_map["tmax"]     = vals
        elif "minimum temperature" in label:
            param_map["tmin"]     = vals
        elif "cloud" in label:
            param_map["cloud"]    = vals
        elif "maximum rh" in label:
            param_map["rh_max"]   = vals
        elif "minimum rh" in label:
            param_map["rh_min"]   = vals
        elif "wind speed" in label:
            param_map["wind"]     = vals
        elif "wind direction" in label:
            param_map["wind_dir"] = vals
        elif "special" in label:
            advisory = cells[1] if len(cells) > 1 else ""
        elif "weekly" in label or "cumulative" in label:
            weekly_rain = cells[1] if len(cells) > 1 else None

    def _get(key, i):
        lst = param_map.get(key, [])
        return lst[i] if i < len(lst) else ""

    days = []
    for i, date in enumerate(dates):
        if not date or not re.match(r"\d{4}-\d{2}-\d{2}", date):
            continue
        days.append({
            "date":     date,
            "tmax":     parse_float(_get("tmax",     i)),
            "tmin":     parse_float(_get("tmin",     i)),
            "rain":     parse_float(_get("rain",     i), default=0.0),
            "cloud":    parse_float(_get("cloud",    i)),
            "rh_max":   parse_float(_get("rh_max",   i)),
            "rh_min":   parse_float(_get("rh_min",   i)),
            "wind":     parse_float(_get("wind",     i)),
            "wind_dir": _get("wind_dir", i),
        })

    log.info("Parsed %d forecast days", len(days))
    return {
        "generated":      datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "advisory":       advisory.strip().capitalize(),
        "weekly_rain_mm": weekly_rain,
        "days":           days,
    }


def parse_monthly_rainfall(soup):
    """
    Parse the monthly and annual rainfall table.

    Identifies the table by looking for multiple month names and a year in
    range 2018–2035.  Returns a list of dicts matching MONTH_COLS.
    """
    rf_table = None
    for tbl in soup.find_all("table"):
        text = tbl.get_text().lower()
        has_months = "jan" in text and "jul" in text and "total" in text
        has_year   = any(str(y) in text for y in range(2018, 2035))
        if has_months and has_year:
            rf_table = tbl
            break

    if rf_table is None:
        log.warning("Monthly rainfall table not found — skipping")
        return []

    results = []
    for row in rf_table.find_all("tr")[1:]:  # skip header row
        cells = [safe_text(c).replace("*", "").strip() for c in row.find_all("td")]
        if len(cells) < 14:
            continue
        try:
            year = int(cells[0])
        except ValueError:
            continue

        month_vals = [parse_float(cells[m + 1], default=0.0) or 0.0 for m in range(12)]
        total      = parse_float(cells[13])
        results.append({
            "year":  year,
            "jan":   month_vals[0],  "feb": month_vals[1],  "mar": month_vals[2],
            "apr":   month_vals[3],  "may": month_vals[4],  "jun": month_vals[5],
            "jul":   month_vals[6],  "aug": month_vals[7],  "sep": month_vals[8],
            "oct":   month_vals[9],  "nov": month_vals[10], "dec": month_vals[11],
            "total": total,
        })

    log.info("Parsed %d rainfall year rows", len(results))
    return results


# ── CSV merge helpers ──────────────────────────────────────────────────────────

def load_csv_as_dict(path, key_col="date"):
    """Load a CSV file into an ordered dict keyed by key_col."""
    result = {}
    try:
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                result[row[key_col]] = row
    except FileNotFoundError:
        pass
    return result


# ── Writers ───────────────────────────────────────────────────────────────────

def merge_obs(new_records, dry_run=False):
    """
    Merge new observation records into the existing CSV.

    - New dates are appended.
    - Existing dates with changed values are updated (handles late corrections
      to the day's data, which IARI sometimes publishes the following morning).
    - The file is always written sorted by date ascending.

    Returns (added, updated) counts.
    """
    existing = load_csv_as_dict(OBS_CSV)
    added = updated = 0

    for rec in new_records:
        date    = rec["date"]
        rec_str = {k: ("" if v is None else str(v)) for k, v in rec.items()}

        if date not in existing:
            existing[date] = rec_str
            added += 1
            log.debug("  + new row: %s", date)
        else:
            old     = existing[date]
            changed = any(
                old.get(k, "") != rec_str.get(k, "")
                for k in OBS_COLUMNS if k != "date"
            )
            if changed:
                existing[date] = rec_str
                updated += 1
                log.debug("  ~ updated: %s", date)

    if dry_run:
        log.info("[DRY RUN] Would write %d rows (%d added, %d updated) → %s",
                 len(existing), added, updated, OBS_CSV)
        return added, updated

    sorted_rows = sorted(existing.values(), key=lambda r: r["date"])
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(OBS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OBS_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(sorted_rows)

    log.info("Wrote %d rows → %s  (%d added, %d updated)",
             len(sorted_rows), OBS_CSV, added, updated)
    return added, updated


def write_forecast(data, dry_run=False):
    """Write forecast JSON (replaces previous file entirely)."""
    if data is None:
        log.warning("No forecast data to write")
        return
    if dry_run:
        log.info("[DRY RUN] Would write forecast.json (%d days)", len(data.get("days", [])))
        return
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(FORECAST_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    log.info("Wrote forecast.json (%d forecast days)", len(data.get("days", [])))


def write_monthly_rainfall(records, dry_run=False):
    """Upsert monthly rainfall rows by year."""
    if not records:
        return
    existing = load_csv_as_dict(RAINFALL_CSV, key_col="year")
    for rec in records:
        rec_str = {k: ("" if v is None else str(v)) for k, v in rec.items()}
        existing[str(rec["year"])] = rec_str

    if dry_run:
        log.info("[DRY RUN] Would write monthly_rainfall.csv (%d years)", len(existing))
        return

    sorted_rows = sorted(existing.values(), key=lambda r: r["year"])
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(RAINFALL_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MONTH_COLS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(sorted_rows)
    log.info("Wrote monthly_rainfall.csv (%d year rows)", len(sorted_rows))


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="IARI daily weather scraper")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse and validate without writing any output files",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable DEBUG-level logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )

    log.info("══════════════════════════════════════════════")
    log.info("  IARI weather scraper starting")
    log.info("  URL      : %s", URL)
    log.info("  Data dir : %s", DATA_DIR)
    if args.dry_run:
        log.info("  Mode     : DRY RUN — no files will be written")
    log.info("══════════════════════════════════════════════")

    # ── 1. Fetch ──────────────────────────────────────────────────────────────
    try:
        soup = fetch_page()
    except Exception as exc:
        log.critical("Could not fetch page — aborting: %s", exc)
        sys.exit(1)

    # ── 2. Parse ──────────────────────────────────────────────────────────────
    errors = []

    try:
        obs_records = parse_obs_table(soup)
        if not obs_records:
            errors.append("Observation table was empty after parsing")
    except Exception as exc:
        log.error("Observation parse error: %s", exc)
        errors.append(f"obs: {exc}")
        obs_records = []

    try:
        forecast = parse_forecast_table(soup)
    except Exception as exc:
        log.error("Forecast parse error: %s", exc)
        errors.append(f"forecast: {exc}")
        forecast = None

    try:
        rainfall = parse_monthly_rainfall(soup)
    except Exception as exc:
        log.error("Rainfall parse error: %s", exc)
        errors.append(f"rainfall: {exc}")
        rainfall = []

    # Abort if the critical observations table failed
    if not obs_records:
        log.critical("No observation records parsed — aborting without writing any files")
        sys.exit(2)

    # ── 3. Write ──────────────────────────────────────────────────────────────
    added, updated = merge_obs(obs_records,    dry_run=args.dry_run)
    write_forecast(forecast,                   dry_run=args.dry_run)
    write_monthly_rainfall(rainfall,           dry_run=args.dry_run)

    # ── 4. Summary ────────────────────────────────────────────────────────────
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    summary = [
        f"Completed  : {ts}",
        f"Obs rows   : {len(obs_records)} parsed  |  {added} added  |  {updated} updated",
        f"Forecast   : {'written ✓' if forecast else 'missing ✗'}",
        f"Rainfall   : {'written ✓' if rainfall else 'missing ✗'}",
    ]
    if errors:
        summary.append("Warnings   : " + " | ".join(errors))

    print("\n" + "\n".join(summary) + "\n")

    # Append to GitHub Actions step summary if running in CI
    gha_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if gha_summary:
        with open(gha_summary, "a", encoding="utf-8") as f:
            f.write("\n## IARI Scrape Summary\n\n```\n")
            f.write("\n".join(summary))
            f.write("\n```\n")

    log.info("══ Done ══")


if __name__ == "__main__":
    main()
