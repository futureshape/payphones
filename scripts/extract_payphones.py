#!/usr/bin/env python3
"""Extract BT payphone closure rows from the PDF and geocode by postcode."""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PDF_PATH = ROOT / "public-payphone-closures-16-4-26.pdf"
DATA_DIR = ROOT / "data"
CSV_PATH = DATA_DIR / "payphones.csv"
GEOJSON_PATH = DATA_DIR / "payphones.geojson"
CACHE_PATH = DATA_DIR / "postcode_cache.json"
FAILURES_PATH = DATA_DIR / "payphones_geocode_failures.csv"
SUMMARY_PATH = DATA_DIR / "summary.json"

POSTCODE_RE = re.compile(r"\b[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}\b")
FULL_ROW_RE = re.compile(
    r"^(?P<address>.*?)\s+"
    r"(?P<postcode>[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2})\s+"
    r"(?P<public_body>.*?)\s+"
    r"(?P<removal_proposal_sent>\d{2}/\d{2}/\d{4})\s+"
    r"(?P<public_body_representation_made>Yes|No)\s+"
    r"(?P<bt_initial_decision>Remove|Retain)\s+"
    r"(?P<bt_initial_decision_date>\d{2}/\d{2}/\d{4})\s+"
    r"(?P<public_body_review_requested>Yes|No)\s+"
    r"(?P<bt_final_decision>Remove|Retain)\s+"
    r"(?P<bt_final_decision_date>\d{2}/\d{2}/\d{4})\s*$"
)
PENDING_FINAL_RE = re.compile(
    r"^(?P<address>.*?)\s+"
    r"(?P<postcode>[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2})\s+"
    r"(?P<public_body>.*?)\s+"
    r"(?P<removal_proposal_sent>\d{2}/\d{2}/\d{4})\s+"
    r"(?P<public_body_representation_made>Yes|No)\s+"
    r"(?P<bt_initial_decision>Remove|Retain)\s+"
    r"(?P<bt_initial_decision_date>\d{2}/\d{2}/\d{4})\s+"
    r"(?P<public_body_review_requested>Yes|No)\s*$"
)
DATE_ONLY_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")
HEADER_WORDS_RE = re.compile(
    r"\b(Address|Post Code|Public Body|Removal|Proposal|decision|requested|General)\b",
    re.IGNORECASE,
)


FIELDNAMES = [
    "id",
    "address",
    "postcode",
    "public_body",
    "removal_proposal_sent",
    "public_body_representation_made",
    "bt_initial_decision",
    "bt_initial_decision_date",
    "public_body_review_requested",
    "bt_final_decision",
    "bt_final_decision_date",
    "latitude",
    "longitude",
    "geocoded_postcode",
    "geocoding_status",
]


def normalize_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def clean_pdf_line(raw: str) -> str:
    line = raw.replace("\f", " ")
    line = re.sub(r"\bGeneral\b|#", " ", line)
    return normalize_spaces(line)


def extract_text(pdf_path: Path) -> str:
    try:
        return subprocess.check_output(
            ["pdftotext", "-layout", str(pdf_path), "-"],
            text=True,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        sys.exit("pdftotext is required. Install poppler-utils/poppler and rerun.")
    except subprocess.CalledProcessError as exc:
        sys.exit(exc.stderr or "Failed to extract PDF text.")


def is_probable_address_continuation(line: str) -> bool:
    if not line or POSTCODE_RE.search(line) or DATE_ONLY_RE.fullmatch(line):
        return False
    if HEADER_WORDS_RE.search(line):
        return False
    if re.search(r"\d{2}/\d{2}/\d{4}", line):
        return False
    return True


def parse_rows(text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    unmatched: list[str] = []
    pending_final: str | None = None
    pending_address: str | None = None

    for raw in text.splitlines():
        line = clean_pdf_line(raw)
        if not line:
            continue

        if pending_final and DATE_ONLY_RE.fullmatch(line):
            line = f"{pending_final} {line}"
            pending_final = None

        if pending_address and POSTCODE_RE.match(line):
            line = f"{pending_address} {line}"
            pending_address = None
        elif POSTCODE_RE.search(line):
            pending_address = None

        if not POSTCODE_RE.search(line):
            if is_probable_address_continuation(line):
                pending_address = line
            continue

        full_match = FULL_ROW_RE.match(line)
        if full_match:
            row = full_match.groupdict()
            rows.append(row)
            continue

        if re.search(r"\b(Remove|Retain)\s*$", line):
            pending_final = line
            continue

        pending_match = PENDING_FINAL_RE.match(line)
        if pending_match:
            row = pending_match.groupdict()
            row["bt_final_decision"] = ""
            row["bt_final_decision_date"] = ""
            rows.append(row)
            continue

        unmatched.append(line)

    if pending_final:
        unmatched.append(pending_final)

    if unmatched:
        sample = "\n".join(f"- {line}" for line in unmatched[:10])
        raise ValueError(f"Could not parse {len(unmatched)} row-like lines:\n{sample}")

    for index, row in enumerate(rows, start=1):
        row["id"] = str(index)
        row["postcode"] = normalize_postcode(row["postcode"])
        for key, value in list(row.items()):
            row[key] = normalize_spaces(value)

    return rows


def normalize_postcode(postcode: str) -> str:
    compact = re.sub(r"\s+", "", postcode.upper())
    if len(compact) <= 3:
        return compact
    return f"{compact[:-3]} {compact[-3:]}"


def load_cache() -> dict[str, dict]:
    if not CACHE_PATH.exists():
        return {}
    return json.loads(CACHE_PATH.read_text(encoding="utf-8"))


def save_cache(cache: dict[str, dict]) -> None:
    CACHE_PATH.write_text(json.dumps(cache, indent=2, sort_keys=True), encoding="utf-8")


def postcodes_io_batch(postcodes: list[str]) -> dict[str, dict]:
    payload = json.dumps({"postcodes": postcodes}).encode("utf-8")
    request = urllib.request.Request(
        "https://api.postcodes.io/postcodes",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        body = json.loads(response.read().decode("utf-8"))

    if body.get("status") != 200:
        raise RuntimeError(f"postcodes.io returned status {body.get('status')}")

    results = {}
    for item in body.get("result", []):
        query = normalize_postcode(item["query"])
        results[query] = item.get("result") or {}
    return results


def geocode_rows(rows: list[dict[str, str]], force: bool = False) -> None:
    cache = {} if force else load_cache()
    unique_postcodes = sorted({row["postcode"] for row in rows})
    missing = [postcode for postcode in unique_postcodes if postcode not in cache]

    for start in range(0, len(missing), 100):
        batch = missing[start : start + 100]
        try:
            results = postcodes_io_batch(batch)
        except (urllib.error.URLError, TimeoutError) as exc:
            raise RuntimeError(f"Could not reach postcodes.io: {exc}") from exc

        for postcode in batch:
            result = results.get(postcode) or {}
            cache[postcode] = {
                "postcode": result.get("postcode") or "",
                "latitude": result.get("latitude"),
                "longitude": result.get("longitude"),
                "status": "ok" if result.get("latitude") and result.get("longitude") else "not_found",
            }

        print(f"Geocoded {min(start + len(batch), len(missing))}/{len(missing)} new postcodes")
        save_cache(cache)
        time.sleep(0.15)

    for row in rows:
        result = cache.get(row["postcode"], {})
        latitude = result.get("latitude")
        longitude = result.get("longitude")
        row["latitude"] = "" if latitude is None else str(latitude)
        row["longitude"] = "" if longitude is None else str(longitude)
        row["geocoded_postcode"] = result.get("postcode", "")
        row["geocoding_status"] = result.get("status", "not_found")


def write_csv(rows: list[dict[str, str]]) -> None:
    with CSV_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def make_geojson(rows: list[dict[str, str]]) -> dict:
    features = []
    for row in rows:
        if not row.get("latitude") or not row.get("longitude"):
            continue

        properties = {key: value for key, value in row.items() if key not in {"latitude", "longitude"}}
        properties["full_address"] = f"{row['address']}, {row['postcode']}"
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(row["longitude"]), float(row["latitude"])],
                },
                "properties": properties,
            }
        )

    return {"type": "FeatureCollection", "features": features}


def write_geojson(rows: list[dict[str, str]]) -> None:
    geojson = make_geojson(rows)
    GEOJSON_PATH.write_text(json.dumps(geojson, indent=2), encoding="utf-8")

    failures = [row for row in rows if row.get("geocoding_status") != "ok"]
    with FAILURES_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(failures)


def write_summary(rows: list[dict[str, str]]) -> None:
    final_decisions: dict[str, int] = {}
    geocoded_final_decisions: dict[str, int] = {}

    for row in rows:
        decision = row["bt_final_decision"] or "Pending"
        final_decisions[decision] = final_decisions.get(decision, 0) + 1
        if row.get("geocoding_status") == "ok":
            geocoded_final_decisions[decision] = geocoded_final_decisions.get(decision, 0) + 1

    summary = {
        "source_pdf": PDF_PATH.name,
        "source_pdf_date": "2026-04-16",
        "total_rows": len(rows),
        "geocoded_rows": sum(1 for row in rows if row.get("geocoding_status") == "ok"),
        "not_geocoded_rows": sum(1 for row in rows if row.get("geocoding_status") != "ok"),
        "final_decisions": final_decisions,
        "geocoded_final_decisions": geocoded_final_decisions,
    }
    SUMMARY_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-geocode", action="store_true", help="Only parse the PDF table.")
    parser.add_argument("--force-geocode", action="store_true", help="Ignore the postcode cache.")
    args = parser.parse_args()

    DATA_DIR.mkdir(exist_ok=True)
    rows = parse_rows(extract_text(PDF_PATH))

    if not args.no_geocode:
        geocode_rows(rows, force=args.force_geocode)
    else:
        for row in rows:
            row["latitude"] = ""
            row["longitude"] = ""
            row["geocoded_postcode"] = ""
            row["geocoding_status"] = ""

    write_csv(rows)
    write_geojson(rows)
    write_summary(rows)

    final_counts = {}
    for row in rows:
        decision = row["bt_final_decision"] or "Pending"
        final_counts[decision] = final_counts.get(decision, 0) + 1

    print(f"Rows: {len(rows)}")
    print(f"Final decisions: {final_counts}")
    print(f"Geocoded rows: {sum(1 for row in rows if row.get('geocoding_status') == 'ok')}")
    print(f"Wrote {CSV_PATH.relative_to(ROOT)} and {GEOJSON_PATH.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
