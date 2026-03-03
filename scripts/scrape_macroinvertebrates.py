#!/usr/bin/env python3
"""Scrape quiz specimen thumbnails from macroinvertebrates.org and tag them."""

from __future__ import annotations

import argparse
import csv
import html
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.cookiejar import CookieJar
from pathlib import Path

BASE_URL = "https://www.macroinvertebrates.org"
SPECIMEN_PATH = "/quiz/new"

# Raw quiz labels -> SciOly Water Quality canonical names
LABEL_MAP = {
    "black fly larva": "blackfly",
    "caddisfly larva (net spinning)": "caddisfly",
    "caddisfly larva (non-net spinning)": "caddisfly",
    "crane fly larva": "crane fly",
    "crayfish": "crayfish",
    "damselfly nymph": "damselfly",
    "dragonfly nymph": "dragonfly",
    "gilled snail": "gilled snail",
    "leech": "leech",
    "lunged snail": "air-breathing snail",
    "mayfly nymph": "mayfly",
    "midge larva": "midge",
    "riffle beetle (adult)": "riffle beetle",
    "riffle beetle (larva)": "riffle beetle",
    "scud": "scud",
    "sowbug": "aquatic sowbug",
    "stonefly nymph": "stonefly",
    "water penny larva": "water penny",
}


def slugify(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "unknown"


def parse_label(page_html: str) -> str | None:
    match = re.search(r"(?:Correct|No), it&#x27;s a (.+?)!", page_html)
    if not match:
        return None
    return html.unescape(match.group(1)).strip()


def parse_genus_id(page_html: str) -> str | None:
    match = re.search(r"genusId\s*:\s*'(\d+)'", page_html)
    return match.group(1) if match else None


def parse_thumbnail_urls(page_html: str) -> list[tuple[str, str]]:
    results = []
    for match in re.finditer(
        r'id="gigapan_thumbnail_(\d+)".*?src="(https://static\.macroinvertebrates\.org/[^"]+)"',
        page_html,
        re.DOTALL,
    ):
        gigapan_id = match.group(1)
        url = html.unescape(match.group(2))
        results.append((gigapan_id, url))
    return results


def read_existing_sources(tags_csv: Path) -> set[str]:
    if not tags_csv.exists():
        return set()
    with tags_csv.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return {row["source_url"] for row in reader if row.get("source_url")}


def ensure_csv(tags_csv: Path) -> None:
    if tags_csv.exists():
        return
    tags_csv.parent.mkdir(parents=True, exist_ok=True)
    with tags_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "downloaded_at_utc",
                "source_page",
                "source_url",
                "local_path",
                "raw_label",
                "mapped_label",
                "genus_id",
                "gigapan_image_id",
            ]
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download macroinvertebrates.org quiz specimen thumbnails and tag them."
    )
    parser.add_argument(
        "--out",
        default="water-quality-images",
        help="Output repo folder (default: water-quality-images)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=80,
        help="How many quiz pages to request (default: 80)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.2,
        help="Delay (seconds) between requests (default: 1.2)",
    )
    parser.add_argument(
        "--only-mapped",
        action="store_true",
        help="Only keep specimens that map to your Water Quality list.",
    )
    args = parser.parse_args()

    out_root = Path(args.out).resolve()
    images_root = out_root / "macroinvertebrates"
    tags_csv = out_root / "metadata" / "tags.csv"

    ensure_csv(tags_csv)
    existing_sources = read_existing_sources(tags_csv)

    cookie_jar = CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
    opener.addheaders = [
        ("User-Agent", "Mozilla/5.0 (compatible; water-quality-bot/1.0)"),
    ]

    downloaded_count = 0
    skipped_count = 0

    for i in range(args.iterations):
        try:
            with opener.open(BASE_URL + SPECIMEN_PATH, timeout=30) as resp:
                page_html = resp.read().decode("utf-8", errors="replace")
        except urllib.error.URLError as exc:
            print(f"[{i+1}/{args.iterations}] request failed: {exc}", file=sys.stderr)
            time.sleep(args.delay)
            continue

        raw_label = parse_label(page_html) or "Unknown"
        mapped_label = LABEL_MAP.get(raw_label.lower())
        if args.only_mapped and not mapped_label:
            skipped_count += 1
            time.sleep(args.delay)
            continue

        genus_id = parse_genus_id(page_html) or ""
        thumbnails = parse_thumbnail_urls(page_html)
        if not thumbnails:
            skipped_count += 1
            time.sleep(args.delay)
            continue

        for view_index, (gigapan_image_id, source_url) in enumerate(thumbnails, start=1):
            if source_url in existing_sources:
                skipped_count += 1
                continue

            if mapped_label:
                label_dir = images_root / slugify(mapped_label)
            else:
                label_dir = images_root / "_unmapped" / slugify(raw_label)
            label_dir.mkdir(parents=True, exist_ok=True)

            ext = os.path.splitext(urllib.parse.urlparse(source_url).path)[1] or ".jpg"
            filename = (
                f"{slugify(raw_label)}__g{genus_id or 'na'}__img{gigapan_image_id}"
                f"__v{view_index}{ext}"
            )
            local_path = label_dir / filename

            try:
                with opener.open(source_url, timeout=30) as img_resp:
                    data = img_resp.read()
                with local_path.open("wb") as f:
                    f.write(data)
            except urllib.error.URLError as exc:
                print(f"download failed for {source_url}: {exc}", file=sys.stderr)
                continue

            with tags_csv.open("a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        datetime.now(timezone.utc).isoformat(),
                        BASE_URL + SPECIMEN_PATH,
                        source_url,
                        str(local_path.relative_to(out_root)),
                        raw_label,
                        mapped_label or "",
                        genus_id,
                        gigapan_image_id,
                    ]
                )

            existing_sources.add(source_url)
            downloaded_count += 1

        print(
            f"[{i+1}/{args.iterations}] label={raw_label!r} mapped={mapped_label!r} "
            f"thumbs={len(thumbnails)} downloaded={downloaded_count} skipped={skipped_count}"
        )
        time.sleep(args.delay)

    # Small summary JSON for tooling
    summary_path = out_root / "metadata" / "summary.json"
    summary = {
        "scraped_at_utc": datetime.now(timezone.utc).isoformat(),
        "iterations": args.iterations,
        "downloaded": downloaded_count,
        "skipped": skipped_count,
        "unique_source_urls": len(existing_sources),
    }
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
