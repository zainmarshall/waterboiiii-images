#!/usr/bin/env python3
"""Download water-quality reference images from supported sources and tag them."""

from __future__ import annotations

import argparse
import csv
import html
import json
import os
import mimetypes
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
GOOGLE_CSE_ENDPOINT = "https://customsearch.googleapis.com/customsearch/v1"
INAT_API = "https://api.inaturalist.org/v1"

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

DEFAULT_GOOGLE_LABELS = [
    "mayfly",
    "aquatic sowbug",
    "water mite",
    "air-breathing snail",
    "whirligig beetle",
    "caddisfly",
    "damselfly",
    "midge",
    "deer fly",
    "water strider",
    "stonefly",
    "dragonfly",
    "blackfly",
    "tubifex",
    "mosquito",
    "dobsonfly",
    "scud",
    "flatworm",
    "blood midge",
    "giant water bug",
    "gilled snail",
    "crane fly",
    "leech",
    "backswimmer",
    "water penny",
    "water boatman",
    "riffle beetle",
    "predaceous diving beetle",
    "water scorpion",
]

# iNaturalist taxon search queries for each Water Quality organism.
# Keys = canonical folder name, values = iNaturalist taxon search term.
INAT_TAXA = {
    "mayfly": "Ephemeroptera",
    "aquatic sowbug": "Asellidae",
    "water mite": "Hydrachnidia",
    "air-breathing snail": "Pulmonata",
    "whirligig beetle": "Gyrinidae",
    "caddisfly": "Trichoptera",
    "damselfly": "Zygoptera",
    "midge": "Chironomidae",
    "deer fly": "Chrysops",
    "water strider": "Gerridae",
    "stonefly": "Plecoptera",
    "dragonfly": "Anisoptera",
    "blackfly": "Simuliidae",
    "tubifex": "Tubifex",
    "mosquito": "Culicidae",
    "dobsonfly": "Corydalidae",
    "scud": "Gammaridae",
    "flatworm": "Turbellaria",
    "blood midge": "Chironominae",
    "giant water bug": "Belostomatidae",
    "gilled snail": "Caenogastropoda",
    "crane fly": "Tipulidae",
    "leech": "Hirudinea",
    "backswimmer": "Notonectidae",
    "water penny": "Psephenidae",
    "water boatman": "Corixidae",
    "riffle beetle": "Elmidae",
    "predaceous diving beetle": "Dytiscidae",
    "water scorpion": "Nepidae",
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


def load_google_labels(labels_file: str | None) -> list[str]:
    if not labels_file:
        return DEFAULT_GOOGLE_LABELS
    path = Path(labels_file)
    if not path.exists():
        raise FileNotFoundError(f"labels file not found: {labels_file}")
    labels = []
    for line in path.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        labels.append(value)
    return labels


def infer_ext_from_response(source_url: str, resp: object) -> str:
    content_type = ""
    headers = getattr(resp, "headers", None)
    if headers is not None:
        content_type = headers.get("Content-Type", "")
    guessed = mimetypes.guess_extension(content_type.split(";")[0].strip()) if content_type else None
    if guessed:
        return guessed
    ext = os.path.splitext(urllib.parse.urlparse(source_url).path)[1]
    return ext if ext else ".jpg"


def download_from_google(args: argparse.Namespace, out_root: Path, tags_csv: Path) -> tuple[int, int, int]:
    api_key = args.google_api_key or os.getenv("GOOGLE_API_KEY")
    cse_id = args.google_cse_id or os.getenv("GOOGLE_CSE_ID")
    if not api_key or not cse_id:
        raise ValueError(
            "Google mode requires --google-api-key/--google-cse-id or env vars GOOGLE_API_KEY/GOOGLE_CSE_ID"
        )

    labels = load_google_labels(args.labels_file)
    if not labels:
        raise ValueError("no labels found for Google mode")

    images_root = out_root / "macroinvertebrates"
    existing_sources = read_existing_sources(tags_csv)

    opener = urllib.request.build_opener()
    opener.addheaders = [
        ("User-Agent", "Mozilla/5.0 (compatible; water-quality-bot/1.0)"),
    ]

    downloaded_count = 0
    skipped_count = 0
    request_count = 0

    for label in labels:
        query = f"{label} macroinvertebrate"
        page_limit = max(1, args.google_pages)
        per_page = min(max(1, args.google_per_page), 10)
        want_total = max(1, args.per_label)
        got_for_label = 0

        for page in range(page_limit):
            if got_for_label >= want_total:
                break

            start_index = page * per_page + 1
            params = {
                "key": api_key,
                "cx": cse_id,
                "searchType": "image",
                "q": query,
                "safe": args.google_safe,
                "num": str(per_page),
                "start": str(start_index),
            }
            search_url = GOOGLE_CSE_ENDPOINT + "?" + urllib.parse.urlencode(params)

            try:
                with opener.open(search_url, timeout=30) as resp:
                    payload = json.loads(resp.read().decode("utf-8", errors="replace"))
            except (urllib.error.URLError, json.JSONDecodeError) as exc:
                print(f"google query failed for {label!r}: {exc}", file=sys.stderr)
                break

            request_count += 1
            items = payload.get("items", [])
            if not items:
                break

            for result_index, item in enumerate(items, start=1):
                if got_for_label >= want_total:
                    break

                source_url = item.get("link")
                source_page = item.get("image", {}).get("contextLink") or search_url
                if not source_url:
                    skipped_count += 1
                    continue
                if source_url in existing_sources:
                    skipped_count += 1
                    continue

                label_dir = images_root / label.lower()
                label_dir.mkdir(parents=True, exist_ok=True)

                try:
                    with opener.open(source_url, timeout=30) as img_resp:
                        data = img_resp.read()
                        ext = infer_ext_from_response(source_url, img_resp)
                except urllib.error.URLError as exc:
                    print(f"download failed for {source_url}: {exc}", file=sys.stderr)
                    skipped_count += 1
                    continue

                filename = (
                    f"{slugify(label)}__google__p{page+1}__r{result_index}"
                    f"__n{got_for_label+1}{ext}"
                )
                local_path = label_dir / filename
                with local_path.open("wb") as f:
                    f.write(data)

                with tags_csv.open("a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(
                        [
                            datetime.now(timezone.utc).isoformat(),
                            source_page,
                            source_url,
                            str(local_path.relative_to(out_root)),
                            label,
                            label,
                            "",
                            "",
                        ]
                    )

                existing_sources.add(source_url)
                downloaded_count += 1
                got_for_label += 1

            time.sleep(args.delay)

        print(
            f"[google] label={label!r} downloaded={got_for_label} "
            f"total_downloaded={downloaded_count} skipped={skipped_count}"
        )

    return downloaded_count, skipped_count, request_count


def resolve_taxon_id(opener: urllib.request.OpenerDirector, name: str) -> int | None:
    """Look up an iNaturalist taxon ID by scientific name."""
    params = urllib.parse.urlencode({"q": name, "per_page": "1", "is_active": "true"})
    url = f"{INAT_API}/taxa?{params}"
    try:
        with opener.open(url, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
        results = data.get("results", [])
        if results:
            return results[0]["id"]
    except (urllib.error.URLError, json.JSONDecodeError, KeyError):
        pass
    return None


def download_from_inat(args: argparse.Namespace, out_root: Path, tags_csv: Path) -> tuple[int, int, int]:
    """Download research-grade photos from iNaturalist for all Water Quality organisms."""
    images_root = out_root / "macroinvertebrates"
    existing_sources = read_existing_sources(tags_csv)

    opener = urllib.request.build_opener()
    opener.addheaders = [
        ("User-Agent", "water-quality-bot/1.0 (https://github.com/zainmarshall/waterboiiii)"),
    ]

    downloaded_count = 0
    skipped_count = 0
    request_count = 0
    per_label = max(1, args.per_label)

    taxa_items = list(INAT_TAXA.items())
    for label_idx, (label, taxon_query) in enumerate(taxa_items, start=1):
        # Resolve taxon ID for precise results
        taxon_id = resolve_taxon_id(opener, taxon_query)
        request_count += 1
        if not taxon_id:
            print(f"[{label_idx}/{len(taxa_items)}] Could not resolve taxon for {label!r} ({taxon_query})", file=sys.stderr)
            time.sleep(args.delay)
            continue

        got_for_label = 0
        page = 1
        max_pages = max(1, (per_label + 29) // 30)  # 30 obs per page

        while got_for_label < per_label and page <= max_pages:
            params = urllib.parse.urlencode({
                "taxon_id": taxon_id,
                "photos": "true",
                "quality_grade": "research",
                "per_page": "30",
                "page": str(page),
                "order_by": "votes",
                "order": "desc",
            })
            obs_url = f"{INAT_API}/observations?{params}"

            try:
                with opener.open(obs_url, timeout=30) as resp:
                    payload = json.loads(resp.read().decode("utf-8", errors="replace"))
            except (urllib.error.URLError, json.JSONDecodeError) as exc:
                print(f"iNat query failed for {label!r} page {page}: {exc}", file=sys.stderr)
                break

            request_count += 1
            observations = payload.get("results", [])
            if not observations:
                break

            for obs in observations:
                if got_for_label >= per_label:
                    break
                photos = obs.get("photos", [])
                if not photos:
                    continue

                # Take the first (best) photo from each observation
                photo = photos[0]
                photo_url = photo.get("url", "")
                if not photo_url:
                    continue

                # Get medium size (592px) instead of square thumbnail
                photo_url = photo_url.replace("/square.", "/medium.")

                if photo_url in existing_sources:
                    skipped_count += 1
                    continue

                # Check license - only download CC-licensed photos
                license_code = photo.get("license_code")
                if not license_code:
                    skipped_count += 1
                    continue

                label_dir = images_root / label.lower()
                label_dir.mkdir(parents=True, exist_ok=True)

                obs_id = obs.get("id", "na")
                photo_id = photo.get("id", "na")
                ext = os.path.splitext(urllib.parse.urlparse(photo_url).path)[1] or ".jpeg"
                filename = f"{slugify(label)}__inat__obs{obs_id}__p{photo_id}{ext}"
                local_path = label_dir / filename

                try:
                    with opener.open(photo_url, timeout=30) as img_resp:
                        data = img_resp.read()
                    with local_path.open("wb") as f:
                        f.write(data)
                except urllib.error.URLError as exc:
                    print(f"download failed for {photo_url}: {exc}", file=sys.stderr)
                    skipped_count += 1
                    continue

                with tags_csv.open("a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        datetime.now(timezone.utc).isoformat(),
                        f"https://www.inaturalist.org/observations/{obs_id}",
                        photo_url,
                        str(local_path.relative_to(out_root)),
                        label,
                        label,
                        "",
                        str(photo_id),
                    ])

                existing_sources.add(photo_url)
                downloaded_count += 1
                got_for_label += 1

            page += 1
            time.sleep(args.delay)

        print(
            f"[{label_idx}/{len(taxa_items)}] label={label!r} taxon={taxon_query!r} "
            f"got={got_for_label} total_downloaded={downloaded_count} skipped={skipped_count}"
        )

    return downloaded_count, skipped_count, request_count


def download_from_macroinvertebrates(args: argparse.Namespace, out_root: Path, tags_csv: Path) -> tuple[int, int, int]:
    images_root = out_root / "macroinvertebrates"
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
                # Keep canonical item names so SciOly-ID can map folders directly.
                label_dir = images_root / mapped_label.lower()
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

    return downloaded_count, skipped_count, args.iterations


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download macroinvertebrate images from supported providers and tag them."
    )
    parser.add_argument(
        "--provider",
        choices=["inat", "macroinvertebrates", "google"],
        default="inat",
        help="Image source provider (default: inat)",
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
        help="Only keep specimens that map to your Water Quality list (macroinvertebrates mode only).",
    )
    parser.add_argument(
        "--labels-file",
        default="",
        help="Optional newline-delimited labels file for Google mode.",
    )
    parser.add_argument(
        "--per-label",
        type=int,
        default=12,
        help="Max images to download per label in Google mode (default: 12).",
    )
    parser.add_argument(
        "--google-pages",
        type=int,
        default=3,
        help="How many Google result pages to request per label (default: 3).",
    )
    parser.add_argument(
        "--google-per-page",
        type=int,
        default=10,
        help="How many results per Google API page (1-10, default: 10).",
    )
    parser.add_argument(
        "--google-safe",
        choices=["off", "active"],
        default="active",
        help="Google SafeSearch mode (default: active).",
    )
    parser.add_argument(
        "--google-api-key",
        default="",
        help="Google API key (or set GOOGLE_API_KEY env var).",
    )
    parser.add_argument(
        "--google-cse-id",
        default="",
        help="Google Programmable Search Engine ID (or set GOOGLE_CSE_ID env var).",
    )
    args = parser.parse_args()

    out_root = Path(args.out).resolve()
    images_root = out_root / "macroinvertebrates"
    tags_csv = out_root / "metadata" / "tags.csv"

    ensure_csv(tags_csv)
    if args.provider == "inat":
        downloaded_count, skipped_count, run_requests = download_from_inat(args, out_root, tags_csv)
    elif args.provider == "google":
        downloaded_count, skipped_count, run_requests = download_from_google(args, out_root, tags_csv)
    else:
        downloaded_count, skipped_count, run_requests = download_from_macroinvertebrates(args, out_root, tags_csv)

    unique_sources = len(read_existing_sources(tags_csv))

    # Small summary JSON for tooling
    summary_path = out_root / "metadata" / "summary.json"
    summary = {
        "scraped_at_utc": datetime.now(timezone.utc).isoformat(),
        "provider": args.provider,
        "iterations": args.iterations if args.provider == "macroinvertebrates" else len(INAT_TAXA) if args.provider == "inat" else None,
        "query_requests": run_requests,
        "downloaded": downloaded_count,
        "skipped": skipped_count,
        "unique_source_urls": unique_sources,
    }
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
