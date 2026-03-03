# Water Quality Images (Macroinvertebrates)

This folder is a standalone image dataset repo for your Water Quality bot.

## What this contains

- `macroinvertebrates/` downloaded quiz specimen thumbnails, grouped by mapped label.
- `metadata/tags.csv` source + tag metadata for every image.
- `metadata/summary.json` last scrape summary.
- `scripts/scrape_macroinvertebrates.py` scraper.

## Source

- Site: `https://www.macroinvertebrates.org/quiz/specimen`
- The site displays a Creative Commons notice (`CC BY-NC-SA 4.0`) in the page footer.
- You should still verify your intended use and attribution requirements before publishing.

## Run scraper

From repo root (`/Users/zain/Developer/waterboiiii`):

```sh
python3 water-quality-images/scripts/scrape_macroinvertebrates.py --iterations 120 --delay 1.2
```

Mapped-only mode (only labels that map to your Water Quality list):

```sh
python3 water-quality-images/scripts/scrape_macroinvertebrates.py --iterations 120 --delay 1.2 --only-mapped
```

## Metadata format (`tags.csv`)

Columns:

1. `downloaded_at_utc`
2. `source_page`
3. `source_url`
4. `local_path`
5. `raw_label`
6. `mapped_label`
7. `genus_id`
8. `gigapan_image_id`

