# BT Payphone Closure Map

This converts `public-payphone-closures-16-4-26.pdf` into structured data and maps the payphones with OpenFreeMap and MapLibre GL JS.

Live site: https://futureshape.github.io/payphones/

## Files

- `.github/workflows/pages.yml`: GitHub Pages deployment for this static site.
- `data/payphones.csv`: full extracted table with postcode.io latitude and longitude fields.
- `data/payphones.geojson`: geocoded points used by the map.
- `data/payphones_geocode_failures.csv`: rows where postcodes.io did not return coordinates.
- `data/postcode_cache.json`: cached postcodes.io responses.
- `data/summary.json`: generated row and decision counts.
- `index.html`: the interactive map.
- `scripts/extract_payphones.py`: repeatable PDF extraction and geocoding script.

## Run

```sh
python3 scripts/extract_payphones.py
python3 -m http.server 8000
```

Then open `http://localhost:8000/`.

The PDF includes repeated `General` labels from the original export; the extractor strips those before parsing.

## Deploy

GitHub Pages is configured with `.github/workflows/pages.yml`. Push to `main` or run the workflow manually from GitHub Actions to deploy the static site.

In the repository settings, set Pages to use **GitHub Actions** as the source. The published URL is:

```text
https://futureshape.github.io/payphones/
```
