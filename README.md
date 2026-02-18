# NYC Eats 🍴

A static-site dashboard mapping every NYC restaurant and bar, built from two
open data sources.

![Leaflet + MarkerCluster map](https://img.shields.io/badge/map-Leaflet%201.9-green)
![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-yellow)

## Data sources

| Source | Records | API |
|--------|---------|-----|
| **DOHMH** — NYC Dept. of Health Restaurant Inspection Results | ~30 k | [Socrata `43nn-pn8j`](https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection-Results/43nn-pn8j) |
| **SLA** — NYS Liquor Authority Active Licenses (NYC only) | ~22 k | [ArcGIS FeatureServer](https://services8.arcgis.com/kHNnQD79LvY0XnKy/arcgis/rest/services/ActiveLicensesV3/FeatureServer/0) |

Sources are pluggable — drop a new `DataSource` subclass into `sources/` and
it's automatically discovered at build time.

## Merge pipeline

Venues appearing in both datasets are merged into a single marker with
`source="both"`.  The pipeline runs three passes:

1. **Exact address + borough** — normalised addresses (suffix canonicalization,
   ordinal stripping, AKA/unit removal) are compared by borough.
2. **Address range containment** — SLA range addresses like `"77 79 HUDSON ST"`
   match a DOHMH entry at `77 HUDSON STREET` if the number falls within the
   range.  Queens block-lot addresses (`30-12 20TH AVE`) are excluded.
3. **Geo-proximity ≤ 30 m** — remaining unmatched venues within 30 metres are
   merged using a grid-based spatial index.  Catches typos, AKA addresses, and
   other text variations.

Before merging, DOHMH venues are deduplicated:
- First by `camis` (unique restaurant ID — the dataset has one row per
  inspection).
- Then by **normalised name + BIN** (Building Identification Number) to
  collapse duplicate registrations in the same building.

Typical result: **~51 k** raw → **~37 k** post-merge, with **~14 k** merged
pairs.  Exact counts are shown in the sidebar's *Pipeline* table.

## Map features

- **Leaflet 1.9** with CARTO light base tiles and **MarkerCluster**
- Grade-letter markers (A/B/C/P/Z) for DOHMH & merged venues
- Martini-glass markers for SLA-only venues
- Purple merged markers showing both names in the popup
- Spiderfying on every zoom level for overlapping markers
- Filter by source, cuisine tag, and search by name
- Cache-busted `venues.js` via SHA-256 content hash

## Project structure

```
nyc-eats/
├── build.py              # Static site generator (fetch → merge → render)
├── Makefile              # Build, deploy, cron targets
├── requirements.txt      # Python deps (requests, jinja2)
├── nginx.conf.in         # nginx config template (values from .env)
├── .env.example          # Server-specific settings template
├── cron/
│   ├── nyc-eats-refresh            # Refresh script (runs on server)
│   ├── nyc-eats-refresh.service.in # systemd service template
│   ├── nyc-eats-refresh.timer      # systemd timer unit
│   └── README.md                   # Cron/timer setup instructions
├── sources/
│   ├── base.py           # Venue dataclass + DataSource ABC + auto-discovery
│   ├── dohmh.py          # DOHMH Socrata source
│   └── sla.py            # SLA ArcGIS FeatureServer source
├── templates/
│   └── index.html        # Jinja2 template (Leaflet map + sidebar)
├── static/
│   ├── style.css         # Dashboard styles
│   └── favicon.svg       # 🍴 favicon
└── dist/                 # Generated site (git-ignored)
```

## Quick start

```bash
# Clone & set up
git clone https://github.com/YOUR_USER/nyc-eats.git
cd nyc-eats
cp .env.example .env     # edit with your server details
make install              # creates .venv, installs deps

# Build (fetches live data, ~30s)
make build

# Or use cached data if already fetched in the last 24h
make build-cached

# Local dev server
make serve            # http://localhost:8000
```

## Deployment

The site can be deployed to any VPS with nginx.  Server-specific settings
(hostname, paths) live in `.env` (see `.env.example`).

```bash
make deploy           # build + rsync + reload nginx
make deploy-only      # rsync + reload (skip rebuild)
```

### Server setup (one-time)

```bash
# On the VPS — create the serving directory:
mkdir -p ~/your-domain.com/dist

# Symlink nginx config (done automatically by `make deploy`)
sudo ln -sf ~/your-domain.com/nginx.conf \
  /etc/nginx/sites-enabled/your-domain.com.conf
sudo nginx -t && sudo systemctl reload nginx

# HTTPS
sudo certbot --nginx -d your-domain.com
```

## Automated refresh

The data is refreshed every **Sunday at 3:00 AM ET** via a systemd timer on the
server.  See [`cron/README.md`](cron/README.md) for full details.

```bash
make timer-install    # uploads units + enables timer on VPS
```

The refresh script lives at [`cron/nyc-eats-refresh`](cron/nyc-eats-refresh) —
it rebuilds the site and rsyncs the output to the serving directory.
Logs go to `$VPS_PATH/refresh.log`.

## Adding a data source

1. Create `sources/my_source.py`
2. Subclass `DataSource` (from `sources.base`)
3. Implement `name`, `description` properties and `fetch() → list[Venue]`
4. Run `make build` — it's auto-discovered

## License

Unlicensed
