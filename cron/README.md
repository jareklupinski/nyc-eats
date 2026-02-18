# Scheduled refresh for NYC Eats

The `nyc-eats-refresh` script rebuilds the site with fresh data from both APIs
and deploys it to the nginx serving directory.  It runs on the VPS as a
**systemd timer**.

## What it does

1. Ensures the Python virtualenv and dependencies are current
2. Runs `build.py` (full fetch — no cache)
3. `rsync`s `dist/` to the nginx serving root

## Schedule

**Every Sunday at ~3:00 AM US/Eastern** (08:00 UTC, with up to 5 min jitter).

## Files

| File | Purpose |
|------|---------|
| `nyc-eats-refresh` | Bash script that does the actual work |
| `nyc-eats-refresh.service.in` | systemd service template (values from `.env`) |
| `nyc-eats-refresh.timer` | systemd timer unit (weekly schedule) |

## Installation

From your local machine (requires `.env` — see `.env.example`):

```bash
make timer-install      # generates service, uploads units, enables timer
```

Or manually on the VPS:

```bash
sudo cp ~/nyc-eats/cron/nyc-eats-refresh.service /etc/systemd/system/
sudo cp ~/nyc-eats/cron/nyc-eats-refresh.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now nyc-eats-refresh.timer
```

## Check status

```bash
# Next scheduled run
ssh $VPS_HOST systemctl list-timers nyc-eats-refresh.timer

# Last run result
ssh $VPS_HOST systemctl status nyc-eats-refresh.service

# Logs (path configured in .env as VPS_PATH)
ssh $VPS_HOST tail -30 ~/your-site/refresh.log
# or via journald:
ssh $VPS_HOST journalctl -u nyc-eats-refresh.service --no-pager -n 30
```

## Manual trigger

```bash
ssh $VPS_HOST sudo systemctl start nyc-eats-refresh.service
```

## Removal

```bash
make timer-remove       # disables + removes units on VPS
```

Or manually:

```bash
sudo systemctl disable --now nyc-eats-refresh.timer
sudo rm /etc/systemd/system/nyc-eats-refresh.{service,timer}
sudo systemctl daemon-reload
```

## Configuration

Environment variables in the refresh script (all optional):

| Variable | Default | Description |
|----------|---------|-------------|
| `NYC_EATS_REPO` | `~/nyc-eats` | Path to the project on the server |
| `NYC_EATS_SERVE` | `~/nyc-eats-site/dist` | nginx serving root |
| `NYC_EATS_LOG` | `~/nyc-eats-site/refresh.log` | Log file path |
