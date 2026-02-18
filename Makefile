.PHONY: build build-cached clean install cron-install cron-remove timer-install timer-remove serve deploy deploy-only

# Load deployment settings from .env (create from .env.example)
-include .env

PYTHON ?= python3
VENV   := .venv
PIP    := $(VENV)/bin/pip
PY     := $(VENV)/bin/python

# Server settings (override in .env or on command line)
VPS_HOST  ?= user@example.com
SITE_DOMAIN ?= food.example.com
VPS_PATH  ?= /home/user/$(SITE_DOMAIN)
VPS_REPO  ?= /home/user/nyc-eats

# --- Setup ---

$(VENV)/bin/activate: requirements.txt
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	touch $@

install: $(VENV)/bin/activate

# --- Build ---

build: install
	$(PY) build.py -v

build-cached: install
	$(PY) build.py --cache -v

# Build only DOHMH (example)
build-dohmh: install
	$(PY) build.py --sources dohmh -v

# --- Cron (local) ---
# Weekly rebuild every Sunday at 3am (for local dev machines with cron)
CRON_JOB := 0 3 * * 0 cd $(CURDIR) && $(MAKE) build >> $(CURDIR)/build.log 2>&1

cron-install:
	(crontab -l 2>/dev/null | grep -v 'nyc-eats'; echo "$(CRON_JOB) \# nyc-eats") | crontab -
	@echo "Cron job installed. Run 'crontab -l' to verify."

cron-remove:
	crontab -l 2>/dev/null | grep -v 'nyc-eats' | crontab -
	@echo "Cron job removed."

# --- Systemd timer (server) ---
# Weekly rebuild on VPS via systemd — see cron/README.md

timer-install: cron/nyc-eats-refresh.service
	rsync -avz cron/ $(VPS_HOST):$(VPS_REPO)/cron/
	ssh $(VPS_HOST) 'chmod +x $(VPS_REPO)/cron/nyc-eats-refresh && \
	  sudo cp $(VPS_REPO)/cron/nyc-eats-refresh.service /etc/systemd/system/ && \
	  sudo cp $(VPS_REPO)/cron/nyc-eats-refresh.timer /etc/systemd/system/ && \
	  sudo systemctl daemon-reload && \
	  sudo systemctl enable --now nyc-eats-refresh.timer'
	@echo "Timer installed. Check with: ssh $(VPS_HOST) systemctl list-timers nyc-eats*"

timer-remove:
	ssh $(VPS_HOST) 'sudo systemctl disable --now nyc-eats-refresh.timer && \
	  sudo rm -f /etc/systemd/system/nyc-eats-refresh.{service,timer} && \
	  sudo systemctl daemon-reload'
	@echo "Timer removed."

# --- Dev ---

serve: build
	cd dist && $(PYTHON) -m http.server 8000

# --- Deploy ---

deploy: build nginx.conf
	rsync -avz --delete dist/ $(VPS_HOST):$(VPS_PATH)/dist/
	rsync -avz nginx.conf $(VPS_HOST):$(VPS_PATH)/nginx.conf
	ssh $(VPS_HOST) 'sudo ln -sf $(VPS_PATH)/nginx.conf /etc/nginx/sites-enabled/$(SITE_DOMAIN).conf && sudo nginx -t && sudo systemctl reload nginx'

deploy-only: nginx.conf
	rsync -avz --delete dist/ $(VPS_HOST):$(VPS_PATH)/dist/
	rsync -avz nginx.conf $(VPS_HOST):$(VPS_PATH)/nginx.conf
	ssh $(VPS_HOST) 'sudo ln -sf $(VPS_PATH)/nginx.conf /etc/nginx/sites-enabled/$(SITE_DOMAIN).conf && sudo nginx -t && sudo systemctl reload nginx'

# --- Generated configs ---
# nginx.conf and the systemd service are generated from .in templates
# with server-specific values substituted from .env.

nginx.conf: nginx.conf.in .env
	sed -e 's|{{SITE_DOMAIN}}|$(SITE_DOMAIN)|g' \
	    -e 's|{{VPS_PATH}}|$(VPS_PATH)|g' $< > $@
	@echo "Generated $@"

cron/nyc-eats-refresh.service: cron/nyc-eats-refresh.service.in .env
	sed -e 's|{{VPS_REPO}}|$(VPS_REPO)|g' \
	    -e 's|{{VPS_PATH}}|$(VPS_PATH)|g' \
	    -e 's|{{VPS_USER}}|$(firstword $(subst @, ,$(VPS_HOST)))|g' $< > $@
	@echo "Generated $@"

clean:
	rm -rf dist .cache
