PYTHON ?= python3
VENV ?= .venv
VENV_PYTHON := $(VENV)/bin/python

.PHONY: install install-python install-dashboard test test-python test-dashboard \
  test-e2e lint lint-python lint-dashboard build build-dashboard \
  build-k8s-copilot build-cost-copilot

install: install-python install-dashboard

install-python:
	$(PYTHON) -m venv $(VENV)
	$(VENV_PYTHON) -m pip install --upgrade pip
	$(VENV_PYTHON) -m pip install -r services/k8s-copilot/requirements.txt -r services/cost-copilot/requirements.txt
	$(VENV_PYTHON) -m pip install pytest pytest-cov ruff

install-dashboard:
	cd services/copilot-dashboard && npm install --legacy-peer-deps --no-audit --no-fund

test: test-python test-dashboard

test-python:
	$(VENV_PYTHON) -m pytest

test-dashboard:
	cd services/copilot-dashboard && npm run test -- --run

# The Gemini file is a real model-agent harness; run it explicitly after
# installing both service dependency sets. It is not part of ordinary CI.
test-e2e:
	$(VENV_PYTHON) -m pytest -o addopts= tests/test_gemini_38_flash_e2e.py

lint: lint-python lint-dashboard

lint-python:
	$(VENV_PYTHON) -m ruff check .

# ESLint is not configured in the dashboard package, so TypeScript checking
# is the supported source-level gate here.
lint-dashboard:
	cd services/copilot-dashboard && npm exec tsc -- --noEmit

build: build-dashboard build-k8s-copilot build-cost-copilot

build-dashboard:
	cd services/copilot-dashboard && npm run build

build-k8s-copilot:
	docker build -t cloud-copilot-k8s:ci services/k8s-copilot

build-cost-copilot:
	docker build -t cloud-copilot-cost:ci services/cost-copilot
