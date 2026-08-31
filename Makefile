.PHONY: validate validate-dev validate-prod
.PHONY: deploy deploy-dev deploy-prod
.PHONY: run-dim-prod
.PHONY: test clean

# ── Validate ────────────────────────────────────────────────────────
validate: validate-dev validate-prod

validate-dev:
	databricks bundle validate --target dev --profile HauNC

validate-prod:
	databricks bundle validate --target prod --profile HauNC

# ── Deploy ──────────────────────────────────────────────────────────
# dev: code-sync only, no resources (pipelines/jobs are created manually in the UI)
deploy-dev:
	databricks bundle deploy --target dev --profile HauNC

deploy-prod:
	databricks bundle deploy --target prod --profile HauNC

# ── Run dim pipeline (prod only — dev pipelines are run manually from the UI) ──
run-dim-prod:
	databricks bundle run dim_pipeline --target prod --profile HauNC

# ── Tests ───────────────────────────────────────────────────────────
test:
	pytest tests/ -v --tb=short

# ── Cleanup ─────────────────────────────────────────────────────────
clean:
	rm -rf .databricks/ __pycache__/ .pytest_cache/
