.PHONY: validate validate-dev validate-prod
.PHONY: deploy deploy-dev deploy-prod
.PHONY: run run-dev run-prod
.PHONY: test clean

# ── Validate ────────────────────────────────────────────────────────
validate: validate-dev validate-prod

validate-dev:
	databricks bundle validate --target dev --profile HauNC

validate-prod:
	databricks bundle validate --target prod --profile HauNC

# ── Deploy ──────────────────────────────────────────────────────────
deploy-dev:
	databricks bundle deploy --target dev --profile HauNC

deploy-prod:
	databricks bundle deploy --target prod --profile HauNC

# ── Run pipelines ───────────────────────────────────────────────────
run-dev: deploy-dev
	databricks bundle run dim_pipeline --target dev --profile HauNC
	databricks bundle run fact_pipeline --target dev --profile HauNC

run-prod: deploy-prod
	databricks bundle run dim_pipeline --target prod --profile HauNC
	databricks bundle run fact_pipeline --target prod --profile HauNC

# ── Tests ───────────────────────────────────────────────────────────
test:
	pytest tests/ -v --tb=short

# ── Cleanup ─────────────────────────────────────────────────────────
clean:
	rm -rf .databricks/ __pycache__/ .pytest_cache/
