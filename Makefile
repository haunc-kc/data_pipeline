.PHONY: validate deploy test run-dim run-fact clean

validate:
	databricks bundle validate --target prod

deploy:
	databricks bundle deploy --target prod

test:
	pytest tests/ -v --tb=short

run-dim:
	databricks bundle run dim_pipeline --target prod

run-fact:
	databricks bundle run fact_pipeline --target prod

clean:
	rm -rf .databricks/ __pycache__/ .pytest_cache/
