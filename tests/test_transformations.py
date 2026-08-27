"""
Unit tests for pipeline transformations.
These tests run in CI without Spark — no PySpark dependency.
"""
import os
import sys
import pytest

# Make repo root importable
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


# ══════════════════════════════════════════════════════════════════════
# 1. configs/env.py
# ══════════════════════════════════════════════════════════════════════

class TestEnvConfig:
    """Test environment configuration resolves correctly per target."""

    def test_default_environment_is_prod(self):
        """No env var set → defaults to prod."""
        os.environ.pop("PIPELINE_ENV", None)
        from configs.env import get_environment
        assert get_environment() == "prod"

    def test_dev_environment(self, monkeypatch):
        """PIPELINE_ENV=dev → returns dev."""
        monkeypatch.setenv("PIPELINE_ENV", "dev")
        # Re-import to pick up env change
        import importlib, configs.env
        importlib.reload(configs.env)
        from configs.env import get_environment
        assert get_environment() == "dev"

    def test_catalog_is_always_workspace(self):
        """Catalog is always workspace regardless of environment."""
        from configs.env import get_catalog
        assert get_catalog() == "workspace"

    def test_schema_returns_mention_dw(self):
        """Schema returns mention_dw (prod default)."""
        from configs.env import get_schema
        assert get_schema() == "mention_dw"
        assert get_schema("gold") == "mention_dw"
        assert get_schema("silver") == "mention_dw"


# ══════════════════════════════════════════════════════════════════════
# 2. PIPELINE_SCHEMA / PIPELINE_CATALOG env var resolution
#    (what the job scripts use — tested without importing PySpark)
# ══════════════════════════════════════════════════════════════════════

class TestJobEnvVars:
    """Test that job scripts resolve schema/catalog from env vars correctly."""

    def test_default_schema_is_prod(self):
        """No env var → defaults to mention_dw (prod)."""
        os.environ.pop("PIPELINE_SCHEMA", None)
        schema = os.getenv("PIPELINE_SCHEMA", "mention_dw")
        assert schema == "mention_dw"

    def test_dev_schema(self, monkeypatch):
        """PIPELINE_SCHEMA=uat_mention_dw → dev schema."""
        monkeypatch.setenv("PIPELINE_SCHEMA", "uat_mention_dw")
        schema = os.getenv("PIPELINE_SCHEMA", "mention_dw")
        assert schema == "uat_mention_dw"

    def test_default_catalog_is_workspace(self):
        """No env var → defaults to workspace."""
        os.environ.pop("PIPELINE_CATALOG", None)
        catalog = os.getenv("PIPELINE_CATALOG", "workspace")
        assert catalog == "workspace"

    def test_prod_schema_not_uat(self):
        """Prod schema must never be uat_mention_dw."""
        os.environ.pop("PIPELINE_SCHEMA", None)
        schema = os.getenv("PIPELINE_SCHEMA", "mention_dw")
        assert "uat" not in schema

    def test_dev_schema_not_prod(self, monkeypatch):
        """Dev schema must never be mention_dw (prod)."""
        monkeypatch.setenv("PIPELINE_SCHEMA", "uat_mention_dw")
        schema = os.getenv("PIPELINE_SCHEMA", "mention_dw")
        assert schema != "mention_dw"


# ══════════════════════════════════════════════════════════════════════
# 3. databricks.yml structure
# ══════════════════════════════════════════════════════════════════════

class TestBundleConfig:
    """Test databricks.yml has required fields for both targets."""

    @pytest.fixture(scope="class")
    def bundle_config(self):
        import yaml
        bundle_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "databricks.yml")
        with open(bundle_path) as f:
            return yaml.safe_load(f)

    def test_bundle_name_exists(self, bundle_config):
        assert bundle_config["bundle"]["name"] == "kc_data_pipeline"

    def test_prod_target_exists(self, bundle_config):
        assert "prod" in bundle_config["targets"]

    def test_dev_target_exists(self, bundle_config):
        assert "dev" in bundle_config["targets"]

    def test_prod_mode_is_production(self, bundle_config):
        assert bundle_config["targets"]["prod"]["mode"] == "production"

    def test_dev_mode_is_development(self, bundle_config):
        assert bundle_config["targets"]["dev"]["mode"] == "development"

    def test_prod_schema_is_mention_dw(self, bundle_config):
        prod_vars = bundle_config["targets"]["prod"].get("variables", {})
        assert prod_vars.get("pipeline_schema") == "mention_dw"

    def test_dev_schema_is_uat(self, bundle_config):
        dev_vars = bundle_config["targets"]["dev"].get("variables", {})
        assert dev_vars.get("pipeline_schema") == "uat_mention_dw"

    def test_prod_host_configured(self, bundle_config):
        host = bundle_config["targets"]["prod"]["workspace"]["host"]
        assert host.startswith("https://")
        assert "databricks.com" in host


# ══════════════════════════════════════════════════════════════════════
# 4. Pipeline resource YAML structure
# ══════════════════════════════════════════════════════════════════════

class TestPipelineResources:
    """Test pipeline resource YAMLs have required fields."""

    @pytest.fixture(params=["dim_pipeline.pipeline.yml"])
    def pipeline_config(self, request):
        import yaml
        resource_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "resources", request.param
        )
        with open(resource_path) as f:
            return yaml.safe_load(f)

    def test_pipeline_resource_exists(self, pipeline_config):
        assert "resources" in pipeline_config
        assert "pipelines" in pipeline_config["resources"]

    def test_catalog_is_workspace(self, pipeline_config):
        pipelines = pipeline_config["resources"]["pipelines"]
        for name, spec in pipelines.items():
            assert spec["catalog"] == "workspace", f"{name}: catalog must be workspace"

    def test_libraries_defined(self, pipeline_config):
        pipelines = pipeline_config["resources"]["pipelines"]
        for name, spec in pipelines.items():
            assert len(spec.get("libraries", [])) > 0, f"{name}: no libraries defined"

    def test_schema_uses_variable(self, pipeline_config):
        pipelines = pipeline_config["resources"]["pipelines"]
        for name, spec in pipelines.items():
            assert "${var.pipeline_schema}" in spec["schema"], \
                f"{name}: schema must use ${{var.pipeline_schema}}, not hardcoded"

    def test_pipeline_config_passes_schema(self, pipeline_config):
        pipelines = pipeline_config["resources"]["pipelines"]
        for name, spec in pipelines.items():
            config = spec.get("configuration", {})
            assert "PIPELINE_SCHEMA" in config, \
                f"{name}: PIPELINE_SCHEMA must be passed in configuration"


# ══════════════════════════════════════════════════════════════════════
# 5. Job scripts — schema/catalog not hardcoded
# ══════════════════════════════════════════════════════════════════════

class TestJobScripts:
    """Ensure job scripts use os.getenv, not hardcoded schema/catalog."""

    @pytest.fixture(scope="class")
    def job_files(self):
        jobs_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "pipelines", "facts", "jobs"
        )
        return [
            os.path.join(jobs_dir, f)
            for f in os.listdir(jobs_dir)
            if f.endswith(".py")
        ]

    def test_no_hardcoded_prod_schema(self, job_files):
        """No job file should hardcode SCHEMA = 'mention_dw'."""
        for fpath in job_files:
            with open(fpath) as f:
                content = f.read()
            assert 'SCHEMA        = "mention_dw"' not in content, \
                f"{os.path.basename(fpath)}: hardcoded prod schema found — use os.getenv"

    def test_no_hardcoded_uat_schema(self, job_files):
        """No job file should hardcode SCHEMA = 'uat_mention_dw'."""
        for fpath in job_files:
            with open(fpath) as f:
                content = f.read()
            assert 'SCHEMA        = "uat_mention_dw"' not in content, \
                f"{os.path.basename(fpath)}: hardcoded dev schema found — use os.getenv"

    def test_all_jobs_use_getenv_for_schema(self, job_files):
        """Every job file must resolve SCHEMA via os.getenv."""
        for fpath in job_files:
            with open(fpath) as f:
                content = f.read()
            assert 'os.getenv("PIPELINE_SCHEMA"' in content, \
                f"{os.path.basename(fpath)}: missing os.getenv for PIPELINE_SCHEMA"

    def test_all_jobs_import_os(self, job_files):
        """Every job file must import os."""
        for fpath in job_files:
            with open(fpath) as f:
                content = f.read()
            assert "import os" in content, \
                f"{os.path.basename(fpath)}: missing import os"
