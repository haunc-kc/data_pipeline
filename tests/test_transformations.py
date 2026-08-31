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
    """Test pipeline resources defined ONCE at the top-level `resources:`
    block in databricks.yml, shared by both dev and prod targets (no
    per-target duplication — avoids copy/paste drift between environments).
    """

    @pytest.fixture
    def bundle_config(self):
        import yaml
        bundle_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "databricks.yml")
        with open(bundle_path) as f:
            return yaml.safe_load(f)

    @pytest.fixture
    def pipeline_config(self, bundle_config):
        return bundle_config

    def test_pipeline_resource_exists(self, pipeline_config):
        assert "resources" in pipeline_config
        assert "pipelines" in pipeline_config["resources"]

    def test_resources_not_duplicated_per_target(self, bundle_config):
        """Resources must be defined once at top level, not copy/pasted into
        each target — this is exactly the drift/typo bug class we hit
        before (e.g. infrom_be.py typo, missing depends_on)."""
        for target_name, target in bundle_config["targets"].items():
            assert "resources" not in target, \
                f"targets.{target_name} must not define its own 'resources' — " \
                "use the shared top-level resources: block instead"

    def test_dim_pipeline_uses_schema_variable(self, bundle_config):
        dim_pipeline = bundle_config["resources"]["pipelines"]["dim_pipeline"]
        assert dim_pipeline["schema"] == "${var.pipeline_schema}", \
            "dim_pipeline must use the pipeline_schema variable, not a hardcoded schema"

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

    def test_no_dunder_file_usage(self, job_files):
        """__file__ is not defined when Databricks runs a script as a
        spark_python_task (or notebook) — job scripts must not depend on it
        for sys.path resolution. Use --repo-root (argparse) instead."""
        for fpath in job_files:
            with open(fpath) as f:
                content = f.read()
            assert "__file__" not in content, \
                f"{os.path.basename(fpath)}: uses __file__, which is undefined " \
                "in Databricks job task execution — use --repo-root argparse instead"

    def test_all_jobs_accept_repo_root_arg(self, job_files):
        """Every job file must accept --repo-root for sys.path resolution."""
        for fpath in job_files:
            with open(fpath) as f:
                content = f.read()
            assert '"--repo-root"' in content, \
                f"{os.path.basename(fpath)}: missing --repo-root argparse argument"

    def test_all_jobs_compile(self, job_files):
        """Every job file must be syntactically valid Python."""
        import py_compile
        import tempfile
        for fpath in job_files:
            with tempfile.TemporaryDirectory() as tmpdir:
                py_compile.compile(fpath, cfile=os.path.join(tmpdir, "out.pyc"), doraise=True)

    def test_all_jobs_accept_pipeline_schema_arg(self, job_files):
        """Every job file must accept --pipeline-schema via argparse.

        Serverless spark_python_task does NOT support environment variables
        (only 'parameters' / CLI args), so os.getenv("PIPELINE_SCHEMA", ...)
        alone silently falls back to its default ("mention_dw" = PROD) when
        run as a job task. CLI args must take priority over the env var.
        """
        for fpath in job_files:
            with open(fpath) as f:
                content = f.read()
            assert '"--pipeline-schema"' in content, \
                f"{os.path.basename(fpath)}: missing --pipeline-schema argparse argument " \
                "— job tasks default to PROD schema without it"
            assert "_args.pipeline_schema" in content, \
                f"{os.path.basename(fpath)}: --pipeline-schema arg must be used " \
                "(_args.pipeline_schema) ahead of os.getenv fallback"

    def test_all_jobs_accept_pipeline_catalog_arg(self, job_files):
        """Every job file must accept --pipeline-catalog via argparse (see
        test_all_jobs_accept_pipeline_schema_arg for rationale)."""
        for fpath in job_files:
            with open(fpath) as f:
                content = f.read()
            assert '"--pipeline-catalog"' in content, \
                f"{os.path.basename(fpath)}: missing --pipeline-catalog argparse argument"
            assert "_args.pipeline_catalog" in content, \
                f"{os.path.basename(fpath)}: --pipeline-catalog arg must be used " \
                "(_args.pipeline_catalog) ahead of os.getenv fallback"


class TestBundleJobParameters:
    """Ensure databricks.yml actually passes --pipeline-schema/--pipeline-catalog
    to every facts_job task (bundle-config-side regression guard, complements
    TestJobScripts which checks the script side)."""

    @pytest.fixture
    def bundle_config(self):
        import yaml
        bundle_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "databricks.yml")
        with open(bundle_path) as f:
            return yaml.safe_load(f)

    def test_facts_job_tasks_pass_pipeline_schema(self, bundle_config):
        facts_job = bundle_config["resources"]["jobs"]["facts_job"]
        for task in facts_job["tasks"]:
            params = task.get("spark_python_task", {}).get("parameters", [])
            assert "--pipeline-schema" in params, \
                f"{task['task_key']}: databricks.yml must pass --pipeline-schema " \
                "or this task silently writes to PROD schema"
            assert "--pipeline-catalog" in params, \
                f"{task['task_key']}: databricks.yml must pass --pipeline-catalog"
