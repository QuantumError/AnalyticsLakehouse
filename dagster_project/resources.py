import json
import subprocess
from dagster import ConfigurableResource
import requests

#----------------------------------------------------------------------------
# Custom resources for dbt CLI and Iceberg REST API
#----------------------------------------------------------------------------

class IcebergRestResource(ConfigurableResource):
    catalog_url: str = "http://iceberg-rest:8181"
    warehouse: str = "s3://warehouse/"

    def _base(self) -> str:
        resp = requests.get(
            f"{self.catalog_url}/v1/config",
            params={"warehouse": self.warehouse},
        )
        resp.raise_for_status()
        overrides = resp.json().get("overrides", {})
        return overrides.get("uri", self.catalog_url)

    def _get_metadata(self, namespace: str, table: str) -> dict:
        base = self._base()
        url = f"{base}/v1/namespaces/{namespace}/tables/{table}"
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.json().get("metadata", {})

    def get_snapshots(self, namespace: str, table: str) -> list[dict]:
        return self._get_metadata(namespace, table).get("snapshots", [])

    def get_current_snapshot_id(self, namespace: str, table: str) -> int | None:
        metadata = self._get_metadata(namespace, table)
        return metadata.get("current-snapshot-id")

    def get_latest_snapshot_id(self, namespace: str, table: str) -> int | None:
        snapshots = self.get_snapshots(namespace, table)
        if not snapshots:
            return None
        return max(s["snapshot-id"] for s in snapshots)

#----------------------------------------------------------------------------
# DbtCustomResource wraps dbt CLI commands for use in Dagster ops
#----------------------------------------------------------------------------

class DbtCustomResource(ConfigurableResource):
    project_dir: str
    profiles_dir: str = ""
    target: str = "prod"

    def _base_cmd(self) -> list[str]:
        return ["dbt", "--no-use-colors"]

    def _subcommand_flags(self, vars: dict | None = None) -> list[str]:
        flags = [
            "--project-dir", self.project_dir,
            "--target", self.target,
        ]
        if self.profiles_dir:
            flags += ["--profiles-dir", self.profiles_dir]
        if vars:
            flags += ["--vars", json.dumps(vars)]
        return flags

    def deps(self) -> None:
        cmd = self._base_cmd() + ["deps"] + self._subcommand_flags()
        self._exec(cmd)

    def run_model(
        self,
        model: str,
        full_refresh: bool = False,
        vars: dict | None = None,
    ) -> None:
        cmd = (
            self._base_cmd()
            + ["run"]
            + self._subcommand_flags(vars=vars)
            + ["--select", model]
        )
        if full_refresh:
            cmd.append("--full-refresh")
        self._exec(cmd)

    def run_models(
        self,
        models: list[str],
        full_refresh: bool = False,
        vars: dict | None = None,
    ) -> None:
        cmd = (
            self._base_cmd()
            + ["run"]
            + self._subcommand_flags(vars=vars)
            + ["--select", " ".join(models)]
        )
        if full_refresh:
            cmd.append("--full-refresh")
        self._exec(cmd)

    def test_models(self,
            models: list[str] | None = None,
            vars: dict | None = None,
            threads: int = 2,
        ) -> None:
            cmd = self._base_cmd() + ["test"] + self._subcommand_flags(vars=vars)
            cmd += ["--threads", str(threads)]
            if models:
                cmd += ["--select", " ".join(models)]
            self._exec(cmd)

    def _exec(self, cmd: list[str]) -> None:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr)
        if result.returncode != 0:
            raise RuntimeError(
                f"dbt command failed (exit {result.returncode}):\n"
                f"{' '.join(cmd)}\n\n"
                f"{result.stdout}\n{result.stderr}"
            )