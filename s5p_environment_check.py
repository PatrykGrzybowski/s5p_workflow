"""Manual environment probe for CloudFerro Workflows / Airflow 3.x."""

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="s5p_environment_check",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["eo", "sentinel5p", "environment"],
)
def s5p_environment_check():
    @task(execution_timeout=timedelta(minutes=2), retries=0)
    def inspect_worker():
        import importlib.util
        import json
        import platform
        import shutil
        import sys
        from pathlib import Path

        report = {
            "python": sys.version,
            "platform": platform.platform(),
            "eodata_path_exists": Path("/eodata").exists(),
            "harpconvert_path": shutil.which("harpconvert"),
            "harpdump_path": shutil.which("harpdump"),
            "harp_module_found": importlib.util.find_spec("harp") is not None,
            "harp_python_api_usable": False,
        }
        if report["harp_module_found"]:
            try:
                import harp

                report["harp_module_file"] = getattr(harp, "__file__", None)
                report["harp_version"] = str(getattr(harp, "__version__", "not exposed"))
                report["harp_python_api_usable"] = all(
                    callable(getattr(harp, name, None))
                    for name in ("import_product", "export_product")
                )
            except Exception as error:
                report["harp_import_error"] = f"{type(error).__name__}: {error}"
        print(json.dumps(report, indent=2))
        print("Probe success only means the inspection ran; inspect the reported capabilities.")
        print("An existing /eodata directory does not prove access to the required L2 file.")
        return report

    inspect_worker()


s5p_environment_check()
