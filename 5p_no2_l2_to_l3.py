"""Airflow 3.x: EODATA -> find NO2 -> HARP -> publish L3.

Connections: eodata_s3, s5p_output_s3.
HARP runs on the Workflows worker in a temporary environment downloaded from S3.
Prepare the conda-pack archive once on compatible Linux and upload to S3.
Workers need access to EODATA, catalogue and output S3, plus writable executable
temporary storage, boto3 and requests. No package solving occurs on workers.
XCom carries only product metadata and S3 locations, never credentials or rasters.
"""

from datetime import timedelta

import pendulum
from airflow.sdk import Connection, dag, get_current_context, task


CATALOGUE_URL = "https://datahub.creodias.eu/odata/v1/Products"
NO2_VARIABLE = "tropospheric_NO2_column_number_density"


def s3_connection(connection_id):
    import boto3
    from botocore.config import Config

    connection = Connection.get(connection_id)
    extra = connection.extra_dejson
    if not connection.login or not connection.password:
        raise ValueError(f"Missing access key or secret in Connection {connection_id}")
    client = boto3.client(
        "s3",
        endpoint_url=extra["endpoint_url"],
        region_name=extra.get("region_name"),
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        aws_session_token=extra.get("aws_session_token"),
        config=Config(**{
            "connect_timeout": 10,
            "read_timeout": 120,
            "s3": {"addressing_style": "path"},
            **extra.get("config_kwargs", {}),
        }),
    )
    default_bucket = "eodata" if connection_id == "eodata_s3" else "s5p_l3"
    return client, extra.get("bucket_name", default_bucket)


def record_progress(stage, **details):
    """Persist diagnostic progress in S3; this is NOT an Airflow heartbeat."""
    import hashlib
    import json
    from datetime import datetime, timezone
    from pathlib import Path

    run_id = get_current_context()["run_id"]
    run_tag = hashlib.sha256(run_id.encode()).hexdigest()[:16]
    report = {"run_id": run_id, "stage": stage,
              "updated_at": datetime.now(timezone.utc).isoformat(), "details": details}
    resources = {}
    for name in ("memory.current", "memory.max", "memory.events", "memory.stat", "cpu.stat"):
        path = Path("/sys/fs/cgroup") / name
        try:
            if path.is_file():
                resources[name] = path.read_text().strip()
        except OSError:
            pass
    report["worker_resources"] = resources
    client, bucket = s3_connection("s5p_output_s3")
    payload = json.dumps(report, indent=2).encode()
    for key in (f"diagnostics/s5p_no2/{run_tag}/progress.json", "diagnostics/s5p_no2/latest.json"):
        client.put_object(Bucket=bucket, Key=key, Body=payload, ContentType="application/json")
    print(f"Progress saved: {stage}; s3://{bucket}/diagnostics/s5p_no2/latest.json", flush=True)


def transfer_config():
    from boto3.s3.transfer import TransferConfig
    return TransferConfig(max_concurrency=1, use_threads=False, max_io_queue=4)


def candidate_keys(product, bucket):
    from urllib.parse import urlsplit

    source = product["S3Path"]
    if source.startswith("s3://"):
        parsed = urlsplit(source)
        if parsed.netloc != bucket:
            raise ValueError(f"Unexpected source bucket: {parsed.netloc}")
        prefix = parsed.path.lstrip("/")
    else:
        prefix = source.removeprefix(f"/{bucket}/").lstrip("/")
    prefix = prefix.rstrip("/")
    return list(dict.fromkeys([f"{prefix}/{product['Name']}", prefix]))


def download_l2(client, bucket, product, destination):
    from botocore.exceptions import ClientError

    for key in candidate_keys(product, bucket):
        try:
            client.download_file(bucket, key, str(destination), Config=transfer_config())
            print(f"Downloaded s3://{bucket}/{key}", flush=True)
            return key
        except ClientError as error:
            if str(error.response["Error"]["Code"]) not in {"404", "NoSuchKey", "NotFound"}:
                raise
    raise FileNotFoundError(f"L2 object not found for {product['Name']}")


def harp_operations(config):
    west, south, east, north = map(float, config["bbox"])
    resolution = float(config["grid_degrees"])
    if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
        raise ValueError("bbox must be [west, south, east, north] within valid geographic bounds")
    if not 0 < resolution <= 5:
        raise ValueError("grid_degrees must be greater than 0 and no more than 5")
    lat_intervals = (north - south) / resolution
    lon_intervals = (east - west) / resolution
    if (abs(lat_intervals - round(lat_intervals)) > 1e-6
            or abs(lon_intervals - round(lon_intervals)) > 1e-6):
        raise ValueError("bbox spans must be divisible by grid_degrees")
    return ";".join([
        f"keep(latitude,longitude,latitude_bounds,longitude_bounds,datetime_start,datetime_length,{NO2_VARIABLE},{NO2_VARIABLE}_validity,surface_zonal_wind_velocity,surface_meridional_wind_velocity)",
        f"latitude>={south}",
        f"latitude<{north}",
        f"longitude>={west}",
        f"longitude<{east}",
        f"{NO2_VARIABLE}_validity>75",
        "derive(surface_wind_speed {time} [m/s])",
        "surface_wind_speed<5",
        f"keep(latitude_bounds,longitude_bounds,datetime_start,datetime_length,{NO2_VARIABLE})",
        "derive(datetime_start {time} [days since 2000-01-01])",
        "derive(datetime_stop {time} [days since 2000-01-01])",
        "exclude(datetime_length)",
        f"bin_spatial({round(lat_intervals)+1},{south},{resolution},{round(lon_intervals)+1},{west},{resolution})",
        f"derive({NO2_VARIABLE} [Pmolec/cm2])",
        "derive(latitude {latitude})",
        "derive(longitude {longitude})",
        "count>0",
    ])


def prepare_harp(work_directory):
    """Load an environment prepared once on Linux using conda-pack."""
    import os
    import platform
    import subprocess
    import tarfile
    import time
    from pathlib import Path

    if platform.system() != "Linux" or platform.machine() not in {"x86_64", "AMD64"}:
        raise RuntimeError("The prepacked HARP environment targets Linux x86-64")
    started = time.monotonic()
    work = Path(work_directory)
    archive_path = work / "harp-runtime.tar.gz"
    prefix = work / "harp-env"
    prefix.mkdir()
    client, bucket = s3_connection("s5p_output_s3")
    runtime_key = "environments/harp-runtime-linux64-v1.tar.gz"
    print(f"Downloading prebuilt HARP environment: s3://{bucket}/{runtime_key}", flush=True)
    record_progress("downloading_environment", runtime_key=runtime_key)
    client.download_file(bucket, runtime_key, str(archive_path), Config=transfer_config())
    print(f"Environment downloaded: {archive_path.stat().st_size / 1024**2:.1f} MiB; extracting...", flush=True)
    record_progress("extracting_environment", archive_bytes=archive_path.stat().st_size)
    with tarfile.open(archive_path, "r:gz") as archive:
        archive.extractall(path=prefix, filter="data")
    python = prefix / "bin/python"
    unpack = prefix / "bin/conda-unpack"
    if not python.is_file() or not unpack.is_file():
        raise ValueError("Expected bin/python and bin/conda-unpack at the archive root. Use conda-pack.")
    environment = os.environ.copy()
    for name in ("PYTHONPATH", "PYTHONHOME", "CONDA_PREFIX", "CONDA_DEFAULT_ENV",
                 "CONDA_PROMPT_MODIFIER", "CONDA_SHLVL"):
        environment.pop(name, None)
    environment["PYTHONNOUSERSITE"] = "1"
    for name in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        environment[name] = "1"
    print("Relocating environment with conda-unpack...", flush=True)
    record_progress("relocating_environment")
    subprocess.run([str(python), str(unpack)], env=environment, check=True, timeout=300)
    # Activate the packed environment for native-library activation scripts.
    runner = ["bash", "-c", 'set -e; source "$1/bin/activate"; shift; exec "$@"',
              "harp-runtime", str(prefix)]
    subprocess.run(runner + ["harpconvert", "-v"], env=environment, check=True, timeout=60)
    print(f"HARP environment ready after {time.monotonic()-started:.1f} s", flush=True)
    archive_path.unlink()
    record_progress("environment_ready", elapsed_seconds=round(time.monotonic()-started, 1))
    return runner, environment

@dag(
    dag_id="s5p_no2_l2_to_l3",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    params={
        "publication_date": "2026-11-02",
        "max_products": 1,
        "bbox": [14.0, 49.0, 25.0, 55.0],
        "grid_degrees": 0.1,
    },
    tags=["eo", "sentinel5p", "no2", "harp"],
)
def s5p_no2_l2_to_l3():
    @task(task_id="1_check_eodata", retries=0, execution_timeout=timedelta(minutes=3))
    def check_eodata():
        client, bucket = s3_connection("eodata_s3")
        client.list_objects_v2(Bucket=bucket, Prefix="Sentinel-5P/", MaxKeys=1)
        print(f"EODATA listing available: s3://{bucket}/Sentinel-5P/")
        return {"bucket": bucket}

    @task(task_id="2_find_data", retries=0, execution_timeout=timedelta(minutes=10))
    def find_data(access_check):
        from datetime import datetime, timezone
        from urllib.parse import urljoin

        import requests

        config = dict(get_current_context()["params"])
        start = datetime.strptime(config["publication_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        limit = int(config["max_products"])
        if not 1 <= limit <= 10:
            raise ValueError("For this test max_products must be between 1 and 10")
        harp_operations(config)
        query = (
            f"PublicationDate ge {start:%Y-%m-%dT00:00:00.000Z} and "
            f"PublicationDate lt {end:%Y-%m-%dT00:00:00.000Z} and Online eq true and "
            "Collection/Name eq 'SENTINEL-5P' and "
            "Attributes/OData.CSC.StringAttribute/any(a:a/Name eq 'productType' "
            "and a/Value eq 'L2__NO2___') and "
            "Attributes/OData.CSC.StringAttribute/any(a:a/Name eq 'processingMode' "
            "and a/Value eq 'OFFL')"
        )
        west, south, east, north = map(float, config["bbox"])
        polygon = f"{west} {south},{east} {south},{east} {north},{west} {north},{west} {south}"
        query += f" and OData.CSC.Intersects(area=geography'SRID=4326;POLYGON(({polygon}))')"
        url = CATALOGUE_URL
        params = {"$filter": query, "$top": limit, "$orderby": "PublicationDate asc"}
        products = []
        while url and len(products) < limit:
            response = requests.get(url, params=params, timeout=(15, 120))
            response.raise_for_status()
            page = response.json()
            products.extend({"Name": item["Name"], "S3Path": item["S3Path"]} for item in page["value"])
            next_page = page.get("@odata.nextLink")
            url = urljoin(response.url, next_page) if next_page else None
            params = None
        products = products[:limit]
        if not products:
            raise ValueError("No matching NO2 OFFL products. Change publication_date.")
        for product in products:
            print(f"Selected: {product['Name']}")
        return {"products": products, "config": config, "source_bucket": access_check["bucket"]}

    @task(task_id="3_process_harp", retries=0, execution_timeout=timedelta(hours=2))
    def process_harp(selection):
        import hashlib
        import json
        import subprocess
        import tempfile
        from pathlib import Path

        record_progress("task3_started")
        source, bucket = s3_connection("eodata_s3")
        output, output_bucket = s3_connection("s5p_output_s3")
        if bucket != selection["source_bucket"]:
            raise ValueError("Source bucket changed between tasks")
        run_id = get_current_context()["run_id"]
        run_tag = hashlib.sha256(run_id.encode()).hexdigest()[:16]
        operations = harp_operations(selection["config"])
        results = []
        import os
        scratch_root = Path(os.environ.get("S5P_WORK_DIR", "/home/airflow/.cache/s5p-work"))
        scratch_root.mkdir(parents=True, exist_ok=True)
        scratch_details = {"scratch_root": str(scratch_root)}
        candidates = []
        try:
            resolved = str(scratch_root.resolve())
            for line in Path("/proc/mounts").read_text().splitlines():
                fields = line.split()
                mountpoint = fields[1].replace("\\040", " ")
                if resolved == mountpoint or resolved.startswith(mountpoint.rstrip("/") + "/"):
                    candidates.append((len(mountpoint), mountpoint, fields[2]))
            if candidates:
                _, mountpoint, filesystem = max(candidates)
                scratch_details.update(mountpoint=mountpoint, filesystem=filesystem)
        except OSError:
            pass
        record_progress("scratch_ready", **scratch_details)
        if scratch_details.get("filesystem") in {"tmpfs", "ramfs"}:
            raise RuntimeError("Scratch is RAM-backed. Set S5P_WORK_DIR to a writable disk-backed location.")
        with tempfile.TemporaryDirectory(prefix="s5p_harp_", dir=str(scratch_root)) as temporary:
            work = Path(temporary)
            runner, environment = prepare_harp(work)
            for index, product in enumerate(selection["products"]):
                l2 = work / f"input_{index}.nc"
                l3 = work / f"output_{index}.nc"
                record_progress("downloading_l2", product=product["Name"])
                source_key = download_l2(source, bucket, product, l2)
                record_progress("processing_harp", product=product["Name"], l2_bytes=l2.stat().st_size)
                print(f"HARP processing: {product['Name']}", flush=True)
                conversion = subprocess.run(
                    runner + ["harpconvert", "-a", operations, "-f", "netcdf", str(l2), str(l3)],
                    env=environment, timeout=3600,
                )
                if conversion.returncode == 2:
                    raise ValueError("HARP selected no valid observations. Check the product, bbox and filters.")
                conversion.check_returncode()
                record_progress("validating_l3", output_bytes=l3.stat().st_size)
                validation_code = (
                    "import harp,numpy as np,json,sys; "
                    "p=harp.import_product(sys.argv[1]); "
                    f"v=p.{NO2_VARIABLE}; "
                    "a=np.asarray(v.data); n=int(np.isfinite(a).sum()); "
                    "assert n>0, 'No finite NO2 cells'; "
                    "print(json.dumps({'shape':list(a.shape),'valid_cells':n,'unit':str(v.unit)}))"
                )
                validation = subprocess.run(
                    runner + ["python", "-c", validation_code, str(l3)], env=environment,
                    check=True, capture_output=True, text=True, timeout=180,
                )
                summary = json.loads(validation.stdout.strip().splitlines()[-1])
                print(f"L3 validation: {summary}", flush=True)
                stem = Path(product["Name"]).stem.replace("_L2__", "_L3__", 1)
                stage_key = f"_staging/s5p_no2/{run_tag}/{stem}.nc"
                record_progress("uploading_l3", summary=summary, output_bytes=l3.stat().st_size)
                output.upload_file(str(l3), output_bucket, stage_key, Config=transfer_config())
                record_progress("l3_staged", stage_key=stage_key)
                results.append({"stage_key": stage_key, "filename": f"{stem}.nc",
                                "source_key": source_key, "size": l3.stat().st_size,
                                "summary": summary})
                l2.unlink()
                l3.unlink()
        record_progress("task3_complete", product_count=len(results))
        return {"results": results, "output_bucket": output_bucket, "run_tag": run_tag,
                "config": selection["config"], "operations": operations}

    @task(task_id="4_publish_output", retries=0, execution_timeout=timedelta(minutes=15))
    def publish_output(processed):
        import json

        record_progress("publishing_output")
        output, bucket = s3_connection("s5p_output_s3")
        if bucket != processed["output_bucket"]:
            raise ValueError("Output bucket changed between tasks")
        prefix = f"s5p_no2/{processed['config']['publication_date']}/{processed['run_tag']}"
        published = []
        for result in processed["results"]:
            final_key = f"{prefix}/{result['filename']}"
            output.copy_object(Bucket=bucket, Key=final_key,
                               CopySource={"Bucket": bucket, "Key": result["stage_key"]})
            info = output.head_object(Bucket=bucket, Key=final_key)
            if info["ContentLength"] != result["size"]:
                raise ValueError(f"Output size mismatch: {final_key}")
            record = {key: value for key, value in result.items() if key != "stage_key"}
            record["output_uri"] = f"s3://{bucket}/{final_key}"
            published.append(record)
            print(f"Published L3: {record['output_uri']}")
        manifest = {"run_id": get_current_context()["run_id"],
                    "config": processed["config"], "operations": processed["operations"],
                    "products": published,
                    "note": "One gridded L3 file per input orbit; not a daily composite."}
        manifest_key = f"{prefix}/manifest.json"
        output.put_object(Bucket=bucket, Key=manifest_key,
                          Body=json.dumps(manifest, indent=2).encode(), ContentType="application/json")
        print(f"Manifest: s3://{bucket}/{manifest_key}")
        for result in processed["results"]:
            output.delete_object(Bucket=bucket, Key=result["stage_key"])
        record_progress("finished", manifest=f"s3://{bucket}/{manifest_key}")
        return {"manifest": f"s3://{bucket}/{manifest_key}", "outputs": published}

    publish_output(process_harp(find_data(check_eodata())))


s5p_no2_l2_to_l3()
