import hashlib
import json
import logging
import math
import os
import re
import subprocess
import tarfile
import tempfile

from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

import pendulum

from airflow.sdk import Connection, dag, get_current_context, task


LOG = logging.getLogger(__name__)

CATALOGUE_URL = "https://datahub.creodias.eu/odata/v1/Products"

HARP_ENVIRONMENT_KEY = (
    "environments/harp-runtime-linux64-v1.tar.gz"
)

NO2 = "tropospheric_NO2_column_number_density"

REDUCE_OPERATIONS = ";".join([
    "squash(time, (latitude, longitude, latitude_bounds, longitude_bounds))",
    "bin()",
])

PRODUCT_PATTERN = re.compile(
    r"^S5P_OFFL_L2__NO2____"
    r"(\d{8}T\d{6})_(\d{8}T\d{6})_(\d{5})_"
)


def s3_connection(connection_id):
    import boto3

    from boto3.s3.transfer import TransferConfig
    from botocore.config import Config

    connection = Connection.get(connection_id)
    extra = connection.extra_dejson

    if not connection.login or not connection.password:
        raise ValueError(
            f"Missing access key or secret in Connection {connection_id}"
        )

    endpoint = extra.get("endpoint_url") or connection.host
    bucket = extra.get("bucket_name")

    if not endpoint or not bucket:
        raise ValueError(
            f"Set endpoint_url and bucket_name in Connection {connection_id}"
        )

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        region_name=extra.get("region_name", "us-east-1"),
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=30,
            read_timeout=300,
            retries={
                "max_attempts": 5,
                "mode": "standard",
            },
        ),
    )

    transfer = TransferConfig(
        max_concurrency=1,
        use_threads=False,
        max_io_queue=4,
    )

    return client, bucket, transfer


def run_command(
    command,
    environment,
    timeout=3600,
    allow_empty=False,
):
    result = subprocess.run(
        [str(value) for value in command],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
    )

    if result.stdout:
        LOG.info("%s", result.stdout.rstrip())

    if allow_empty and result.returncode == 2:
        return 2

    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed with exit code {result.returncode}: "
            f"{command[0]}"
        )

    return 0


def prepare_harp(work, client, bucket, transfer):
    archive = work / "harp-runtime.tar.gz"
    prefix = work / "harp-runtime"

    LOG.info(
        "Downloading HARP environment: s3://%s/%s",
        bucket,
        HARP_ENVIRONMENT_KEY,
    )

    client.download_file(
        bucket,
        HARP_ENVIRONMENT_KEY,
        str(archive),
        Config=transfer,
    )

    prefix.mkdir()

    LOG.info("Extracting HARP environment")

    with tarfile.open(archive, "r:gz") as package:
        package.extractall(prefix, filter="data")

    archive.unlink()

    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in ("PYTHONPATH", "PYTHONHOME")
        and not key.startswith("CONDA")
    }

    environment.update({
        "PYTHONNOUSERSITE": "1",
        "OMP_NUM_THREADS": "1",
        "OPENBLAS_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1",
        "NUMEXPR_NUM_THREADS": "1",
    })

    LOG.info("Relocating HARP environment")

    run_command(
        [
            prefix / "bin/python",
            prefix / "bin/conda-unpack",
        ],
        environment,
        timeout=600,
    )

    runner = [
        "bash",
        "-c",
        'set -e; source "$1/bin/activate"; shift; exec "$@"',
        "harp-runtime",
        str(prefix),
    ]

    run_command(
        runner + ["harpconvert", "-v"],
        environment,
        timeout=60,
    )

    return runner, environment


def download_product(
    client,
    bucket,
    transfer,
    product,
    destination,
):
    from botocore.exceptions import ClientError

    prefix = product["S3Path"]

    if prefix.startswith("s3://"):
        prefix = prefix[5:].split("/", 1)[1]

    prefix = prefix.lstrip("/")

    if prefix.startswith(bucket + "/"):
        prefix = prefix[len(bucket) + 1:]

    prefix = prefix.rstrip("/")
    name = product["Name"]

    candidates = (
        [prefix]
        if prefix.endswith(".nc")
        else [f"{prefix}/{name}", prefix]
    )

    for key in candidates:
        try:
            client.download_file(
                bucket,
                key,
                str(destination),
                Config=transfer,
            )

            LOG.info("Downloaded: s3://%s/%s", bucket, key)
            return key

        except ClientError as error:
            code = str(error.response["Error"]["Code"])

            if code not in ("404", "NoSuchKey", "NotFound"):
                raise

    raise FileNotFoundError(
        f"Cannot download product: {name}"
    )


@dag(
    dag_id="s5p_no2_weekly",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    params={
        "week_start": "2026-10-19",
        "bbox": [14.0, 49.0, 25.0, 55.0],
        "grid_degrees": 0.1,
        "qa_threshold": 75,
    },
    tags=["Sentinel-5P", "NO2", "HARP", "weekly"],
)
def s5p_no2_weekly():

    @task(
        task_id="1_check_eodata",
        retries=0,
        execution_timeout=timedelta(minutes=5),
    )
    def check_eodata():
        client, bucket, _ = s3_connection("eodata_s3")

        client.list_objects_v2(
            Bucket=bucket,
            Prefix="Sentinel-5P/",
            MaxKeys=1,
        )

        LOG.info("EODATA connection works: %s", bucket)
        return bucket

    @task(
        task_id="2_find_data",
        retries=0,
        execution_timeout=timedelta(minutes=15),
    )
    def find_data(eodata_bucket):
        parameters = get_current_context()["params"]

        period_start = datetime.strptime(
            parameters["week_start"],
            "%Y-%m-%d",
        ).replace(tzinfo=timezone.utc)

        period_end = period_start + timedelta(days=7)
        last_day = period_end - timedelta(days=1)

        west, south, east, north = map(
            float, parameters["bbox"]
        )

        step = float(parameters["grid_degrees"])
        qa = float(parameters["qa_threshold"])

        if not (-180 <= west < east <= 180):
            raise ValueError("Invalid longitude bounds")

        if not (-90 <= south < north <= 90):
            raise ValueError("Invalid latitude bounds")

        if not math.isfinite(step) or step <= 0:
            raise ValueError("grid_degrees must be positive")

        if not 0 <= qa <= 100:
            raise ValueError(
                "qa_threshold must be between 0 and 100"
            )

        for extent in (east - west, north - south):
            if not math.isclose(
                extent / step,
                round(extent / step),
                rel_tol=0,
                abs_tol=1e-6,
            ):
                raise ValueError(
                    "bbox dimensions must be divisible "
                    "by grid_degrees"
                )

        start = period_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        end = period_end.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Select all available OFFL orbits overlapping
        # the seven-day observation period.
        query = {
            "$filter": (
                "Online eq true"
                " and startswith(Name,'S5P_OFFL_L2__NO2____')"
                f" and ContentDate/Start lt {end}"
                f" and ContentDate/End gt {start}"
            ),
            "$select": "Id,Name,S3Path,ContentDate",
            "$orderby": "ContentDate/Start asc,Name asc",
            "$top": 1000,
        }

        url = CATALOGUE_URL + "?" + urlencode(query)
        products = []

        while url:
            with urlopen(url, timeout=120) as response:
                page = json.load(response)

            products.extend(page.get("value", []))
            url = page.get("@odata.nextLink")

        # Keep one processing version per orbit
        # and sensing interval.
        unique = {}

        for product in products:
            match = PRODUCT_PATTERN.match(product["Name"])

            if not match:
                continue

            orbit_key = match.groups()
            previous = unique.get(orbit_key)

            if (
                previous is None
                or product["Name"] > previous["Name"]
            ):
                unique[orbit_key] = product

        products = sorted(
            unique.values(),
            key=lambda product: product["Name"],
        )

        if not products:
            raise ValueError(
                "No available OFFL NO2 products for "
                f"{period_start.date()}–{last_day.date()}"
            )

        LOG.info(
            "Observation period: %s to %s UTC, inclusive",
            period_start.date(),
            last_day.date(),
        )

        LOG.info("Selected orbit products: %s", len(products))

        for product in products:
            LOG.info("Selected: %s", product["Name"])

        return {
            "week_start": period_start.strftime("%Y-%m-%d"),
            "week_end": last_day.strftime("%Y-%m-%d"),
            "period_end_exclusive": period_end.strftime("%Y-%m-%d"),
            "bbox": [west, south, east, north],
            "grid_degrees": step,
            "qa_threshold": qa,
            "eodata_bucket": eodata_bucket,
            "products": products,
        }

    @task(
        task_id="3_process_harp",
        retries=0,
        execution_timeout=timedelta(hours=12),
    )
    def process_harp(selection):
        source, source_bucket, source_transfer = s3_connection(
            "eodata_s3"
        )

        output, output_bucket, output_transfer = s3_connection(
            "s5p_output_s3"
        )

        context = get_current_context()
        run_id = context["run_id"]

        run_tag = hashlib.sha256(
            run_id.encode()
        ).hexdigest()[:16]

        week_start = selection["week_start"]
        week_end = selection["week_end"]

        period_start = datetime.strptime(
            week_start, "%Y-%m-%d"
        ).replace(tzinfo=timezone.utc)

        period_end = period_start + timedelta(days=7)

        epoch = datetime(2000, 1, 1, tzinfo=timezone.utc)

        start_seconds = int(
            (period_start - epoch).total_seconds()
        )

        end_seconds = int(
            (period_end - epoch).total_seconds()
        )

        west, south, east, north = selection["bbox"]
        step = selection["grid_degrees"]
        qa = selection["qa_threshold"]

        latitude_edges = round((north - south) / step) + 1
        longitude_edges = round((east - west) / step) + 1

        # Filter individual observations to the exact
        # seven-day UTC interval before spatial binning.
        operations = ";".join([
            (
                "keep(datetime_start,latitude,longitude,"
                f"latitude_bounds,longitude_bounds,{NO2},"
                f"{NO2}_validity)"
            ),
            (
                f"datetime_start>={start_seconds} "
                "[s since 2000-01-01]"
            ),
            (
                f"datetime_start<{end_seconds} "
                "[s since 2000-01-01]"
            ),
            f"{NO2}_validity>{qa}",
            f"valid({NO2})",
            (
                "keep(latitude,longitude,"
                f"latitude_bounds,longitude_bounds,{NO2})"
            ),
            f"derive({NO2} [Pmolec/cm2])",
            (
                f"bin_spatial("
                f"{latitude_edges},{south},{step},"
                f"{longitude_edges},{west},{step})"
            ),
            "derive(latitude {latitude})",
            "derive(longitude {longitude})",
        ])

        LOG.info(
            "Weekly processing: %s to %s UTC, inclusive",
            week_start,
            week_end,
        )

        LOG.info("HARP operations: %s", operations)
        LOG.info("HARP reduce operations: %s", REDUCE_OPERATIONS)

        scratch = Path(
            os.environ.get(
                "S5P_WORK_DIR",
                "/home/airflow/.cache/s5p-work",
            )
        )

        scratch.mkdir(parents=True, exist_ok=True)

        used = []
        skipped = []

        with tempfile.TemporaryDirectory(dir=scratch) as temporary:
            work = Path(temporary)

            runner, environment = prepare_harp(
                work,
                output,
                output_bucket,
                output_transfer,
            )

            weekly = work / "weekly.nc"

            # Process one orbit at a time, retaining
            # only the current orbit and running mean.
            for index, product in enumerate(
                selection["products"], start=1
            ):
                LOG.info(
                    "Processing orbit %s/%s: %s",
                    index,
                    len(selection["products"]),
                    product["Name"],
                )

                l2 = work / "input.nc"
                orbit_l3 = work / "orbit.nc"

                source_key = download_product(
                    source,
                    source_bucket,
                    source_transfer,
                    product,
                    l2,
                )

                try:
                    status = run_command(
                        runner + [
                            "harpconvert",
                            "-a", operations,
                            "-f", "netcdf",
                            l2,
                            orbit_l3,
                        ],
                        environment,
                        allow_empty=True,
                    )
                finally:
                    l2.unlink(missing_ok=True)

                if status == 2:
                    LOG.info(
                        "Skipping empty orbit: %s",
                        product["Name"],
                    )

                    skipped.append(product["Name"])
                    orbit_l3.unlink(missing_ok=True)
                    continue

                if not weekly.exists():
                    orbit_l3.replace(weekly)

                else:
                    merged = work / "merged.nc"

                    run_command(
                        runner + [
                            "harpmerge",
                            "-ar", REDUCE_OPERATIONS,
                            "-f", "netcdf",
                            weekly,
                            orbit_l3,
                            merged,
                        ],
                        environment,
                    )

                    merged.replace(weekly)
                    orbit_l3.unlink()

                used.append({
                    "name": product["Name"],
                    "source_key": source_key,
                })

                LOG.info(
                    "Weekly mean includes %s non-empty orbit products",
                    len(used),
                )

            if not used:
                raise ValueError(
                    "No valid observations for this "
                    "week, bbox and QA threshold"
                )

            summary_file = work / "summary.json"

            validation_code = """
import json
import sys

import harp
import numpy as np

product = harp.import_product(sys.argv[1])
variable = product.tropospheric_NO2_column_number_density

values = variable.data
valid_cells = int(np.isfinite(values).sum())

if valid_cells == 0:
    raise ValueError(
        "Weekly result contains no finite NO2 values"
    )

summary = {
    "unit": variable.unit,
    "shape": list(values.shape),
    "valid_cells": valid_cells,
}

with open(sys.argv[2], "w") as stream:
    json.dump(summary, stream)
"""

            run_command(
                runner + [
                    "python",
                    "-c", validation_code,
                    weekly,
                    summary_file,
                ],
                environment,
                timeout=300,
            )

            summary = json.loads(summary_file.read_text())

            filename = (
                "S5P_OFFL_L3__NO2___WEEKLY_"
                f"{week_start.replace('-', '')}_"
                f"{week_end.replace('-', '')}.nc"
            )

            stage_key = (
                f"_staging/s5p_no2_weekly/{run_tag}/{filename}"
            )

            LOG.info(
                "Uploading weekly mean: s3://%s/%s",
                output_bucket,
                stage_key,
            )

            output.upload_file(
                str(weekly),
                output_bucket,
                stage_key,
                Config=output_transfer,
            )

            size = weekly.stat().st_size

            # Keep the full input inventory in S3
            # instead of passing it through XCom.
            manifest = {
                "run_id": run_id,
                "run_tag": run_tag,
                "week_start": week_start,
                "week_end": week_end,
                "period_end_exclusive": selection[
                    "period_end_exclusive"
                ],
                "timezone": "UTC",
                "bbox": selection["bbox"],
                "grid_degrees": step,
                "qa_threshold": qa,
                "operations": operations,
                "reduce_operations": REDUCE_OPERATIONS,
                "aggregation": (
                    "Observation-weighted weekly mean "
                    "using HARP spatial weights and bin()"
                ),
                "selected_orbits": len(selection["products"]),
                "used_orbits": used,
                "empty_orbits": skipped,
                "summary": summary,
                "filename": filename,
                "size": size,
            }

            stage_manifest_key = (
                f"_staging/s5p_no2_weekly/{run_tag}/manifest.json"
            )

            output.put_object(
                Bucket=output_bucket,
                Key=stage_manifest_key,
                Body=json.dumps(manifest, indent=2).encode(),
                ContentType="application/json",
            )

        return {
            "run_tag": run_tag,
            "week_start": week_start,
            "week_end": week_end,
            "filename": filename,
            "stage_key": stage_key,
            "stage_manifest_key": stage_manifest_key,
            "size": size,
            "used_orbits": len(used),
            "empty_orbits": len(skipped),
            "summary": summary,
        }

    @task(
        task_id="4_publish_output",
        retries=0,
        execution_timeout=timedelta(minutes=15),
    )
    def publish_output(result):
        client, bucket, _ = s3_connection("s5p_output_s3")

        prefix = (
            "s5p_no2_weekly/"
            f"{result['week_start']}_{result['week_end']}/"
            f"{result['run_tag']}"
        )

        output_key = f"{prefix}/{result['filename']}"
        manifest_key = f"{prefix}/manifest.json"

        client.copy_object(
            Bucket=bucket,
            Key=output_key,
            CopySource={
                "Bucket": bucket,
                "Key": result["stage_key"],
            },
        )

        stored = client.head_object(
            Bucket=bucket,
            Key=output_key,
        )

        if stored["ContentLength"] != result["size"]:
            raise ValueError("Published file size mismatch")

        response = client.get_object(
            Bucket=bucket,
            Key=result["stage_manifest_key"],
        )

        try:
            manifest = json.loads(response["Body"].read())
        finally:
            response["Body"].close()

        output_uri = f"s3://{bucket}/{output_key}"

        manifest["output_uri"] = output_uri
        manifest["created_at"] = datetime.now(
            timezone.utc
        ).isoformat()

        client.put_object(
            Bucket=bucket,
            Key=manifest_key,
            Body=json.dumps(manifest, indent=2).encode(),
            ContentType="application/json",
        )

        for key in (
            result["stage_key"],
            result["stage_manifest_key"],
        ):
            client.delete_object(
                Bucket=bucket,
                Key=key,
            )

        LOG.info("Published weekly L3: %s", output_uri)
        LOG.info(
            "Manifest: s3://%s/%s",
            bucket,
            manifest_key,
        )
        LOG.info(
            "Used orbit products: %s",
            result["used_orbits"],
        )
        LOG.info(
            "Empty orbit products: %s",
            result["empty_orbits"],
        )
        LOG.info("Summary: %s", result["summary"])

        return {
            "output": output_uri,
            "manifest": f"s3://{bucket}/{manifest_key}",
            "used_orbits": result["used_orbits"],
            "summary": result["summary"],
        }

    checked = check_eodata()
    selected = find_data(checked)
    processed = process_harp(selected)
    publish_output(processed)


s5p_no2_weekly()
