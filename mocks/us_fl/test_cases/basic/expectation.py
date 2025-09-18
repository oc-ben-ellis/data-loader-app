import os
from pathlib import Path

import boto3


def _s3_client(endpoint: str):
    return boto3.client(
        "s3",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-west-2"),
        endpoint_url=endpoint,
    )


def _sqs_client(endpoint: str):
    return boto3.client(
        "sqs",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-west-2"),
        endpoint_url=endpoint,
    )


def _list_keys(s3, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []) or []:
            keys.append(item["Key"])  # type: ignore[index]
    return keys


def _assert_s3_objects(endpoint: str, registry_id: str) -> None:
    bucket = "oc-local-data-pipeline"
    s3 = _s3_client(endpoint)
    base_prefix = f"raw/{registry_id}/data/"
    keys = _list_keys(s3, bucket, base_prefix)
    if not keys:
        raise AssertionError(f"No raw objects found under s3://{bucket}/{base_prefix}")

    completed = [k for k in keys if k.endswith("metadata/_completed.json")]
    if not completed:
        raise AssertionError("No completed bundle metadata found in raw stage")

    bundle_meta_prefix = completed[0].rsplit("metadata/_completed.json", 1)[0]
    manifest_key = f"{bundle_meta_prefix}_manifest.jsonl"
    if manifest_key not in keys:
        keys = _list_keys(s3, bucket, base_prefix)
        if manifest_key not in keys:
            raise AssertionError("_manifest.jsonl missing for completed bundle")

    content_prefix = bundle_meta_prefix.replace("metadata/", "content/")
    content_keys = [
        k for k in keys if k.startswith(content_prefix) and not k.endswith("/")
    ]
    if not content_keys:
        raise AssertionError("No bundle content objects found in raw stage")

    bundle_hashes_prefix = f"raw/{registry_id}/bundle_hashes/"
    hash_keys = _list_keys(s3, bucket, bundle_hashes_prefix)
    if not any(k.endswith("_latest") for k in hash_keys):
        raise AssertionError("bundle_hashes/_latest not found")


def _assert_sqs_message(endpoint: str) -> None:
    sqs = _sqs_client(endpoint)
    # Match docker-compose default queue URL
    queue_url = f"{endpoint}/000000000000/data-pipeline-orchestration-queue"
    try:
        resp = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=1
        )
    except Exception as e:
        raise AssertionError(f"Failed to receive message from SQS: {e}")
    msgs = resp.get("Messages", [])
    if not msgs:
        raise AssertionError("Expected at least one SQS message in orchestration queue")


def _assert_sftp_files_intact() -> None:
    # Verify that input files still exist on the host-mounted directory used by sftp container
    env_dir = Path(__file__).resolve().parents[2] / "environment"
    host_data_dir = env_dir / "data" / "doc"
    expected_paths = [
        host_data_dir / "cor" / "20250829c.txt",
        host_data_dir / "cor" / "20250913c.txt",
        host_data_dir / "cor" / "20250915c.txt",
        host_data_dir / "Quarterly" / "Cor" / "cordata.zip",
    ]
    for p in expected_paths:
        if not p.exists():
            raise AssertionError(f"Expected SFTP file still present: {p}")


def main() -> None:
    endpoint = os.environ["LOCALSTACK_ENDPOINT"]
    registry_id = os.environ["DATA_REGISTRY_ID"]

    _assert_s3_objects(endpoint, registry_id)
    _assert_sqs_message(endpoint)
    _assert_sftp_files_intact()


if __name__ == "__main__":
    main()
