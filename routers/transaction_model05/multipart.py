from fastapi import APIRouter, HTTPException, Query
from datetime import datetime
from typing import Any, List
import time, math, json, asyncio, aioboto3
from ...mysql_creds import *
from botocore.client import Config
import time, json, boto3, os
import os
from ...core.r2_client import get_r2_client


router = APIRouter(prefix="", tags=["Multipart"])


@router.post("/multipart/abort-all")
def abort_all_multipart_uploads(bucket_name: str):
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("ENDPOINT"),
        aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4"),
    )

    paginator = s3.get_paginator("list_multipart_uploads")

    aborted = []
    total = 0

    for page in paginator.paginate(Bucket=bucket_name):

        uploads = page.get("Uploads", [])
        for u in uploads:
            key = u["Key"]
            upload_id = u["UploadId"]
            s3.abort_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id
            )
            total += 1
            aborted.append({"key": key, "upload_id": upload_id})

    return {
        "bucket": bucket_name,
        "aborted_uploads": total,
        "details": aborted
    }

@router.delete("/multipart/abort-all")
def abort_all_multipart_uploads(bucket_name: str):
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("ENDPOINT"),
        aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4"),
    )

    paginator = s3.get_paginator("list_multipart_uploads")

    aborted = []
    total = 0

    for page in paginator.paginate(Bucket=bucket_name):
        uploads = page.get("Uploads", [])
        for u in uploads:
            key = u["Key"]
            upload_id = u["UploadId"]

            # abort incomplete upload
            s3.abort_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id
            )

            # delete any partial object if exists
            try:
                s3.delete_object(Bucket=bucket_name, Key=key)
            except Exception as delete_err:
                print(f"Failed to delete key: {key} - {delete_err}")
                pass

            total += 1
            aborted.append({"key": key, "upload_id": upload_id})

    return {
        "bucket": bucket_name,
        "aborted_uploads": total,
        "status": "multipart aborted + objects deleted",
        "details": aborted
    }