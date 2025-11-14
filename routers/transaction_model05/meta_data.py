from fastapi import APIRouter, HTTPException
import boto3
import os
from ...core.r2_client import get_r2_client

router = APIRouter(prefix="", tags=["Metadata"])

R2_BUCKET_NAME = os.getenv("BUCKET_NAME")

@router.get("/metadata/list")
def list_iceberg_metadata():

    r2_client = get_r2_client()
    try:

        response = r2_client.list_objects_v2(
            Bucket=R2_BUCKET_NAME,
            Prefix="__r2_data_catalog/"
        )
        metadata_files = []
        if "Contents" in response:
            for obj in response["Contents"]:
                key = obj["Key"]
                if "/metadata/" in key:
                    metadata_files.append({
                        "key": key,
                        "size": obj["Size"],
                        "last_modified": obj["LastModified"].isoformat()
                    })

        return {
            "bucket": R2_BUCKET_NAME,
            "total_metadata_files": len(metadata_files),
            "files": metadata_files
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing metadata: {str(e)}")


@router.get("/metadata/uploads")
def list_incomplete_uploads():

    r2_client = get_r2_client()
    try:
        uploads = r2_client.list_multipart_uploads(Bucket=R2_BUCKET_NAME)
        active_uploads = []

        if "Uploads" in uploads:
            for u in uploads["Uploads"]:
                active_uploads.append({
                    "key": u["Key"],
                    "upload_id": u["UploadId"],
                    "initiated": u["Initiated"].isoformat()
                })

        return {
            "bucket": R2_BUCKET_NAME,
            "total_incomplete_uploads": len(active_uploads),
            "uploads": active_uploads
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing multipart uploads: {str(e)}")