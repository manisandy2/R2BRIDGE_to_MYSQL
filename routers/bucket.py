from fastapi import APIRouter, Query, Body, HTTPException,UploadFile, File
from botocore.exceptions import ClientError
# import logging
import json
from ..core.r2_client import get_r2_client

# logger = logging.getLogger(__name__)
router = APIRouter(prefix="/bucket", tags=["R2 Bucket"])

@router.get("/list-buckets")
def list_buckets():
    r2_client = get_r2_client()
    try:
        response = r2_client.list_buckets()
        buckets = [b["Name"] for b in response["Buckets"]]
        return {"buckets": buckets}
    except ClientError as e:
        return {"error": str(e)}

@router.post("/create-bucket")
def create_bucket(bucket_name: str = Query(..., description="Name of the new R2 bucket")):
    r2_client = get_r2_client()
    try:
        r2_client.create_bucket(Bucket=bucket_name)
        return {"message": f"✅ Bucket '{bucket_name}' created successfully"}
    except ClientError as e:
        return {"error": str(e)}

@router.post("/upload-object")
async def upload_object(
    bucket_name: str = Query(..., description="Bucket name"),
    object_key: str = Query(..., description="Object key (filename in bucket)"),
    file: UploadFile = File(...)
):
    """Upload or update (overwrite) an object"""
    r2_client = get_r2_client()
    try:
        file_content = await file.read()
        r2_client.put_object(Bucket=bucket_name, Key=object_key, Body=file_content)
        return {"message": f"✅ Object '{object_key}' uploaded to bucket '{bucket_name}'"}
    except ClientError as e:

        raise HTTPException(status_code=400, detail=f"Upload failed: {e.response['Error']['Message']}")


@router.delete("/delete-folder")
def delete_folder(
        bucket_name: str = Query(..., description="Bucket name"),
        folder_path: str = Query(..., description="Folder path to delete (will remove all objects inside)")
):
    """Delete a folder (all objects with that prefix)"""
    r2_client = get_r2_client()
    try:
        response = r2_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
        if "Contents" not in response:
            return {"message": f"⚠️ No objects found in folder '{folder_path}'"}

        objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
        r2_client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects_to_delete})
        return {"message": f" Folder '{folder_path}' deleted with {len(objects_to_delete)} objects"}
    except ClientError as e:
        # logger.error(f"Delete folder failed: {e}")
        raise HTTPException(status_code=400, detail=f"Delete folder failed: {e.response['Error']['Message']}")


@router.delete("/delete")
def delete_bucket(bucket_name: str = Query(..., description="Bucket name"),
                  force: bool = Query(False, description="Force delete non-empty bucket")):
    r2_client = get_r2_client()
    try:
        if force:
            # Delete all objects inside before removing bucket
            objects = r2_client.list_objects_v2(Bucket=bucket_name)
            if "Contents" in objects:
                to_delete = [{"Key": obj["Key"]} for obj in objects["Contents"]]
                r2_client.delete_objects(Bucket=bucket_name, Delete={"Objects": to_delete})

        # Try deleting the bucket
        r2_client.delete_bucket(Bucket=bucket_name)
        return {"message": f"🗑️ Bucket '{bucket_name}' deleted successfully"}
    except ClientError as e:
        # logger.error(f"Bucket deletion failed: {e}")
        raise HTTPException(status_code=400, detail=e.response["Error"]["Message"])

@router.post("/save")
def save_json(
    bucket_name: str = Query(..., description="Bucket name"),
    folder_path: str = Query("json_store/", description="Folder path inside bucket (default=json_store/)"),
    model: dict = Body(..., description="JSON data to save")
):

    r2_client = get_r2_client()

    try:
        # 1. List existing objects to find max serial number
        response = r2_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
        max_id = 0
        if "Contents" in response:
            for obj in response["Contents"]:
                filename = obj["Key"].split("/")[-1]
                if filename.endswith(".json") and filename[:-5].isdigit():
                    max_id = max(max_id, int(filename[:-5]))

        # 2. New serial number
        new_id = max_id + 1
        object_key = f"{folder_path.rstrip('/')}/{new_id}.json"

        # 3. Upload JSON
        r2_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=json.dumps(model).encode("utf-8"),
            ContentType="application/json"
        )

        return {"message": f"✅ JSON saved as {object_key}", "id": new_id, "data": model}

    except ClientError as e:
        # logger.error(f"Save JSON failed: {e}")
        raise HTTPException(status_code=400, detail=e.response["Error"]["Message"])