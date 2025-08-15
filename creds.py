
from dotenv import load_dotenv
import os
from pyiceberg.catalog.rest import RestCatalog
from botocore.client import Config
import boto3

load_dotenv()


class Creds:
    def __init__(self):
        self.CATALOG_URI = os.getenv("CATALOG_URI")
        self.WAREHOUSE = os.getenv("WAREHOUSE")
        self.TOKEN = os.getenv("TOKEN")
        self.CATALOG_NAME = os.getenv("CATALOG_NAME")

    def catalog_valid(self):
        if not all([self.CATALOG_URI, self.WAREHOUSE, self.TOKEN]):
            raise ValueError("Missing environment variables. Please check CATALOG_URI, WAREHOUSE, or TOKEN.")

        return RestCatalog(
            name=self.CATALOG_NAME,
            warehouse=self.WAREHOUSE,
            uri=self.CATALOG_URI,
            token=self.TOKEN
        )
# creds = Creds()
# creds.catalog_valid()
# print(creds.catalog_valid())

class CloudflareR2Creds:
    def __init__(self):
        self.ACCOUNT_ID = os.getenv("ACCOUNT_ID")
        self.ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")
        self.SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")
        self.BUCKET_NAME = os.getenv("BUCKET_NAME")
        self.ENDPOINT = os.getenv("ENDPOINT")
        self.client = None



    def get_client(self):
        if not self.client:
            if not all([self.ACCESS_KEY_ID, self.SECRET_ACCESS_KEY, self.ENDPOINT]):
                raise ValueError("Missing Cloudflare R2 environment variables.")
        self.client = boto3.client(
            "s3",
            endpoint_url=self.ENDPOINT,
            aws_access_key_id=self.ACCESS_KEY_ID,
            aws_secret_access_key=self.SECRET_ACCESS_KEY,
            config=Config(signature_version="s3v4"),
            region_name="auto"
        )
        return self.client

# creds = CloudflareR2Creds()
# print(creds.get_client().list_buckets())