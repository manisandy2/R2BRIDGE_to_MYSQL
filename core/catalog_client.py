
from dotenv import load_dotenv
import os
from pyiceberg.catalog.rest import RestCatalog
import logging
from fastapi import HTTPException

load_dotenv()
logger = logging.getLogger(__name__)


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
def get_catalog_client():
    try:
        return Creds().catalog_valid()
    except Exception as e:
        logger.error(f"Failed to initialize Iceberg catalog client: {e}")
        raise HTTPException(status_code=500, detail="Cloudflare R2 client initialization failed")