
from fastapi import APIRouter,HTTPException,Query
from ...core.catalog_client import get_catalog_client
import logging
from pyiceberg.exceptions import NamespaceAlreadyExistsError,NoSuchNamespaceError

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/namespaces", tags=["Transaction Namespaces"])

@router.get("/list")
def list_namespaces():
    catalog = get_catalog_client()
    try:
        namespaces = catalog.list_namespaces()
        logger.info("Fetched namespaces successfully.")
        return {"status": "success", "data": namespaces}
    except Exception as e:
        logger.error(f"Failed to list namespaces: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list namespaces: {str(e)}")
    finally:
        try:
            catalog.close()
        # except Exception:
        #     pass
        except Exception as close_err:
            logger.warning(f"Failed to close catalog: {close_err}")

# new version
# @router.get("/list")
# def list_namespaces():
#     catalog = None
#     try:
#         catalog = get_catalog_client()
#         raw = catalog.list_namespaces()
#         namespaces = [".".join(ns) for ns in raw]     # flatten tuple to string
#         logger.info("Fetched namespaces successfully.")
#         return {"status": "success", "data": namespaces}
#     except Exception as e:
#         logger.error(f"Failed to list namespaces: {e}")
#         raise HTTPException(status_code=500, detail=f"Failed to list namespaces: {str(e)}")
#     finally:
#         if catalog:
#             try:
#                 catalog.close()
#             except Exception as close_err:
#                 logger.warning(f"Failed to close catalog: {close_err}")



@router.post("/create")
def create_namespace(namespace: str = Query(..., description="Namespace (e.g. 'transaction')")):
    catalog = get_catalog_client()
    try:
        catalog.create_namespace(namespace)
        logger.info(f"Namespace '{namespace}' created successfully.")
        return {"status": "success", "message": f"Namespace '{namespace}' created successfully."}
    except NamespaceAlreadyExistsError:
        logger.warning(f"Namespace '{namespace}' already exists.")
        raise HTTPException(status_code=409, detail=f"Namespace '{namespace}' already exists.")
    except Exception as e:
        logger.error(f"Failed to create namespace '{namespace}': {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create namespace '{namespace}': {str(e)}")
    finally:
        try:
            catalog.close()
        except Exception as close_err:
            logger.warning(f"Failed to close catalog: {close_err}")
            # pass

@router.delete("/delete")
def delete_namespace(namespace: str = Query(..., description="Namespace to delete")):
    catalog = get_catalog_client()
    try:
        catalog.drop_namespace(namespace)
        logger.info(f"Namespace '{namespace}' deleted successfully.")
        return {"status": "success", "message": f"Namespace '{namespace}' deleted successfully."}
    except NoSuchNamespaceError:
        logger.warning(f"Namespace '{namespace}' does not exist.")
        raise HTTPException(status_code=404, detail=f"Namespace '{namespace}' does not exist.")
    except Exception as e:
        logger.error(f"Failed to delete namespace '{namespace}': {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete namespace '{namespace}': {str(e)}")
    finally:
        try:
            catalog.close()
        except Exception as close_err:
            logger.warning(f"Failed to close catalog: {close_err}")
            # pass