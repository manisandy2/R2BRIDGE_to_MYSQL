from fastapi import APIRouter,Query,HTTPException
from pyiceberg.exceptions import NoSuchTableError
from ...core.catalog_client import get_catalog_client
# from core.catalog_client import security,verify_jwt
# from fastapi.security import HTTPAuthorizationCredentials

router = APIRouter(prefix="", tags=["Transaction Tables"])


@router.get("/table/list")
def get_tables(
        namespace: str = Query(..., description="Namespace to list tables from"),
        # credentials: HTTPAuthorizationCredentials = Depends(security)
):
    # verify_jwt(credentials.credentials)
    try:
        catalog = get_catalog_client()
        tables = catalog.list_tables(namespace)

        if tables:
            return {"namespace": namespace, "tables": tables}
        else:
            return {"namespace": namespace, "tables": [], "message": "No tables found."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list tables in namespace '{namespace}': {str(e)}")

@router.post("/table/rename")
def rename_table(
    namespace: str = Query(..., description="Namespace containing the table"),
    old_table_name: str = Query(..., description="Current table name (e.g. 'transactions')"),
    new_table_name: str = Query(..., description="New table name (e.g. 'transactions_v2')"),
    # credentials: HTTPAuthorizationCredentials = Depends(security)
):
    # Step 1: Verify JWT
    # try:
    #     # verify_jwt(credentials.credentials)
    # except Exception as e:
    #     raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")

    catalog = get_catalog_client()
    try:
        # Step 2: Build full table identifiers
        old_identifier = f"{namespace}.{old_table_name}"
        new_identifier = f"{namespace}.{new_table_name}"

        # Step 3: Rename the table
        catalog.rename_table(old_identifier, new_identifier)

        return {
            "status": "success",
            "message": f"Table renamed from '{old_table_name}' to '{new_table_name}' in namespace '{namespace}' successfully."
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to rename table '{old_table_name}' in namespace '{namespace}': {str(e)}")

    finally:
        try:
            catalog.close()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to close catalog: {str(e)}")
            # pass


@router.delete("/table/delete")
def delete_table(
    namespace: str = Query(..., description="Namespace of the table"),
    table_name: str = Query(..., description="Name of the table to drop"),
    # credentials: HTTPAuthorizationCredentials = Depends(security)
):
    # verify_jwt(credentials.credentials)
    catalog = get_catalog_client()
    full_table_name = f"{namespace}.{table_name}"

    try:
        catalog.drop_table(full_table_name)
        return {"message": f"Table '{full_table_name}' dropped successfully."}

    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table '{full_table_name}' does not exist.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to drop table '{full_table_name}': {str(e)}")
