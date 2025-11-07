
from fastapi import APIRouter,Query,HTTPException
from ...core.catalog_client import get_catalog_client
from pydantic import BaseModel
from pyiceberg.types import (
    IntegerType,
    StringType,
    DoubleType,
    BooleanType,
    TimestampType,
)
from typing import Optional

router = APIRouter(prefix="/columns", tags=["columns"])

import logging

logger = logging.getLogger(__name__)

def get_iceberg_type(type_str: str):
    type_str = type_str.lower()
    if type_str in ["int", "integer"]:
        return IntegerType()
    elif type_str in ["string", "str"]:
        return StringType()
    elif type_str in ["double", "float"]:
        return DoubleType()
    elif type_str in ["bool", "boolean"]:
        return BooleanType()
    elif type_str in ["timestamp", "datetime"]:
        return TimestampType()
    else:
        raise ValueError(f"Unsupported Iceberg type: {type_str}")

# @router.put("/update_column")
# def update_column(
#     column_name: str = Query(..., description="Column name to update"),
#     column_type: Optional[str] = Query(None, description="New column type (optional)"),
#     doc: Optional[str] = Query(None, description="New column description (optional)"),
#     namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
#     table_name: str = Query(..., description="Table name (e.g. 'Table name')")
# ):
#     if not column_type and not doc:
#         raise HTTPException(
#             status_code=400,
#             detail="No updates provided. You must specify either 'column_type' or 'doc'."
#         )
#
#     try:
#         catalog = get_catalog_client()
#         table = catalog.load_table(f"{namespace}.{table_name}")
#
#         with table.update_schema() as update:
#             updated = False
#
#             if column_type:
#                 iceberg_type = get_iceberg_type(column_type)
#                 update.update_column_type(column_name, iceberg_type)
#                 updated = True
#
#             if doc:
#                 update.update_column_doc(column_name, doc)
#                 updated = True
#
#             if not updated:
#                 raise HTTPException(status_code=400, detail="No updates provided. Specify column_type or doc.")
#
#         # Reload schema after update
#         table = catalog.load_table(f"{namespace}.{table_name}")
#         updated_field = next((f for f in table.schema().fields if f.name == column_name), None)
#
#         return {
#             "status": "success",
#             "message": f"Column '{column_name}' updated successfully.",
#             "updated_column": {
#                 "name": updated_field.name,
#                 "type": str(updated_field.field_type),
#                 "doc": updated_field.doc,
#             } if updated_field else None
#         }
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Update column failed: {e}")

# @router.put("/update_column")
# def update_column(
#     column_name: str = Query(..., description="Column name to update"),
#     column_type: Optional[str] = Query(None, description="New column type (optional)"),
#     doc: Optional[str] = Query(None, description="New column description (optional)"),
#     namespace: str = Query(..., description="Namespace (e.g., 'sales')"),
#     table_name: str = Query(..., description="Table name (e.g., 'transactions')")
# ):
#     """
#     Update a column's type or documentation in an Apache Iceberg table.
#     """
#
#     # --- Step 1: Validate request ---
#     if not column_type and not doc:
#         raise HTTPException(
#             status_code=400,
#             detail="No updates provided. You must specify either 'column_type' or 'doc'."
#         )
#
#     try:
#         # --- Step 2: Load catalog and table ---
#         catalog = get_catalog_client()
#         identifier = f"{namespace}.{table_name}"
#
#         try:
#             table = catalog.load_table(identifier)
#         except Exception as load_err:
#             logger.error(f"Failed to load table {identifier}: {load_err}")
#             raise HTTPException(
#                 status_code=404,
#                 detail=f"Table '{identifier}' not found in catalog."
#             )
#
#         # --- Step 3: Apply schema updates ---
#         with table.update_schema() as update:
#             if column_type:
#                 try:
#                     iceberg_type = get_iceberg_type(column_type)
#                     update.update_column_type(column_name, iceberg_type)
#                     logger.info(f"Updated column type for '{column_name}' → {column_type}")
#                 except Exception as type_err:
#                     raise HTTPException(
#                         status_code=400,
#                         detail=f"Invalid column type '{column_type}': {type_err}"
#                     )
#
#             if doc:
#                 update.update_column_doc(column_name, doc)
#                 logger.info(f"Updated column doc for '{column_name}' → {doc}")
#
#         # --- Step 4: Reload updated schema ---
#         table = catalog.load_table(identifier)
#         updated_field = next((f for f in table.schema().fields if f.name == column_name), None)
#
#         if not updated_field:
#             raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found after update.")
#
#         # --- Step 5: Success response ---
#         return {
#             "status": "success",
#             "message": f"Column '{column_name}' updated successfully in '{identifier}'.",
#             "updated_column": {
#                 "name": updated_field.name,
#                 "type": str(updated_field.field_type),
#                 "doc": updated_field.doc,
#             }
#         }
#
#     except HTTPException:
#         raise  # re-raise FastAPI HTTPExceptions untouched
#
#     except Exception as e:
#         logger.exception(f"Unexpected error updating column '{column_name}': {e}")
#         raise HTTPException(status_code=500, detail=f"Internal server error: {e}")
# @router.put("/update_column")
# def update_column(
#     column_name: str = Query(..., description="Existing column name to update"),
#     column_type: Optional[str] = Query(None, description="New column type (optional)"),
#     doc: Optional[str] = Query(None, description="New column description (optional)"),
#     new_column_name: Optional[str] = Query(None, description="If provided, a new column will be added using this name (optional)"),
#     namespace: str = Query(..., description="Namespace (e.g., 'sales')"),
#     table_name: str = Query(..., description="Table name (e.g., 'transactions')")
# ):
#     """
#     Update a column's type or documentation in an Apache Iceberg table.
#     Optionally add a new column (with a different name) before updating.
#     """
#
#     # --- Step 1: Validate request ---
#     if not any([column_type, doc, new_column_name]):
#         raise HTTPException(
#             status_code=400,
#             detail="No updates provided. You must specify 'column_type', 'doc', or 'new_column_name'."
#         )
#
#     try:
#         # --- Step 2: Load catalog and table ---
#         catalog = get_catalog_client()
#         identifier = f"{namespace}.{table_name}"
#
#         try:
#             table = catalog.load_table(identifier)
#         except Exception as load_err:
#             logger.error(f"Failed to load table {identifier}: {load_err}")
#             raise HTTPException(status_code=404, detail=f"Table '{identifier}' not found in catalog.")
#
#         # --- Step 3: Prepare schema update ---
#         with table.update_schema() as update:
#             # If new_column_name provided, add a new column
#             if new_column_name:
#                 old_field = next((f for f in table.schema().fields if f.name == column_name), None)
#                 if not old_field:
#                     raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found.")
#
#                 # Determine new column type and doc
#                 new_type = get_iceberg_type(column_type) if column_type else old_field.field_type
#                 new_doc = doc if doc else old_field.doc
#
#                 update.add_column(new_column_name, new_type, doc=new_doc)
#                 logger.info(f"Added new column '{new_column_name}' with type {new_type} and doc '{new_doc}'")
#
#             # Otherwise, just update existing column
#             else:
#                 if column_type:
#                     iceberg_type = get_iceberg_type(column_type)
#                     update.update_column_type(column_name, iceberg_type)
#                     logger.info(f"Updated column type for '{column_name}' → {column_type}")
#
#                 if doc:
#                     update.update_column_doc(column_name, doc)
#                     logger.info(f"Updated column doc for '{column_name}' → {doc}")
#
#         # --- Step 4: Reload updated schema ---
#         table = catalog.load_table(identifier)
#         updated_field_name = new_column_name if new_column_name else column_name
#         updated_field = next((f for f in table.schema().fields if f.name == updated_field_name), None)
#
#         if not updated_field:
#             raise HTTPException(status_code=404, detail=f"Column '{updated_field_name}' not found after update.")
#
#         # --- Step 5: Success response ---
#         return {
#             "status": "success",
#             "message": f"Column '{updated_field_name}' updated successfully in '{identifier}'.",
#             "updated_column": {
#                 "name": updated_field.name,
#                 "type": str(updated_field.field_type),
#                 "doc": updated_field.doc,
#             }
#         }
#
#     except HTTPException:
#         raise
#
#     except Exception as e:
#         logger.exception(f"Unexpected error updating column '{column_name}': {e}")
#         raise HTTPException(status_code=500, detail=f"Internal server error: {e}")
import pyiceberg.io.pyarrow as paio

@router.put("/update_column_with_data_copy")
def update_column_with_data_copy(
    column_name: str = Query(..., description="Old column name"),
    new_column_name: str = Query(..., description="New column name"),
    column_type: Optional[str] = Query(None, description="New column type (optional)"),
    doc: Optional[str] = Query(None, description="New column description (optional)"),
    namespace: str = Query(...),
    table_name: str = Query(...)
):
    """
    Add a new column and copy old column data into it.
    Optionally update its type or doc.
    """

    try:
        catalog = get_catalog_client()
        identifier = f"{namespace}.{table_name}"
        table = catalog.load_table(identifier)

        # --- Step 1: Schema update ---
        old_field = next((f for f in table.schema().fields if f.name == column_name), None)
        if not old_field:
            raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found.")

        new_type = get_iceberg_type(column_type) if column_type else old_field.field_type
        new_doc = doc if doc else old_field.doc

        with table.update_schema() as update:
            update.add_column(new_column_name, new_type, doc=new_doc)
            logger.info(f"Added new column '{new_column_name}' of type '{new_type}'")

        # --- Step 2: Load existing data ---
        import pyarrow.dataset as ds
        import pyarrow as pa

        dataset = ds.dataset(table.location(), format="iceberg")
        table_data = dataset.to_table()
        df = table_data.to_pandas()

        if column_name not in df.columns:
            raise HTTPException(status_code=400, detail=f"Column '{column_name}' not found in data files.")

        # --- Step 3: Copy data ---
        df[new_column_name] = df[column_name]
        logger.info(f"Copied data from '{column_name}' to '{new_column_name}'")

        # --- Step 4: Write data back to Iceberg ---
        import pyiceberg.io.pyarrow as paio
        writer = paio.PyArrowFileIO()
        table.overwrite(pa.Table.from_pandas(df), overwrite=True)
        logger.info(f"Data rewrite completed for table '{identifier}'")

        return {
            "status": "success",
            "message": f"New column '{new_column_name}' added and populated from '{column_name}'",
            "rows_updated": len(df)
        }

    except Exception as e:
        logger.exception(f"Failed to update and copy data: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")

@router.delete("/")
def delete_column(
        column_name: str = Query(..., description="Column name to delete"),
        namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
        table_name: str = Query(..., description="Table name (e.g. 'Table name')")
):
    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")

        with table.update_schema() as update:
            update.delete_column(column_name)

        return {"status": "success", "message": f"Column '{column_name}' deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete column failed: {e}")
from datetime import datetime
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual

@router.delete("/delete")
def delete_data_or_column(
    namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
    table_name: str = Query(..., description="Table name (e.g. 'TableName')"),
    column_name: str = Query(None, description="Column name to delete (optional)"),
    date_column: str = Query(None, description="Date column for filtering (optional)"),
    start_date: str = Query(None, description="Start date (ISO format, e.g., 2025-10-01T00:00:00)"),
    end_date: str = Query(None, description="End date (ISO format, e.g., 2025-10-10T23:59:59)")
):
    """
    Deletes either a column (schema delete) or rows in a date range (data delete).
    """
    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")

        # 🧩 If column name is given, delete the column
        if column_name:
            with table.update_schema() as update:
                update.delete_column(column_name)
            return {"status": "success", "message": f"Column '{column_name}' deleted successfully"}

        # 🕓 If date range filters are given, delete rows
        if date_column and (start_date or end_date):
            expressions = []

            if start_date:
                try:
                    start_dt = datetime.fromisoformat(start_date)
                    expressions.append(GreaterThanOrEqual(date_column, start_dt))
                except ValueError:
                    raise HTTPException(status_code=400, detail="Invalid start_date format. Use ISO 8601 (YYYY-MM-DDTHH:MM:SS).")

            if end_date:
                try:
                    end_dt = datetime.fromisoformat(end_date)
                    expressions.append(LessThanOrEqual(date_column, end_dt))
                except ValueError:
                    raise HTTPException(status_code=400, detail="Invalid end_date format. Use ISO 8601 (YYYY-MM-DDTHH:MM:SS).")

            # Combine expressions with AND
            delete_filter = expressions[0]
            for expr in expressions[1:]:
                delete_filter = And(delete_filter, expr)

            # Execute Iceberg delete
            table.delete_where(delete_filter)
            return {
                "status": "success",
                "message": f"Rows deleted successfully for date range in '{date_column}'"
            }

        raise HTTPException(status_code=400, detail="Provide either 'column_name' for schema delete or 'date_column' with date range for data delete.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete operation failed: {e}")