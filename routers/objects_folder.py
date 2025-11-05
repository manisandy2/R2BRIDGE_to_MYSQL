
from fastapi import APIRouter,HTTPException,Query,Body
from ..core.catalog_client import get_catalog_client
from pyiceberg.exceptions import NoSuchTableError
import logging
from pyiceberg.exceptions import NamespaceAlreadyExistsError,NoSuchNamespaceError
import time
from ..mysql_creds import *
from ..mapping import *
from pyiceberg.schema import Schema
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

router = APIRouter(prefix="/objects", tags=["ObjectsFolder"])

@router.get("/list")
def get_tables(namespace: str = Query(..., description="Namespace to list tables from")):
    try:
        catalog = get_catalog_client()
        tables = catalog.list_tables(namespace)

        if tables:
            return {"namespace": namespace, "tables": tables}
        else:
            return {"namespace": namespace, "tables": [], "message": "No tables found."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list tables in namespace '{namespace}': {str(e)}")

# @router.post("/create")
# def transactions(
#     namespace: str = Query(..., description="transactions (e.g. 'transactions')"),
#     table_name: str = Query(..., description="transactions pos (e.g. 'transactions pos')"),
#     start_range: int = Query(0, description="Start row (e.g. 0)"),
#     end_range: int = Query(100000, description="End row (e.g. 100000)"),
#     dbname:str = Query(..., description="Database name"),
#     metadata: Optional[Dict[str, str]] = Body(None, description="Custom metadata key/value pairs")
# ):
#     start_time = time.time()
#
#     mysql_creds = MysqlCatalog()
#     try:
#         description = mysql_creds.get_describe(dbname)
#         rows = mysql_creds.get_range(dbname,start_range,end_range)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#
#     if not rows:
#         raise HTTPException(status_code=400, detail="No data found in the given range.")
#
#     iceberg_fields, arrow_fields = [],[]
#
#     for idx, column in enumerate(description):
#         name = column["Field"]
#         col_type = column["Type"].split('(')[0].lower()
#         is_nullable = column["Null"].upper() == "YES"
#
#         is_primary = column["Key"] == "PRI"
#         is_unique = column["Key"] == "UNI"
#
#         ice_type = type_mapping.get(col_type, StringType())
#         arrow_type = arrow_mapping.get(col_type, pa.string())
#
#         # tu_rows.append([name, col_type,is_nullable,is_key,is_primary, str(ice_type),str(arrow_type)])
#
#         iceberg_fields.append(NestedField(field_id=idx + 1, name=name, field_type=ice_type, required=not is_nullable))
#         arrow_fields.append(pa.field(name, arrow_type, nullable=is_nullable))
#
#
#     iceberg_schema = Schema(*iceberg_fields)
#     arrow_schema = pa.schema(arrow_fields)
#
#     pylist_rows = []
#     for row in rows:
#         converted = {}
#         for field in arrow_schema:
#             val = row[field.name]
#
#             if pa.types.is_integer(field.type):
#                 converted[field.name] = int(val) if val is not None else None
#             elif pa.types.is_floating(field.type):
#                 converted[field.name] = float(val) if val is not None else None
#             else:
#                 converted[field.name] = val
#         pylist_rows.append(converted)
#
#     arrow_table = pa.Table.from_pylist(pylist_rows, schema=arrow_schema)
#
#     catalog = get_catalog_client()
#     # catalog = creds.catalog_valid()
#
#     table_identifier = "{}.{}".format(namespace, table_name)
#     try:
#         tbl = catalog.create_table(table_identifier, schema=iceberg_schema,
#                                    properties=metadata if metadata else {})
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error creating table: {str(e)}")
#     tbl.append(arrow_table,)
#
#     elapsed = time.time() - start_time
#     return {
#         "status": "success",
#         # "action": action,
#         "namespace": namespace,
#         "table": table_name,
#         "rows_written": len(pylist_rows),
#         "elapsed_seconds": round(elapsed, 2),
#         "schema": [f.name for f in iceberg_schema.columns],
#         "metadata": metadata or {},
#         "table_properties": tbl.properties if hasattr(tbl, "properties") else {}
#     }
#

@router.post("/InsertOne")
def insert_one(
    namespace: str = Query(..., description="transactions (e.g. 'transactions')"),
    table_name: str = Query(..., description="transactions pos (e.g. 'transactions pos')"),
    record: dict = Body(..., description="Single JSON record to insert into Iceberg")
):
    start_time = time.time()

    iceberg_fields = []
    arrow_fields = []

    for idx, (name, value) in enumerate(record.items()):
        # --- Infer column type ---
        if isinstance(value, bool):
            ice_type = BooleanType()
            arrow_type = pa.bool_()
        elif isinstance(value, int):
            ice_type = LongType()     # always use 64-bit
            arrow_type = pa.int64()
        elif isinstance(value, float):
            ice_type = DoubleType()
            arrow_type = pa.float64()
        else:
            ice_type = StringType()
            arrow_type = pa.string()

        iceberg_fields.append(
            NestedField(field_id=idx + 1, name=name, field_type=ice_type, required=False)
        )
        arrow_fields.append(pa.field(name, arrow_type, nullable=True))

    iceberg_schema = Schema(*iceberg_fields)
    arrow_schema = pa.schema(arrow_fields)

    def convert_row(row):
        converted = {}
        for field in arrow_schema:
            val = row.get(field.name)
            if pa.types.is_integer(field.type):
                converted[field.name] = int(val) if val is not None else None
            elif pa.types.is_floating(field.type):
                converted[field.name] = float(val) if val is not None else None
            elif pa.types.is_boolean(field.type):
                converted[field.name] = bool(val) if val is not None else None
            else:
                converted[field.name] = str(val) if val is not None else None
        return converted

    converted_record = convert_row(record)
    arrow_table = pa.Table.from_pylist([converted_record], schema=arrow_schema)

    catalog = get_catalog_client()
    table_identifier = f"{namespace}.{table_name}"


    try:
        tbl = catalog.load_table(table_identifier)
    except NoSuchTableError:
        tbl = catalog.create_table(table_identifier, schema=iceberg_schema)

    tbl.append(arrow_table)
    elapsed = time.time() - start_time
    return {
        "status": "success",
        "namespace": namespace,
        "table": table_name,
        "rows_written": 1,
        "elapsed_seconds": round(elapsed, 2)
    }
# @router.put("/update")
# def update_transactions(
#     namespace: str = Query(..., description="transactions (e.g. 'transactions')"),
#     table_name: str = Query(..., description="transactions pos (e.g. 'transactions pos')"),
#     start_range: int = Query(0, description="Start row (e.g. 0)"),
#     end_range: int = Query(100000, description="End row (e.g. 100000)"),
#     dbname:str = Query(..., description="Database name")
# ):
#     total_start = time.time()
#
#     mysql_creds = MysqlCatalog()
#
#     step_start = time.time()
#     try:
#         description = mysql_creds.get_describe(dbname)
#         rows = mysql_creds.get_range(dbname,start_range,end_range)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#
#     if not rows:
#         raise HTTPException(status_code=400, detail="No data found in the given range.")
#     db_fetch_time = time.time() - step_start
#
#     iceberg_fields = []
#     arrow_fields = []
#     print("Reading files")
#     step_start = time.time()
#     for idx, column in enumerate(description):
#         # print(idx)
#         name = column["Field"]
#         col_type = column["Type"].split('(')[0].lower()
#         is_nullable = column["Null"].upper() == "YES"
#
#         ice_type = type_mapping.get(col_type, StringType())
#         arrow_type = arrow_mapping.get(col_type, pa.string())
#
#         # tu_rows.append([name, col_type,is_nullable,is_key,is_primary, str(ice_type),str(arrow_type)])
#
#         iceberg_fields.append(NestedField(field_id=idx + 1, name=name, field_type=ice_type, required=not is_nullable))
#         arrow_fields.append(pa.field(name, arrow_type, nullable=is_nullable))
#
#
#     iceberg_schema = Schema(*iceberg_fields)
#     arrow_schema = pa.schema(arrow_fields)
#     schema_build_time = time.time() - step_start
#
#     step_start = time.time()
#     pylist_rows = []
#     print("data pushing")
#     for row in rows:
#         # print("row",row)
#         converted = {}
#         for field in arrow_schema:
#             val = row[field.name]
#
#             if pa.types.is_integer(field.type):
#                 converted[field.name] = int(val) if val is not None else None
#             elif pa.types.is_floating(field.type):
#                 converted[field.name] = float(val) if val is not None else None
#             else:
#                 converted[field.name] = val
#         pylist_rows.append(converted)
#
#     arrow_table = pa.Table.from_pylist(pylist_rows, schema=arrow_schema)
#     data_convert_time = time.time() - step_start
#
#     step_start = time.time()
#     catalog = get_catalog_client()
#
#
#     table_identifier = "{}.{}".format(namespace, table_name)
#
#     try:
#         tbl = catalog.load_table(table_identifier)
#     except Exception:
#         raise HTTPException(status_code=404, detail=f"Table '{table_identifier}' not found.")
#
#     tbl.append(arrow_table)
#     catalog_append_time = time.time() - step_start
#
#     total_elapsed = time.time() - total_start
#
#     return {
#         "status": "success",
#         # "action": action,
#         "namespace": namespace,
#         "table": table_name,
#         "rows_written": len(pylist_rows),
#         "time_model": {
#             "db_fetch": f"{db_fetch_time:.2f} sec",
#             "schema_build": f"{schema_build_time:.2f} sec",
#             "data_conversion": f"{data_convert_time:.2f} sec",
#             "catalog_append": f"{catalog_append_time:.2f} sec",
#             "total_elapsed": f"{total_elapsed:.2f} sec"
#         }
#     }

####### row wise
# @router.put("/update")
# def update_transactions(
#     namespace: str = Query(..., description="transactions (e.g. 'transactions')"),
#     table_name: str = Query(..., description="transactions pos (e.g. 'transactions pos')"),
#     start_range: int = Query(0, description="Start row (e.g. 0)"),
#     end_range: int = Query(100000, description="End row (e.g. 100000)"),
#     dbname: str = Query(..., description="Database name")
# ):
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()
#
#     # --- DB Fetch ---
#     print("DB Fetch")
#     step_start = time.time()
#     try:
#         description = mysql_creds.get_describe(dbname)
#         rows = mysql_creds.get_range(dbname, start_range, end_range)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#
#     if not rows:
#         raise HTTPException(status_code=400, detail="No data found in the given range.")
#     db_fetch_time = time.time() - step_start
#
#     # --- Schema Build ---
#     print("Schema Build")
#     step_start = time.time()
#
#
#     iceberg_fields = []
#     arrow_fields = []
#     for idx, column in enumerate(description):
#         name = column["Field"]
#         col_type = column["Type"].split('(')[0].lower()
#         is_nullable = column["Null"].upper() == "YES"
#
#         ice_type = type_mapping.get(col_type, StringType())
#         arrow_type = arrow_mapping.get(col_type, pa.string())
#
#         iceberg_fields.append(
#             NestedField(field_id=idx + 1, name=name, field_type=ice_type, required=not is_nullable)
#         )
#         arrow_fields.append(pa.field(name, arrow_type, nullable=is_nullable))
#
#     iceberg_schema = Schema(*iceberg_fields)
#     arrow_schema = pa.schema(arrow_fields)
#     schema_build_time = time.time() - step_start
#
#     # --- Parallel Data Conversion ---
#     print("data conversion row wise")
#     step_start = time.time()
#
#     def convert_row(row):
#         converted = {}
#         for field in arrow_schema:
#             val = row[field.name]
#             if pa.types.is_integer(field.type):
#                 converted[field.name] = int(val) if val is not None else None
#             elif pa.types.is_floating(field.type):
#                 converted[field.name] = float(val) if val is not None else None
#             else:
#                 converted[field.name] = val
#         return converted
#
#     print("data conversion 01")
#     pylist_rows = []
#     with ThreadPoolExecutor(max_workers=10) as executor:  # tune workers
#         futures = [executor.submit(convert_row, row) for row in rows]
#         for future in as_completed(futures):
#             pylist_rows.append(future.result())
#
#     print("data conversion 02")
#     arrow_table = pa.Table.from_pylist(pylist_rows, schema=arrow_schema)
#     data_convert_time = time.time() - step_start
#
#     # --- Catalog Append ---
#     print("Catalog Build")
#     step_start = time.time()
#     catalog = get_catalog_client()
#     table_identifier = "{}.{}".format(namespace, table_name)
#
#     try:
#         tbl = catalog.load_table(table_identifier)
#     except Exception:
#         raise HTTPException(status_code=404, detail=f"Table '{table_identifier}' not found.")
#
#     tbl.append(arrow_table)
#     catalog_append_time = time.time() - step_start
#
#     # --- Total Time ---
#     total_elapsed = time.time() - total_start
#
#     return {
#         "status": "success",
#         "namespace": namespace,
#         "table": table_name,
#         "rows_written": len(pylist_rows),
#         "time_model": {
#             "db_fetch": f"{db_fetch_time:.2f} sec",
#             "schema_build": f"{schema_build_time:.2f} sec",
#             "data_conversion": f"{data_convert_time:.2f} sec (multithreaded)",
#             "catalog_append": f"{catalog_append_time:.2f} sec",
#             "total_elapsed": f"{total_elapsed:.2f} sec"
#         }
#     }

#### column -wise
# @router.put("/update")
# def update_transactions(
#     namespace: str = Query(..., description="transactions (e.g. 'transactions')"),
#     table_name: str = Query(..., description="transactions pos (e.g. 'transactions pos')"),
#     start_range: int = Query(0, description="Start row (e.g. 0)"),
#     end_range: int = Query(100000, description="End row (e.g. 100000)"),
#     dbname: str = Query(..., description="Database name")
# ):
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()
#
#     # --- DB Fetch ---
#     print("DB Fetch")
#     step_start = time.time()
#     try:
#         description = mysql_creds.get_describe(dbname)
#         rows = mysql_creds.get_range(dbname, start_range, end_range)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#
#     if not rows:
#         raise HTTPException(status_code=400, detail="No data found in the given range.")
#     db_fetch_time = time.time() - step_start
#
#     # --- Schema Build ---
#     print("Schema Build")
#     step_start = time.time()
#     iceberg_fields = []
#     arrow_fields = []
#     for idx, column in enumerate(description):
#         name = column["Field"]
#         col_type = column["Type"].split('(')[0].lower()
#         is_nullable = column["Null"].upper() == "YES"
#
#         ice_type = type_mapping.get(col_type, StringType())
#         arrow_type = arrow_mapping.get(col_type, pa.string())
#
#         iceberg_fields.append(
#             NestedField(field_id=idx + 1, name=name, field_type=ice_type, required=not is_nullable)
#         )
#         arrow_fields.append(pa.field(name, arrow_type, nullable=is_nullable))
#
#     iceberg_schema = Schema(*iceberg_fields)
#     arrow_schema = pa.schema(arrow_fields)
#     schema_build_time = time.time() - step_start
#
#     # --- Fast Column-wise Data Conversion ---
#     print("Data Conversion (column-wise)")
#     step_start = time.time()
#
#     columns_data = {field.name: [] for field in arrow_schema}
#
#     for row in rows:
#         for field in arrow_schema:
#             val = row[field.name]
#             if pa.types.is_integer(field.type):
#                 columns_data[field.name].append(int(val) if val is not None else None)
#             elif pa.types.is_floating(field.type):
#                 columns_data[field.name].append(float(val) if val is not None else None)
#             else:
#                 columns_data[field.name].append(val)
#
#     arrow_table = pa.Table.from_pydict(columns_data, schema=arrow_schema)
#     data_convert_time = time.time() - step_start
#
#     # --- Catalog Append ---
#     print("Catalog Append")
#     step_start = time.time()
#     catalog = get_catalog_client()
#     table_identifier = f"{namespace}.{table_name}"
#
#     try:
#         tbl = catalog.load_table(table_identifier)
#     except Exception:
#         raise HTTPException(status_code=404, detail=f"Table '{table_identifier}' not found.")
#
#     tbl.append(arrow_table)
#     catalog_append_time = time.time() - step_start
#
#     # --- Total Time ---
#     total_elapsed = time.time() - total_start
#
#     return {
#         "status": "success",
#         "namespace": namespace,
#         "table": table_name,
#         "rows_written": len(rows),
#         "time_model": {
#             "db_fetch": f"{db_fetch_time:.2f} sec",
#             "schema_build": f"{schema_build_time:.2f} sec",
#             "data_conversion": f"{data_convert_time:.2f} sec (column-wise, fast)",
#             "catalog_append": f"{catalog_append_time:.2f} sec",
#             "total_elapsed": f"{total_elapsed:.2f} sec"
#         }
#     }

@router.delete("/delete")
def delete_table(
    namespace: str = Query(..., description="Namespace of the table"),
    table_name: str = Query(..., description="Name of the table to drop")
):
    catalog = get_catalog_client()
    full_table_name = f"{namespace}.{table_name}"

    try:
        catalog.drop_table(full_table_name)
        return {"message": f"Table '{full_table_name}' dropped successfully."}

    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table '{full_table_name}' does not exist.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to drop table '{full_table_name}': {str(e)}")

@router.get("/data")
def read_table(
    namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
    table_name: str = Query(..., description="Table name (e.g. 'Table name')")
):
    try:
        catalog = get_catalog_client()
        table = catalog.load_table((namespace, table_name))

        reader = table.scan().to_arrow()
        df = reader.to_pandas()

        # Replace NaN/Inf with None so JSON can serialize
        df = df.replace({pd.NA: None, float("nan"): None, float("inf"): None, -float("inf"): None})

        return {
            "namespace": namespace,
            "table_name": table_name,
            "records_count": len(df),
            "data": df.to_dict(orient="records")
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read Iceberg table: {str(e)}")

@router.get("/Inspect")
def table_inspect(
    namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
    table_name: str = Query(..., description="Table name (e.g. 'Table name')")
):
    try:
        catalog = get_catalog_client()
        table = catalog.load_table((namespace, table_name))

        snapshots = list(table.snapshots())

        snapshot_data = []
        for s in snapshots:
            snapshot_data.append({
                "snapshot_id": getattr(s, "snapshot_id", None),
                "parent_snapshot_id": getattr(s, "parent_snapshot_id", None),
                "timestamp_ms": getattr(s, "timestamp_ms", None),
                "manifest_list": getattr(s, "manifest_list", None),
                "summary": getattr(s, "summary", {})
            })

        return {
            "namespace": namespace,
            "table_name": table.name,
            "records_count": len(snapshots),
            "snapshots": snapshot_data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to inspect table: {str(e)}")

import logging

logger = logging.getLogger(__name__)

@router.post("/register_table")
def register_table(
    namespace: str = Query(..., description="Namespace (e.g. 'sales')"),
    table_name: str = Query(..., description="Table name to register (e.g. 'transactions_copy')"),
    metadata_location: str = Query(..., description="Full path to the metadata.json file")
):
    """
    Register an existing Iceberg table in the catalog using its metadata.json file.
    """
    catalog = get_catalog_client()
    identifier = f"{namespace}.{table_name}"

    try:
        # Check if table already exists
        if catalog.table_exists(identifier):
            raise HTTPException(status_code=400, detail=f"Table '{identifier}' already exists.")

        # Register the table using existing metadata
        catalog.register_table(
            identifier=identifier,
            metadata_location=metadata_location
        )

        logger.info(f"Registered table successfully: {identifier}")

        return {
            "status": "success",
            "message": f"Table '{identifier}' registered successfully.",
            "metadata_location": metadata_location
        }

    except Exception as e:
        logger.error(f"Failed to register table '{identifier}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to register table '{identifier}': {str(e)}")


@router.get("/get_metadata_location")
def get_metadata_location(
    namespace: str = Query(..., description="Namespace of the table (e.g. 'sales')"),
    table_name: str = Query(..., description="Name of the table (e.g. 'transactions')")
):
    """
    Returns the metadata location of an Iceberg table given namespace and table name.
    """
    try:
        catalog = get_catalog_client()
        full_table_name = f"{namespace}.{table_name}"

        # Load the table
        table = catalog.load_table(full_table_name)

        # Get metadata location
        metadata_location = table.metadata_location  # usually a string URL/path
        logger.info(f"Metadata location for table '{full_table_name}': {metadata_location}")

        return {
            "status": "success",
            "namespace": namespace,
            "table_name": table_name,
            "metadata_location": metadata_location
        }

    except Exception as e:
        logger.error(f"Failed to get metadata location for '{full_table_name}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get metadata location: {str(e)}")


# @router.get("/get_schema_update")
# def get_schema_update(
#     namespace: str = Query(..., description="Namespace of the table (e.g. 'sales')"),
#     table_name: str = Query(..., description="Name of the table (e.g. 'transactions')")
# ):
#     """
#         Updates the 'Bill_Date__c' column to DateType and returns the updated schema.
#         """
#     try:
#         catalog = get_catalog_client()
#         full_table_name = f"{namespace}.{table_name}"
#         table = catalog.load_table(full_table_name)
#
#         # Build new schema
#         new_fields = []
#         column_found = False
#         for field in table.schema().fields:
#             if field.name == "Bill_Date__c":
#                 new_fields.append(NestedField(field.field_id, field.name, DateType(), field.required))
#                 column_found = True
#             else:
#                 new_fields.append(field)
#
#         if not column_found:
#             raise HTTPException(status_code=404, detail="Column 'Bill_Date__c' not found in table.")
#
#         new_schema = Schema(*new_fields)
#
#         # Apply schema evolution
#         table.update_schema().replace_schema(new_schema).commit()
#
#         # Prepare JSON-serializable schema
#         schema_json = [
#             {
#                 "id": field.field_id,
#                 "name": field.name,
#                 "type": str(field.type),
#                 "required": field.required
#             }
#             for field in new_schema.fields
#         ]
#
#         return {
#             "status": "success",
#             "table_name": table_name,
#             "updated_schema": schema_json
#         }
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to update schema: {str(e)}")


@router.get("/update_column_date")
def update_column_date(
    namespace: str = Query(...),
    table_name: str = Query(...),
    column_name: str = Query("Bill_Date__c")
):
    try:
        catalog = get_catalog_client()
        table = catalog.load_table(f"{namespace}.{table_name}")

        # column_field = next((f for f in table.schema().fields if f.name == column_name), None)
        # if not column_field:
        #     raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found")

        # Update column type
        # table.update_schema().update_column(
        #     field_id=column_field.field_id,
        #     new_type=DateType()
        # ).commit()
        # table.update_schema().update_column(
        #     column_name=column_name,
        #     new_type=DateType()
        # ).commit()
            # Find the field object
        old_field = next((f for f in table.schema().fields if f.name == column_name), None)
        if not old_field:
            raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found")

        # Update column type
        table.update_schema().update_column(
            old_field,
            new_type=DateType()
        ).commit()

        return {
            "status": "success",
            "message": f"Column '{column_name}' updated to DateType successfully."
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update column: {str(e)}")