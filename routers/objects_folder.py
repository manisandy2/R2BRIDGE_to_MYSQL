
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

@router.post("/create")
def transactions(
    namespace: str = Query(..., description="transactions (e.g. 'transactions')"),
    table_name: str = Query(..., description="transactions pos (e.g. 'transactions pos')"),
    start_range: int = Query(0, description="Start row (e.g. 0)"),
    end_range: int = Query(100000, description="End row (e.g. 100000)"),
    dbname:str = Query(..., description="Database name"),
    metadata: Optional[Dict[str, str]] = Body(None, description="Custom metadata key/value pairs")
):
    start_time = time.time()

    mysql_creds = MysqlCatalog()
    try:
        description = mysql_creds.get_describe(dbname)
        rows = mysql_creds.get_range(dbname,start_range,end_range)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")

    if not rows:
        raise HTTPException(status_code=400, detail="No data found in the given range.")

    iceberg_fields, arrow_fields = [],[]

    for idx, column in enumerate(description):
        name = column["Field"]
        col_type = column["Type"].split('(')[0].lower()
        is_nullable = column["Null"].upper() == "YES"

        is_primary = column["Key"] == "PRI"
        is_unique = column["Key"] == "UNI"

        ice_type = type_mapping.get(col_type, StringType())
        arrow_type = arrow_mapping.get(col_type, pa.string())

        # tu_rows.append([name, col_type,is_nullable,is_key,is_primary, str(ice_type),str(arrow_type)])

        iceberg_fields.append(NestedField(field_id=idx + 1, name=name, field_type=ice_type, required=not is_nullable))
        arrow_fields.append(pa.field(name, arrow_type, nullable=is_nullable))


    iceberg_schema = Schema(*iceberg_fields)
    arrow_schema = pa.schema(arrow_fields)

    pylist_rows = []
    for row in rows:
        converted = {}
        for field in arrow_schema:
            val = row[field.name]

            if pa.types.is_integer(field.type):
                converted[field.name] = int(val) if val is not None else None
            elif pa.types.is_floating(field.type):
                converted[field.name] = float(val) if val is not None else None
            else:
                converted[field.name] = val
        pylist_rows.append(converted)

    arrow_table = pa.Table.from_pylist(pylist_rows, schema=arrow_schema)

    catalog = get_catalog_client()
    # catalog = creds.catalog_valid()

    table_identifier = "{}.{}".format(namespace, table_name)
    try:
        tbl = catalog.create_table(table_identifier, schema=iceberg_schema,
                                   properties=metadata if metadata else {})

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating table: {str(e)}")
    tbl.append(arrow_table,)

    elapsed = time.time() - start_time
    return {
        "status": "success",
        # "action": action,
        "namespace": namespace,
        "table": table_name,
        "rows_written": len(pylist_rows),
        "elapsed_seconds": round(elapsed, 2),
        "schema": [f.name for f in iceberg_schema.columns],
        "metadata": metadata or {},
        "table_properties": tbl.properties if hasattr(tbl, "properties") else {}
    }
#
@router.post("/InsertOne")
def insert_one(
    namespace: str = Query(..., description="transactions (e.g. 'transactions')"),
    table_name: str = Query(..., description="transactions pos (e.g. 'transactions pos')"),
    record: dict = Body(..., description="Single JSON record to insert into Iceberg")
):
    start_time = time.time()

    catalog = get_catalog_client()
    table_identifier = f"{namespace}.{table_name}"
    tbl = catalog.load_table(table_identifier)

    arrow_schema = tbl.schema().as_arrow()
    print(arrow_schema)
    # pylist_rows = []
    # for row in record["rows"]:

    for field in arrow_schema:
            if field.name not in record:
                record[field.name] = None

    arrow_table = pa.Table.from_pylist([record], schema=arrow_schema)

    tbl.append(arrow_table)

    elapsed = time.time() - start_time
    return {
        "status": "success",
        "namespace": namespace,
        "table": table_name,
        "rows_written": 1,
        "elapsed_seconds": round(elapsed, 2)
    }
@router.put("/update")
def update_transactions(
    namespace: str = Query(..., description="transactions (e.g. 'transactions')"),
    table_name: str = Query(..., description="transactions pos (e.g. 'transactions pos')"),
    start_range: int = Query(0, description="Start row (e.g. 0)"),
    end_range: int = Query(100000, description="End row (e.g. 100000)"),
    dbname:str = Query(..., description="Database name")
):
    start_time = time.time()

    mysql_creds = MysqlCatalog()
    try:
        description = mysql_creds.get_describe(dbname)
        rows = mysql_creds.get_range(dbname,start_range,end_range)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")

    if not rows:
        raise HTTPException(status_code=400, detail="No data found in the given range.")

    iceberg_fields = []
    arrow_fields = []

    for idx, column in enumerate(description):
        name = column["Field"]
        col_type = column["Type"].split('(')[0].lower()
        is_nullable = column["Null"].upper() == "YES"

        ice_type = type_mapping.get(col_type, StringType())
        arrow_type = arrow_mapping.get(col_type, pa.string())

        # tu_rows.append([name, col_type,is_nullable,is_key,is_primary, str(ice_type),str(arrow_type)])

        iceberg_fields.append(NestedField(field_id=idx + 1, name=name, field_type=ice_type, required=not is_nullable))
        arrow_fields.append(pa.field(name, arrow_type, nullable=is_nullable))

    iceberg_schema = Schema(*iceberg_fields)
    arrow_schema = pa.schema(arrow_fields)

    pylist_rows = []
    for row in rows:
        converted = {}
        for field in arrow_schema:
            val = row[field.name]

            if pa.types.is_integer(field.type):
                converted[field.name] = int(val) if val is not None else None
            elif pa.types.is_floating(field.type):
                converted[field.name] = float(val) if val is not None else None
            else:
                converted[field.name] = val
        pylist_rows.append(converted)

    arrow_table = pa.Table.from_pylist(pylist_rows, schema=arrow_schema)

    catalog = get_catalog_client()


    table_identifier = "{}.{}".format(namespace, table_name)

    try:
        tbl = catalog.load_table(table_identifier)
    except Exception:
        raise HTTPException(status_code=404, detail=f"Table '{table_identifier}' not found.")

    tbl.append(arrow_table)
    elapsed = time.time() - start_time
    return {
        "status": "success",
        # "action": action,
        "namespace": namespace,
        "table": table_name,
        "rows_written": len(pylist_rows),
        "elapsed_seconds": round(elapsed, 2)
    }


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

