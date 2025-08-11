from fastapi import FastAPI,Query,HTTPException
# from mysql_catalog import MysqlCatalog
from mysql_creds import  MysqlCatalog
from pyiceberg.exceptions import NoSuchNamespaceError,NamespaceAlreadyExistsError,TableAlreadyExistsError
# from creds import Creds
from creds import Creds, CloudflareR2Creds
from pydantic import BaseModel
from pyiceberg.exceptions import NoSuchTableError
from mapping import *
from pyiceberg.schema import Schema, NestedField
import json
import os
from fastapi import FastAPI, Query, HTTPException
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import GreaterThanOrEqual,EqualTo

app = FastAPI()


# @app.get("/employees")
# def read_employees():
#     catalog = MysqlCatalog()
#     try:
#         data = catalog.get_employees()
#         return {"employees": data}
#     finally:
#         catalog.close()


@app.get("/employees/count")
def employee_count():
    catalog = MysqlCatalog()
    try:
        count = catalog.get_count()
        return {"count": count}
    finally:
        catalog.close()

@app.get("/employees/limit")
def get_limited_employees(limit: int = Query(10, gt=0, le=1000, description="Number of rows to return (max 1000)")):
    catalog = MysqlCatalog()
    try:
        records = catalog.get_limits(limit)
        return {"limit": limit, "employees": records}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch employees: {str(e)}")
    finally:
        catalog.close()

@app.get("/employees/schema")
def employee_schema():
    catalog = MysqlCatalog()
    try:
        description = catalog.get_describe()
        schema = [{"name": col[0], "type": col[1]} for col in description]
        return {"schema": schema}
    finally:
        catalog.close()


@app.get("/iceberg/namespaces")
def list_namespaces():
    try:
        catalog = Creds().catalog_valid()
        namespaces = catalog.list_namespaces()
        return {"namespaces": namespaces}
    
    # catalog.list_namespaces
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list namespaces: {str(e)}")

class NamespaceRequest(BaseModel):
    name: str

@app.post("/iceberg/namespaces")
def create_namespace(
namespace: str = Query(..., description="Namespace (e.g. 'employees')"),
):
    try:
        catalog = Creds().catalog_valid()
        catalog.create_namespace(namespace)
        return {"message": f"Namespace '{namespace}' created successfully."}
    except NamespaceAlreadyExistsError:
        raise HTTPException(status_code=409, detail=f"Namespace '{namespace}' already exists.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create namespace '{namespace}': {str(e)}")

@app.delete("/iceberg/namespaces")
def delete_namespace(namespace: str = Query(..., description="Namespace to delete")):
    catalog = Creds().catalog_valid()
    try:
        catalog.drop_namespace(namespace)
        # print(f"Table '{f"{namespace}"}' dropped successfully.")
        return {"message": f" Namespace '{namespace}' dropped successfully."}
    except NoSuchNamespaceError:
        # print(f"Table '{f"{namespace}"}' does not exist.")
        raise HTTPException(status_code=404, detail=f"Namespace '{namespace}' does not exist.")
    except Exception as e:
        # print(f"Failed to drop table '{f"{namespace}"}': {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete namespace '{namespace}': {str(e)}")


@app.get("/iceberg/tables")
def list_tables(namespace: str = Query(..., description="Namespace to list tables from")):
    try:
        catalog = Creds().catalog_valid()
        tables = catalog.list_tables(namespace)

        if tables:
            return {"namespace": namespace, "tables": tables}
        else:
            return {"namespace": namespace, "tables": [], "message": "No tables found."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list tables in namespace '{namespace}': {str(e)}")


@app.post("/iceberg/create-table")
def create_table(
        namespace: str = Query(..., description="Namespace (e.g. 'employees')"),
        table_name: str = Query(..., description="Table name (e.g. 'people')"),
        start: int = Query(0, description="Start row (e.g. 0)"),
        end: int = Query(100, description="End row (e.g. 100)")
):
    try:
        mysql_catalog = MysqlCatalog()
        description = mysql_catalog.get_describe()
        rows = mysql_catalog.get_range(start= start, end=end)

        # Create Iceberg Catalog
        creds = Creds()
        catalog = creds.catalog_valid()

        # Create Namespace if not exists
        # try:
        #     catalog.load_namespace(namespace)
        # except NamespaceAlreadyExistsError:
        #     raise HTTPException(status_code=409, detail=f"Namespace '{namespace}' already exists.")

        table_identifier = f"{namespace}.{table_name}"

        # Generate schemas
        iceberg_fields = []
        arrow_fields = []
        for i, column in enumerate(description):
            name = column[0]
            col_type = column[1].split('(')[0].lower()
            nullable = column[2] == "YES"

            if name == "emp_no":
                nullable = False

            ice_type = type_mapping.get(col_type, StringType())
            arrow_type = arrow_mapping.get(col_type, pa.string())

            iceberg_fields.append(NestedField(field_id=i + 1, name=name, field_type=ice_type, required=not nullable))
            arrow_fields.append(pa.field(name, arrow_type, nullable=nullable))

        iceberg_schema = Schema(*iceberg_fields)
        arrow_schema = pa.schema(arrow_fields)

        # Create Table
        try:
            tbl = catalog.create_table(table_identifier, schema=iceberg_schema)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Table creation failed: {e}")

        # Convert MySQL rows to Arrow Table
        column_names = [desc[0] for desc in description]
        arrow_table = pa.Table.from_pylist(
            [dict(zip(column_names, row)) for row in rows],
            schema=arrow_schema
        )

        # Append data
        tbl.append(arrow_table)

        return {
            "message": f"Iceberg table '{table_identifier}' created and data appended successfully.",
            "rows_inserted": len(rows)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Error: {str(e)}")

@app.post("/iceberg/create-table-json")
def create_table_json_store(
        namespace: str = Query(..., description="Namespace (e.g. 'employees')"),
        table_name: str = Query(..., description="Table name (e.g. 'people')"),
        start: int = Query(0, description="Start row (e.g. 0)"),
        end: int = Query(100, description="End row (e.g. 100)")
):
    try:
        mysql_catalog = MysqlCatalog()
        description = mysql_catalog.get_describe()
        rows = mysql_catalog.get_range(start= start, end=end)

        # Create Iceberg Catalog
        creds = Creds()
        catalog = creds.catalog_valid()


        cloud_r2_creds = CloudflareR2Creds()
        r2_client = cloud_r2_creds.get_client()
        r2_key = f"iceberg_json/{namespace}_{table_name}.json"


        table_identifier = f"{namespace}.{table_name}"

        # Generate schemas
        iceberg_fields = []
        arrow_fields = []
        for i, column in enumerate(description):
            name = column[0]
            col_type = column[1].split('(')[0].lower()
            nullable = column[2] == "YES"

            if name == "emp_no":
                nullable = False

            ice_type = type_mapping.get(col_type, StringType())
            arrow_type = arrow_mapping.get(col_type, pa.string())

            iceberg_fields.append(NestedField(field_id=i + 1, name=name, field_type=ice_type, required=not nullable))
            arrow_fields.append(pa.field(name, arrow_type, nullable=nullable))

        iceberg_schema = Schema(*iceberg_fields)
        arrow_schema = pa.schema(arrow_fields)

        # Create Table
        try:
            tbl = catalog.create_table(table_identifier, schema=iceberg_schema)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Table creation failed: {e}")

        # Convert MySQL rows to Arrow Table
        column_names = [desc[0] for desc in description]
        arrow_table = pa.Table.from_pylist(
            [dict(zip(column_names, row)) for row in rows],schema=arrow_schema)



        # Append data
        tbl.append(arrow_table)

        data_dicts = [dict(zip(column_names, row)) for row in rows]
        json_data = json.dumps(data_dicts, default=str, indent=2)

        backup_dir = "json_backups"
        os.makedirs(backup_dir, exist_ok=True)

        json_path = os.path.join(backup_dir, f"{namespace}_{table_name}.json")
        with open(json_path, "w") as f:
            f.write(json_data)


        # cloud_r2_creds.put_object

        try:
            r2_client.put_object(
                Bucket=cloud_r2_creds.BUCKET_NAME,
                Key=r2_key,
                Body=json_data,
                ContentType="application/json"
            )
            print(f"Uploaded {json_path} to R2 bucket '{r2_client.BUCKET_NAME}'")
        except Exception as e:
            print(f"Error uploading JSON: {e}")


        return {
            "message": f"Iceberg table '{table_identifier}' created and data appended successfully.",
            "rows_inserted": len(rows),
            "json_backup_path": json_path
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Error: {str(e)}")

@app.delete("/iceberg/tables")
def delete_table(
    namespace: str = Query(..., description="Namespace of the table"),
    table_name: str = Query(..., description="Name of the table to drop")
):
    catalog = Creds().catalog_valid()
    full_table_name = f"{namespace}.{table_name}"

    try:
        catalog.drop_table(full_table_name)
        return {"message": f"Table '{full_table_name}' dropped successfully."}

    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Table '{full_table_name}' does not exist.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to drop table '{full_table_name}': {str(e)}")


@app.get("/iceberg/get-table")
def read_table(
    namespace: str = Query(..., description="Namespace (e.g. 'employees')"),
    table_name: str = Query(..., description="Table name (e.g. 'people')")
):
    try:
        # Create Iceberg Catalog instance
        catalog = Creds().catalog_valid()

        # Load the table
        table = catalog.load_table((namespace, table_name))

        # Scan and convert to PyArrow Table
        reader = table.scan().to_arrow()
        df = reader.to_pandas()

        # Convert Pandas to dict (records format) for JSON response
        return {
            "namespace": namespace,
            "table_name": table_name,
            "records_count": len(df),
            "data": df.to_dict(orient="records")
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read Iceberg table: {str(e)}")



@app.put("/iceberg/table-update")
def update_table(
    namespace: str = Query(..., description="Namespace (e.g. 'employees')"),
    table_name: str = Query(..., description="Table name (e.g. 'people')"),
    start: int = Query(0, description="Start row (e.g. 0)"),
    end: int = Query(1000, description="End row (e.g. 1000)")
):

    # Step 1: MySQL - Get schema and rows
    try:
        mysql_catalog = MysqlCatalog()
        description = mysql_catalog.get_describe()
        rows = mysql_catalog.get_range(start=start, end=end)
        if not rows:
            raise HTTPException(status_code=400, detail="No data found in the given range.")

        # Step 2: Iceberg Catalog setup
        creds = Creds()
        catalog = creds.catalog_valid()

        # Step 3: Schema preparation
        iceberg_fields = []
        arrow_fields = []
        for i, column in enumerate(description):
            name = column[0]
            col_type = column[1].split('(')[0].lower()
            nullable = column[2] == "YES"
            if name == "emp_no":
                nullable = False  # example required field

            ice_type = type_mapping.get(col_type, StringType())
            arrow_type = arrow_mapping.get(col_type, pa.string())

            iceberg_fields.append(NestedField(field_id=i + 1, name=name, field_type=ice_type, required=not nullable))
            arrow_fields.append(pa.field(name, arrow_type, nullable=nullable))

        iceberg_schema = Schema(*iceberg_fields)
        arrow_schema = pa.schema(arrow_fields)

        table_identifier = f"{namespace}.{table_name}"

        # Step 4: Create or Load Iceberg Table

        try:
            tbl = catalog.load_table(table_identifier)
        except Exception:
            raise HTTPException(status_code=404, detail=f"Table '{table_identifier}' not found.")

        # Step 5: Convert MySQL rows to Arrow Table
        column_names = [desc[0] for desc in description]
        arrow_table = pa.Table.from_pylist(
            [dict(zip(column_names, row)) for row in rows],
            schema=arrow_schema
        )

        # Step 6: Write to Iceberg Table

        # tbl.overwrite(arrow_table)
        tbl.append(arrow_table)

        return {
            "message": f"Iceberg table '{table_identifier}' and overwritten with new data.",
            "rows_append": len(rows)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Error: {str(e)}")


@app.get("/iceberg/table-inspect")
def table_inspect(
    namespace: str = Query(..., description="Namespace (e.g. 'employees')"),
    table_name: str = Query(..., description="Table name (e.g. 'people')")
):
    try:
        catalog = Creds().catalog_valid()
        # table_identifier = f"{namespace}.{table_name}"
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





@app.get("/iceberg/scan-files")
def scan_iceberg_files(
    namespace: str = Query(..., description="Namespace (e.g. 'nyc')"),
    table_name: str = Query(..., description="Table name (e.g. 'taxis')"),
    column: str = Query(..., description="Column to filter on (e.g. 'trip_distance')"),
    min_value: int = Query(..., description="Minimum value for filtering"),
    limit: int = Query(100, description="Limit on number of rows to scan")
):
    try:
        # Load the catalog
        creds = Creds()
        catalog = creds.catalog_valid()

        # Build table identifier
        table_identifier = f"{namespace}.{table_name}"

        # Load Iceberg table
        table = catalog.load_table(table_identifier)
        schema_obj = table.schema()

        # field_type = table.schema.find_field(column).field_type
        field_type = schema_obj.find_field(column).field_type

        if isinstance(field_type, (IntegerType, LongType)):
            cast_value = int(float(min_value))
        elif isinstance(field_type, (FloatType, DoubleType)):
            cast_value = float(min_value)
        elif isinstance(field_type, StringType):
            cast_value = str(min_value)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported column type: {field_type}")

        # Scan with filter
        scan = table.scan(
            row_filter=EqualTo(column, cast_value),
            limit=limit
        )


        # Extract file paths
        file_paths = [task.file.file_path for task in scan.plan_files()]

        return {
            "table": table_identifier,
            "filter": f"{column} >= {cast_value}",
            "file_count": len(file_paths),
            "files": file_paths,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error scanning table: {str(e)}")