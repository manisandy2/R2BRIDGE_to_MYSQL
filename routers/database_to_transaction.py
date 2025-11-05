
from fastapi import APIRouter,HTTPException,Query,Body
import time
from ..mysql_creds import *
from ..mapping import *
from ..core.catalog_client import get_catalog_client
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from datetime import datetime
from pyiceberg.table import Table
import pandas as pd
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform,YearTransform,MonthTransform,DayTransform
from pyiceberg.exceptions import BadRequestError
from pyiceberg.expressions import EqualTo,And,GreaterThanOrEqual,LessThanOrEqual,In


router = APIRouter(prefix="", tags=["Database to Transaction"])


# @router.post("/create")
# def transactions(
#     # namespace: str = Query(..., description="transactions (e.g. 'transactions')"),
#     # table_name: str = Query(..., description="transactions pos (e.g. 'transactions pos')"),
#     start_range: int = Query(0, description="Start row (e.g. 0)"),
#     end_range: int = Query(100000, description="End row (e.g. 100000)"),
#     # dbname:str = Query(..., description="Database name"),
#     metadata: Optional[Dict[str, str]] = Body(None, description="Custom metadata key/value pairs")
# ):
#     start_time = time.time()
#     mysql_creds = MysqlCatalog()
#
#     # ---------- NameSpaces and TableName ----------------
#
#     namespace,table_name = "transaction","pos_transaction"
#     dbname = "Transaction_pos"
#
#     # --- DB Fetch ---
#     print("DB Fetch")
#     step_start = time.time()
#     try:
#         description = mysql_creds.get_describe(dbname)
#         rows = mysql_creds.get_range(dbname, start_range, end_range)
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#
#     if not rows:
#         raise HTTPException(status_code=400, detail="No data found in the given range.")
#     db_fetch_time = time.time() - step_start
#
#     print("DB Fetch:",db_fetch_time)
#     print("rows:", len(rows))
#     for row in rows:
#         print("pri_id:", row["pri_id"],"Bill_Date__c",row["Bill_Date__c"])
#         if row["Bill_Date__c"]:
#             # Convert string to datetime (adjust format as needed)
#             bill_date = datetime.strptime(row["Bill_Date__c"], "%Y-%m-%d")
#             print("Type:", type(bill_date))
#             print("Date:", bill_date.year())
#             print("Date:", bill_date.month())
#             print("Date:", bill_date.day())
#         else:
#             print("Bill_Date__c is None or empty")
#
#
#     # iceberg_fields, arrow_fields = [],[]
#
    # iceberg_schema, arrow_schema = build_schemas_from_mysql(description, type_mapping, arrow_mapping)
#
#
#     # for idx, column in enumerate(description):
#     #     name = column["Field"]
#     #     col_type = column["Type"].split('(')[0].lower()
#     #     is_nullable = column["Null"].upper() == "YES"
#     #
#     #     is_primary = column["Key"] == "PRI"
#     #     is_unique = column["Key"] == "UNI"
#     #
#     #     ice_type = type_mapping.get(col_type, StringType())
#     #     arrow_type = arrow_mapping.get(col_type, pa.string())
#     #
#     #     # tu_rows.append([name, col_type,is_nullable,is_key,is_primary, str(ice_type),str(arrow_type)])
#     #
#     #     iceberg_fields.append(NestedField(field_id=idx + 1, name=name, field_type=ice_type, required=not is_nullable))
#     #     arrow_fields.append(pa.field(name, arrow_type, nullable=is_nullable))
#
#     # for idx, column in enumerate(description):
#     #     # print(f"{idx}: {column}")
#     #     name = column["Field"]
#     #
#     #     col_type = column["Type"].split('(')[0].lower()
#     #     print(f"{idx}: {name}:'|':{col_type}")
#     #     is_nullable = column["Null"].upper() == "YES"
#     #
#     #     ice_type = type_mapping.get(col_type, StringType())
#     #     arrow_type = arrow_mapping.get(col_type, pa.string())
#     #
#     #     iceberg_fields.append(
#     #         NestedField(field_id=idx + 1, name=name, field_type=ice_type, required=not is_nullable)
#     #     )
#     #     arrow_fields.append(pa.field(name, arrow_type, nullable=is_nullable))
#     #
#     # # iceberg_schema = Schema(*iceberg_schema)
#     # # arrow_schema = pa.schema(arrow_schema)
#     # iceberg_schema = Schema(*iceberg_fields)
#     # arrow_schema = pa.schema(arrow_fields)
#     # schema_build_time = time.time() - step_start
#     # print("Schema Build:",schema_build_time)
#     # print("DB all",description)
#     # print("DB single",description[0])
#     # print("DB rows",rows[0])
#     iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
#
#     ########################
#     # row
#     # pylist_rows = []
#     # for row in rows:
#     #     converted = {}
#     #     for field in arrow_schema:
#     #         val = row[field.name]
#     #
#     #         if pa.types.is_integer(field.type):
#     #             converted[field.name] = int(val) if val is not None else None
#     #         elif pa.types.is_floating(field.type):
#     #             converted[field.name] = float(val) if val is not None else None
#     #         else:
#     #             converted[field.name] = val
#     #     pylist_rows.append(converted)
#
#     # pylist_rows = []
#     # for row in rows:
#     #     converted = {}
#     #     for field in arrow_schema:
#     #         val = row[field.name]
#     #         if val in [None, "", "NULL", "null"]:
#     #             converted[field.name] = None
#     #         elif pa.types.is_integer(field.type):
#     #             try:
#     #                 converted[field.name] = int(val)
#     #             except (ValueError, TypeError):
#     #                 converted[field.name] = None
#     #         elif pa.types.is_floating(field.type):
#     #             try:
#     #                 converted[field.name] = float(val)
#     #             except (ValueError, TypeError):
#     #                 converted[field.name] = None
#     #         else:
#     #             converted[field.name] = str(val) if val is not None else None
#     #     pylist_rows.append(converted)
#
#
#     converted_records = [convert_row(r, arrow_schema) for r in rows]
#     # print("Converted Records:",converted_records)
#
#     arrow_table = pa.Table.from_pylist(converted_records, schema=arrow_schema)
#
#     catalog = get_catalog_client()
#     # catalog = creds.catalog_valid()
#     try:
#         catalog.load_namespace_properties(namespace)
#     except NoSuchNamespaceError:
#         catalog.create_namespace(namespace)
#
#     table_identifier = f"{namespace}.{table_name}"
#     try:
#         tbl = catalog.load_table(table_identifier)
#     except NoSuchTableError:
#         tbl = catalog.create_table(
#             identifier=table_identifier,
#             schema=iceberg_schema,
#
#         )
#     # tbl = get_or_create_table(catalog, table_identifier, iceberg_schema)
#     # try:
#     #     tbl = catalog.create_table(table_identifier, schema=iceberg_schema,
#     #                                properties=metadata if metadata else {})
#
#     # except Exception as e:
#     #     raise HTTPException(status_code=500, detail=f"Error creating table: {str(e)}")
#     tbl.append(arrow_table,)
#
#     elapsed = time.time() - start_time
#     return {
#         "status": "success",
#         # "action": action,
#         "namespace": namespace,
#         "table": table_name,
#         # "rows_written": len(pylist_rows),
#         "elapsed_seconds": round(elapsed, 2),
#         "schema": [f.name for f in iceberg_schema.columns],
#         "metadata": metadata or {},
#         "table_properties": tbl.properties if hasattr(tbl, "properties") else {}
#     }

# @router.post("/create")
# def transactions(
#     start_range: int = Query(0, description="Start row (e.g. 0)"),
#     end_range: int = Query(100000, description="End row (e.g. 100000)"),
#     metadata: Optional[Dict[str, str]] = Body(None, description="Custom metadata key/value pairs")
# ):
#     start_time = time.time()
#     mysql_creds = MysqlCatalog()
#
#     # ---------- Namespace & Table ----------
#     namespace, table_name = "transaction", "pos_transaction"
#     dbname = "Transaction_pos"
#
#     # ---------- Step 1: Fetch from MySQL ----------
#     print("DB Fetch started...")
#     step_start = time.time()
#     try:
#         description = mysql_creds.get_describe(dbname)
#         rows = mysql_creds.get_range(dbname, start_range, end_range)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#
#     if not rows:
#         raise HTTPException(status_code=400, detail="No data found in the given range.")
#
#     db_fetch_time = time.time() - step_start
#     print(f"DB Fetch completed in {db_fetch_time:.2f} sec")
#
#     # ---------- Step 2: Build Iceberg & Arrow Schema ----------
#     iceberg_fields, arrow_fields = [], []
#
#     for idx, column in enumerate(description):
#         name = column["Field"]
#         col_type = column["Type"].split('(')[0].lower()
#         is_nullable = column["Null"].upper() == "YES"
#         ice_type = type_mapping.get(col_type, StringType())
#         arrow_type = arrow_mapping.get(col_type, pa.string())
#
#         iceberg_fields.append(
#             NestedField(field_id=idx + 1, name=name, field_type=ice_type, required=not is_nullable)
#         )
#         arrow_fields.append(pa.field(name, arrow_type, nullable=is_nullable))
#
#         print(f"{idx}: {name}:'|':{col_type}")
#
#     iceberg_schema = Schema(*iceberg_fields)
#     arrow_schema = pa.schema(arrow_fields)
#     print("Schema built successfully.")
#
#     # ---------- Step 3: Data Type Normalization ----------
#     pylist_rows = []
#     for row in rows:
#         converted = {}
#         for field in arrow_schema:
#             val = row[field.name]
#
#             if val in [None, "", "NULL", "null"]:
#                 converted[field.name] = None
#                 continue
#
#             try:
#                 if pa.types.is_integer(field.type):
#                     converted[field.name] = int(val)
#                 elif pa.types.is_floating(field.type):
#                     converted[field.name] = float(val)
#                 else:
#                     converted[field.name] = str(val)
#             except Exception as e:
#                 print(f"Type conversion failed for column {field.name}: {val} ({e})")
#                 converted[field.name] = None
#
#         pylist_rows.append(converted)
#
#     print(f"Data normalization done for {len(pylist_rows)} rows.")
#
#     # ---------- Step 4: Create PyArrow Table ----------
#     try:
#         arrow_table = pa.Table.from_pylist(pylist_rows, schema=arrow_schema)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Arrow table conversion error: {str(e)}")
#
#     print("Arrow table created successfully.")
#
#     # ---------- Step 5: Iceberg Table Append ----------
#     catalog = get_catalog_client()
#     table_identifier = f"{namespace}.{table_name}"
#
#     try:
#         tbl = get_or_create_table(catalog, table_identifier, iceberg_schema)
#         tbl.append(arrow_table)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Iceberg table write error: {str(e)}")
#
#     elapsed = time.time() - start_time
#
#     print(f"✅ Process completed in {elapsed:.2f} sec")
#
#     # ---------- Step 6: Response ----------
#     return {
#         "status": "success",
#         "namespace": namespace,
#         "table": table_name,
#         "rows_written": len(pylist_rows),
#         "elapsed_seconds": round(elapsed, 2),
#         "schema": [f.name for f in iceberg_schema.columns],
#         "metadata": metadata or {},
#         "table_properties": getattr(tbl, "properties", {}),
#     }

# @router.post("/create")
# def transactions(
#     start_range: int = Query(0, description="Start row (e.g. 0)"),
#     end_range: int = Query(100000, description="End row (e.g. 100000)"),
# ):
#     start_time = time.time()
#     mysql_creds = MysqlCatalog()
#
#     # ---------- NameSpaces and TableName ----------------
#
#     namespace,table_name = "transaction","pos_transaction"
#     dbname = "Transaction_pos"
#
#     # --- DB Fetch ---
#     print("DB Fetch")
#     step_start = time.time()
#     try:
#         # description = mysql_creds.get_describe(dbname)
#         rows = mysql_creds.get_range(dbname, start_range, end_range)
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#
#     if not rows:
#         raise HTTPException(status_code=400, detail="No data found in the given range.")
#
#     for row in rows:
#         print("pri_id:", row["pri_id"], "Bill_Date__c:", row["Bill_Date__c"])
#
#         bill_date_str = row.get("Bill_Date__c")
#
#         if bill_date_str:
#             try:
#                 row["Bill_Date__c"] = parser.parse(bill_date_str)
#             except Exception:
#                 row["Bill_Date__c"] = None
#             else:
#                 row["Bill_Date__c"] = None
#
#     print("typing test")
#     print("#"*100)
#     # for row in rows:
#     #     print(row['Bill_Date__c'])
#     #     print(type(row['Bill_Date__c']))
#
#     iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
#     # print("schema:", iceberg_schema)
#     # Bill_Date__c = iceberg_schema.find_field("Bill_Date__c")
#
#     converted_records = [convert_row(r, arrow_schema) for r in rows]
#
#     print("Records:",converted_records)
#
#     # arrow_table = pa.Table.from_pylist(converted_records, schema=arrow_schema)
#     try:
#         arrow_table = pa.Table.from_pylist(converted_records, schema=arrow_schema)
#         # return arrow_table
#
#     except pa.lib.ArrowTypeError as e:
#         print("❌ ArrowTypeError occurred!")
#         print(e)
#
#         # Debug: check each row and each field
#         for row_idx, row in enumerate(converted_records):
#             for field in arrow_schema:
#                 val = row.get(field.name)
#                 try:
#                     pa.array([val], type=field.type)
#                 except Exception as field_e:
#                     print(f"Row {row_idx}, Field '{field.name}', Value: {val}, Type: {field.type}")
#                     print(f"  Error: {field_e}")
#
#         # raise  # re-raise after debugging
#
#     catalog = get_catalog_client()
#
#     try:
#         catalog.load_namespace_properties(namespace)
#     except NoSuchNamespaceError:
#         catalog.create_namespace(namespace)
#
#     table_identifier = f"{namespace}.{table_name}"
#
#     # try:
#     #     tbl = catalog.load_table(table_identifier)
#     # except NoSuchTableError:
#     #     tbl = catalog.create_table(
#     #         identifier=table_identifier,
#     #         schema=iceberg_schema,
#     #
#     #     )
#     year_field = iceberg_schema.find_field("Bill_Date__c")
#     month_field = iceberg_schema.find_field("Bill_Date__c")
#     day_field = iceberg_schema.find_field("Bill_Date__c")
#
#     source_id_year_field = year_field.field_id
#     source_id_month_field = month_field.field_id
#     source_id_day_field = day_field.field_id
#
#     print("#"*100)
#     print("year_field:", source_id_year_field)
#     print("month_field:", source_id_month_field)
#     print("day_field:", source_id_day_field)
#
#     try:
#         tbl = catalog.load_table(table_identifier)
#         print("✅ Table exists. Skipping creation, ready to append data.")
#     except NoSuchTableError:
#         partition_spec = PartitionSpec(
#             fields=[
#                 PartitionField(source_id=iceberg_schema.find_field("Bill_Date__c").field_id, field_id=2001,
#                                transform=IdentityTransform(), name="Bill_Date__c"),
#                 # PartitionField(source_id=source_id_month_field, field_id=2002,
#                 #                transform=MonthTransform(), name="month"),
#                 # PartitionField(source_id=source_id_day_field, field_id=2003,
#                 #                transform=DayTransform(), name="day"),
#             ]
#         )
#         # tbl = catalog.create_table(
#         #     identifier=table_identifier,
#         #     schema=iceberg_schema,
#         #     partition_spec=partition_spec,
#         #     properties={"write.partition.path-style": "directory"},
#         # )
#         try:
#             tbl = catalog.create_table(
#                 identifier=table_identifier,
#                 schema=iceberg_schema,
#                 partition_spec=partition_spec,
#                 properties={"write.partition.path-style": "directory"},
#             )
#         except BadRequestError as e:
#             # This will catch redundant partitions, invalid schema, etc.
#             print("❌ Failed to create Iceberg table!")
#             print("Error message:", str(e))
#             raise HTTPException(
#                 status_code=400,
#                 detail=f"Failed to create Iceberg table: {str(e)}"
#             )
#         except Exception as e:
#             # Catch any other unexpected errors
#             print("❌ Unexpected error while creating Iceberg table")
#             print("Error message:", str(e))
#             raise HTTPException(
#                 status_code=500,
#                 detail=f"Unexpected error while creating Iceberg table: {str(e)}"
#             )
#         else:
#             print("✅ Iceberg table created successfully")
#
#     try:
#         tbl.append(arrow_table)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to append data to Iceberg table: {str(e)}")
#
#     elapsed = time.time() - start_time
#     return {
#         "status": "success",
#         # "action": action,
#         "namespace": namespace,
#         "table": table_name,
#         # "rows_written": len(pylist_rows),
#         "elapsed_seconds": round(elapsed, 2),
#         "schema": [f.name for f in iceberg_schema.columns],
#         # "metadata": metadata or {},
#         "table_properties": tbl.properties if hasattr(tbl, "properties") else {}
#     }
# create auto shema convert datetime frame filed
# @router.post("/create")
# def transactions(
#     start_range: int = Query(0, description="Start row (e.g. 0)"),
#     end_range: int = Query(100000, description="End row (e.g. 100000)"),
# ):
#     start_time = time.time()
#     mysql_creds = MysqlCatalog()  # Your MySQL wrapper
#
#     namespace, table_name = "transaction", "pos_transaction"
#     dbname = "Transaction_pos"
#
#     # ---------- Fetch data from MySQL ----------
#     try:
#         rows = mysql_creds.get_range(dbname, start_range, end_range)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#
#     if not rows:
#         raise HTTPException(status_code=400, detail="No data found in the given range.")
#
#         # ---------- Convert date fields ----------
#     # def parse_date(val):
#     #     if isinstance(val, datetime):
#     #         return val
#     #     for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
#     #         try:
#     #             return datetime.strptime(val, fmt)
#     #         except ValueError:
#     #             continue
#     #     raise ValueError(f"Invalid date format: {val}")
#     def parse_datetime_field(value):
#         """Convert MySQL string or datetime into a Python datetime object."""
#         if isinstance(value, datetime):
#             return value
#         if value in (None, "", "NULL"):
#             return None
#
#         for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y %H:%M:%S"):
#             try:
#                 return datetime.strptime(value, fmt)
#             except (ValueError, TypeError):
#                 continue
#         return None
#
#     # ---------- Convert Bill_Date__c to timestamp ----------
#     converted_rows = []
#     for row in rows:
#         # Convert Bill_Date__c
#         row["Bill_Date__c"] = parse_datetime_field(row["Bill_Date__c"])
#
#         # Convert updated_At (and others if any)
#         if "updated_At" in row:
#             row["updated_At"] = parse_datetime_field(row["updated_At"])
#
#         # Add partition fields
#         bill_date = row["Bill_Date__c"]
#         if bill_date:
#             row["year"] = bill_date.year
#             row["month"] = bill_date.month
#             row["day"] = bill_date.day
#         converted_rows.append(row)
#
#     print("Convert:",converted_rows)
#
#
#     # ---------- Infer Iceberg / Arrow schema ----------
#     iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
#
#     # ---------- Convert records to match Arrow schema ----------
#     converted_records = [convert_row(r, arrow_schema) for r in converted_rows]
#
#     try:
#         arrow_table = pa.Table.from_pylist(converted_records, schema=arrow_schema)
#     except pa.lib.ArrowTypeError as e:
#         # Debug row/field causing error
#         for row_idx, row in enumerate(converted_records):
#             for field in arrow_schema:
#                 val = row.get(field.name)
#                 try:
#                     pa.array([val], type=field.type)
#                 except Exception as field_e:
#                     print(f"Row {row_idx}, Field '{field.name}', Value: {val}, Type: {field.type}")
#                     print(f"  Error: {field_e}")
#         raise HTTPException(status_code=400, detail=f"Arrow conversion error: {str(e)}")
#
#     # ---------- Iceberg catalog ----------
#     catalog = get_catalog_client()
#     try:
#         catalog.load_namespace_properties(namespace)
#     except NoSuchNamespaceError:
#         catalog.create_namespace(namespace)
#
#     table_identifier = f"{namespace}.{table_name}"
#
#     # ---------- Create table if not exists with partitions ----------
#     try:
#         tbl = catalog.load_table(table_identifier)
#         print("✅ Table exists. Ready to append data.")
#     except NoSuchTableError:
#         # Partition: year/month/day from Bill_Date__c timestamp
#         partition_spec = PartitionSpec(
#             fields=[
#                 PartitionField(
#                     source_id=iceberg_schema.find_field("year").field_id,
#                     field_id=2001,
#                     transform=IdentityTransform(),
#                     name="year"
#                 ),
#                 PartitionField(
#                     source_id=iceberg_schema.find_field("month").field_id,
#                     field_id=2002,
#                     transform=IdentityTransform(),
#                     name="month"
#                 ),
#                 PartitionField(
#                     source_id=iceberg_schema.find_field("day").field_id,
#                     field_id=2003,
#                     transform=IdentityTransform(),
#                     name="day"
#                 ),
#             ]
#         )
#
#
#         tbl = catalog.create_table(
#             identifier=table_identifier,
#             schema=iceberg_schema,
#             partition_spec=partition_spec,
#             properties={"write.partition.path-style": "directory"},
#         )
#         print("✅ Iceberg table created successfully")
#
#
#     # ---------- Append data ----------
#     try:
#         tbl.append(arrow_table)
#         tbl.refresh()
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to append data to Iceberg table: {str(e)}")
#
#     elapsed = time.time() - start_time
#     return {
#         "status": "success",
#         "namespace": namespace,
#         "table": table_name,
#         "rows_written": len(converted_records),
#         "elapsed_seconds": round(elapsed, 2),
#         "schema": [f.name for f in iceberg_schema.columns],
#         "table_properties": getattr(tbl, "properties", {}),
#     }

# string split year month day pos transaction
# @router.post("/create")
# def transaction(
#     start_range: int = Query(0, description="Start row (e.g. 0)"),
#     end_range: int = Query(100000, description="End row (e.g. 100000)"),
# ):
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()  # Your MySQL wrapper
#
#     namespace, table_name = "transaction", "pos_transaction_01"
#     # dbname = "Transaction_pos"
#     dbname = "Transaction"
#
#     # ---------- Fetch data from MySQL ----------
#
#     db_fetch_start = time.time()
#     try:
#         rows = mysql_creds.get_range(dbname, start_range, end_range)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#     print("MySQL fetch", time.time() - db_fetch_start)
#     if not rows:
#         raise HTTPException(status_code=400, detail="No data found in the given range.")
#
#     # ---------- Convert Bill_Date__c to timestamp ----------
#     convert_start = time.time()
#     converted_rows = []
#     for row in rows:
#         bill_date = row.get("Bill_Date__c","")
#
#         row["year"] = int(bill_date[:4])
#         row["month"] = int(bill_date[5:7])
#         row["day"] = int(bill_date[8:10])
#         converted_rows.append(row)
#     # print("rows", converted_rows)
#     print("Bill_Date__c conversion",  time.time() - convert_start)
#     schema_start = time.time()
#     # ---------- Infer Iceberg / Arrow schema ----------
#     iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
#     print("Schema inference", schema_start)
#     # ---------- Convert records to match Arrow schema ----------
#     arrow_conv_start = time.time()
#     converted_records = [convert_row(r, arrow_schema) for r in converted_rows]
#
#     print("Records conversion to Arrow",  time.time() - arrow_conv_start  )
#     # ---------- Create Arrow Table ----------
#     arrow_table_start = time.time()
#
#     try:
#         arrow_table = pa.Table.from_pylist(converted_records, schema=arrow_schema)
#     except pa.lib.ArrowTypeError as e:
#         # Debug row/field causing error
#         for row_idx, row in enumerate(converted_records):
#             for field in arrow_schema:
#                 val = row.get(field.name)
#                 try:
#                     pa.array([val], type=field.type)
#                 except Exception as field_e:
#                     print(f"Row {row_idx}, Field '{field.name}', Value: {val}, Type: {field.type}")
#                     print(f"  Error: {field_e}")
#         raise HTTPException(status_code=400, detail=f"Arrow conversion error: {str(e)}")
#     print("Arrow table creation", time.time() - arrow_table_start)
#     catalog_start = time.time()
#     # ---------- Iceberg catalog ----------
#     catalog = get_catalog_client()
#     try:
#         catalog.load_namespace_properties(namespace)
#     except NoSuchNamespaceError:
#         catalog.create_namespace(namespace)
#
#     table_identifier = f"{namespace}.{table_name}"
#     print("Iceberg catalog setup", time.time() -catalog_start)
#     # ---------- Create table if not exists with partitions ----------
#     table_start = time.time()
#     try:
#         tbl = catalog.load_table(table_identifier)
#         print("Table exists. Ready to append data.")
#     except NoSuchTableError:
#         partition_spec = PartitionSpec(
#             fields=[
#                 PartitionField(
#                     source_id=iceberg_schema.find_field("year").field_id,
#                     field_id=2001,
#                     transform=IdentityTransform(),
#                     name="year"
#                 ),
#                 PartitionField(
#                     source_id=iceberg_schema.find_field("month").field_id,
#                     field_id=2002,
#                     transform=IdentityTransform(),
#                     name="month"
#                 ),
#                 # PartitionField(
#                 #     source_id=iceberg_schema.find_field("day").field_id,
#                 #     field_id=2003,
#                 #     transform=IdentityTransform(),
#                 #     name="day"
#                 # ),
#             ]
#         )
#
#
#         tbl = catalog.create_table(
#             identifier=table_identifier,
#             schema=iceberg_schema,
#             partition_spec=partition_spec,
#             properties={"write.partition.path-style": "directory"},
#         )
#         print("✅ Iceberg table created successfully")
#         print("Iceberg table creation/load", time.time() - table_start)
#
#     append_start = time.time()
#     # ---------- Append data ----------
#     try:
#         tbl.append(arrow_table)
#         tbl.refresh()
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to append data to Iceberg table: {str(e)}")
#
#     print("Append data to Iceberg", time.time() - append_start)
#     elapsed = time.time() - total_start
#     return {
#         "status": "success",
#         "namespace": namespace,
#         "table": table_name,
#         "rows_written": len(converted_records),
#         "elapsed_seconds": round(elapsed, 2),
#         "schema": [f.name for f in iceberg_schema.columns],
#         "table_properties": getattr(tbl, "properties", {}),
#     }
# version 01
# transaction data invalid literal for int() with base 10: '6/24'
# row-wise
@router.post("/create")
def transaction(
    start_range: int = Query(0, description="Start row (e.g. 0)"),
    end_range: int = Query(100000, description="End row (e.g. 100000)"),
):
    total_start = time.time()
    mysql_creds = MysqlCatalog()  # Your MySQL wrapper

    namespace, table_name = "pos_transactions", "transaction"
    # dbname = "Transaction_pos"
    dbname = "Transaction"

    # ---------- Fetch data from MySQL ----------

    db_fetch_start = time.time()
    try:
        rows = mysql_creds.get_range(dbname, start_range, end_range)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
    # print("MySQL fetch", time.time() - db_fetch_start)
    if not rows:
        raise HTTPException(status_code=400, detail="No data found in the given range.")

    def safe_parse_date(value):
        """Try multiple formats or datetime types for Bill_Date__c"""
        if isinstance(value, datetime):
            return value  # already a datetime object
        if not value:
            return None

        # Try common string formats
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(value[:10], fmt)
            except Exception:
                continue
        return None  # fallback if nothing matches

    # ---------- Convert Bill_Date__c to timestamp ----------
    convert_start = time.time()
    converted_rows = []
    for row in rows:
        bill_date = row.get("Bill_Date__c","")
        dt = safe_parse_date(bill_date)
        if dt:
            row["year"] = dt.year
            row["month"] = dt.month
            row["day"] = dt.day
        else:
            row["year"] = row["month"] = row["day"] = None  # fallback for invalid date

        converted_rows.append(row)



    # print("rows", converted_rows)
    # print("Bill_Date__c conversion",  time.time() - convert_start)
    # schema_start = time.time()
    # print("###" * 50)
    # print("infer_schema ", )
    # ---------- Infer Iceberg / Arrow schema ----------
    # iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
    # print("Schema inference", schema_start)
    # ---------- Convert records to match Arrow schema ----------
    arrow_conv_start = time.time()
    # print("###"*50)
    # print("infer_schema Arrow conversion", )
    # converted_records = [convert_row(r, arrow_schema) for r in converted_rows]
    converted_records = [convert_column(r, arrow_schema) for r in converted_rows]

    # print("Records conversion to Arrow",  time.time() - arrow_conv_start  )
    # ---------- Create Arrow Table ----------
    arrow_table_start = time.time()

    try:
        arrow_table = pa.Table.from_pylist(converted_records, schema=arrow_schema)
    except pa.lib.ArrowTypeError as e:
        # Debug row/field causing error
        for row_idx, row in enumerate(converted_records):
            for field in arrow_schema:
                val = row.get(field.name)
                try:
                    pa.array([val], type=field.type)
                except Exception as field_e:
                    print(f"Row {row_idx}, Field '{field.name}', Value: {val}, Type: {field.type}")
                    print(f"  Error: {field_e}")
        raise HTTPException(status_code=400, detail=f"Arrow conversion error: {str(e)}")
    # print("Arrow table creation", time.time() - arrow_table_start)
    catalog_start = time.time()
    # ---------- Iceberg catalog ----------
    catalog = get_catalog_client()
    try:
        catalog.load_namespace_properties(namespace)
    except NoSuchNamespaceError:
        catalog.create_namespace(namespace)

    table_identifier = f"{namespace}.{table_name}"
    # print("Iceberg catalog setup", time.time() -catalog_start)
    # ---------- Create table if not exists with partitions ----------
    table_start = time.time()
    try:
        tbl = catalog.load_table(table_identifier)
        # print("Table exists. Ready to append data.")
    except NoSuchTableError:
        partition_spec = PartitionSpec(
            fields=[
                PartitionField(
                    source_id=iceberg_schema.find_field("year").field_id,
                    field_id=2001,
                    transform=IdentityTransform(),
                    name="year"
                ),
                PartitionField(
                    source_id=iceberg_schema.find_field("month").field_id,
                    field_id=2002,
                    transform=IdentityTransform(),
                    name="month"
                ),
                # PartitionField(
                #     source_id=iceberg_schema.find_field("day").field_id,
                #     field_id=2003,
                #     transform=IdentityTransform(),
                #     name="day"
                # ),
                # PartitionField(
                #     source_id=iceberg_schema.find_field("pri_id").field_id,
                #     field_id=2003,
                #     transform=IdentityTransform(),
                #     name="day"
                # ),
            ]
        )


        tbl = catalog.create_table(
            identifier=table_identifier,
            schema=iceberg_schema,
            partition_spec=partition_spec,
            properties={"write.partition.path-style": "directory"},
        )
        print("✅ Iceberg table created successfully")
        # print("Iceberg table creation/load", time.time() - table_start)




    append_start = time.time()
    # ---------- Append data ----------
    try:
        tbl.append(arrow_table)
        tbl.refresh()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to append data to Iceberg table: {str(e)}")

    # print("Append data to Iceberg", time.time() - append_start)
    elapsed = time.time() - total_start
    return {
        "status": "success",
        "namespace": namespace,
        "table": table_name,
        "rows_written": len(converted_records),
        "elapsed_seconds": round(elapsed, 2),
        "schema": [f.name for f in iceberg_schema.columns],
        "table_properties": getattr(tbl, "properties", {}),
    }

# Version 01 update code
# @router.put("/update")
# def update_transactions(
#     # namespace: str = Query(..., description="transactions (e.g. 'transactions')"),
#     # table_name: str = Query(..., description="transactions pos (e.g. 'transactions pos')"),
#     start_range: int = Query(0, description="Start row (e.g. 0)"),
#     end_range: int = Query(100000, description="End row (e.g. 100000)"),
#     # dbname: str = Query(..., description="Database name")
# ):
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()
#
#     namespace, table_name = "pos_transactions", "transaction"
#     dbname = "Transaction"
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
#     print("rows:",len(rows))
#     for row in rows:
#         print("pri_id:", row[0])
#
#     print("Schema Build")
#     step_start = time.time()
#
#     iceberg_fields = []
#     arrow_fields = []
#
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
#     # 3 Using PyArrow Column - Wise Conversion
#
#     def convert_table_columnwise(table: pa.Table):
#         converted_cols = {}
#         for field in table.schema:
#             col = table[column_name := field.name]
#             if pa.types.is_integer(field.type):
#                 converted_cols[column_name] = col.cast(pa.int64())
#             elif pa.types.is_floating(field.type):
#                 converted_cols[column_name] = col.cast(pa.float64())
#             else:
#                 converted_cols[column_name] = col  # keep original type
#         return pa.table(converted_cols)
#
#     # df = table.to_pandas()  # Convert Arrow Table to Pandas DataFrame
#     #
#     # for column in df.columns:
#     #     if pa.types.is_integer(table.schema.field(column).type):
#     #         df[column] = df[column].astype('Int64')  # nullable integer
#     #     elif pa.types.is_floating(table.schema.field(column).type):
#     #         df[column] = df[column].astype(float)
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


# @router.post("/create")
# def source_to_target_table(
#         source_table_identifier: str = Query(..., description="Source table identifier (namespace.table)"),
#         destination_table_identifier: str = Query(..., description="Destination table identifier (namespace.table)")
# ):
#     try:
#         catalog = get_catalog_client()  # Your catalog client
#         source_table: Table = catalog.load_table(source_table_identifier)
#         target_table: Table = catalog.load_table(destination_table_identifier)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to load tables: {str(e)}")
#
#     copied_files = []
#     errors = []
#
#     for task in source_table.scan().plan_files():
#         try:
#             file_path = task.file.file_path
#             # Read file (add storage_options if needed, e.g., S3 credentials)
#             df = pd.read_parquet(file_path)
#
#             # Append to target table using Arrow Table (more memory efficient)
#             target_table.new_append().append(df.to_dict(orient="records")).commit()
#
#             copied_files.append(file_path)
#         except Exception as e:
#             errors.append({"file": task.file.file_path, "error": str(e)})
#
#     return {
#         "source_table": source_table_identifier,
#         "destination_table": destination_table_identifier,
#         "copied_files_count": len(copied_files),
#         "errors": errors
#     }
# @router.post("/create")
# def transaction(
#     start_range: int = Query(0, description="Start row (e.g. 0)"),
#     end_range: int = Query(100000, description="End row (e.g. 100000)")
# ):
#     total_start = time.time()
#     mysql_creds = MysqlCatalog()
#
#     namespace, table_name, dbname = "pos_transactions", "transaction", "Transaction"
#
#     # ---------- Fetch data ----------
#     db_fetch_start = time.time()
#     try:
#         rows = mysql_creds.get_range(dbname, start_range, end_range)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
#
#     print(f"MySQL fetch time: {round(time.time() - db_fetch_start, 2)} sec")
#     if not rows:
#         raise HTTPException(status_code=400, detail="No data found in the given range.")
#
#     # ---------- Transform rows ----------
#     convert_start = time.time()
#     converted_rows = []
#     for row in rows:
#         bill_date = row.get("Bill_Date__c")
#         dt = safe_parse_date(bill_date)
#         if dt:
#             row["year"], row["month"], row["day"] = dt.year, dt.month, dt.day
#         else:
#             row["year"], row["month"], row["day"] = None, None, None
#
#         converted_rows.append(convert_row(row))
#     print(f"Row conversion time: {round(time.time() - convert_start, 2)} sec")
#
#     # ---------- Schema & Arrow Table ----------
#     iceberg_schema = infer_schema(converted_rows[0])
#     try:
#         arrow_table = pa.Table.from_pylist(converted_rows, schema=schema)
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=f"Arrow table creation error: {str(e)}")
#
#     print(f"Arrow table rows: {arrow_table.num_rows}, columns: {arrow_table.num_columns}")
#
#     # ---------- Iceberg Catalog Setup ----------
#     catalog = get_catalog_client()
#     try:
#         catalog.load_namespace_properties(namespace)
#     except NoSuchNamespaceError:
#         catalog.create_namespace(namespace)
#
#     table_identifier = f"{namespace}.{table_name}"
#
#     try:
#         tbl = catalog.load_table(table_identifier)
#         print("Table found — appending data.")
#     except NoSuchTableError:
#         partition_spec = PartitionSpec(
#             fields=[
#                 PartitionField(
#                     source_id=iceberg_schema.find_field("year").field_id,
#                     field_id=2001,
#                     transform=IdentityTransform(),
#                     name="year"
#                 ),
#                 PartitionField(
#                     source_id=iceberg_schema.find_field("month").field_id,
#                     field_id=2002,
#                     transform=IdentityTransform(),
#                     name="month"
#                 ),
#             ]
#         )
#         tbl = catalog.create_table(
#             identifier=table_identifier,
#             schema=iceberg_schema,
#             partition_spec=partition_spec,
#             properties={"write.partition.path-style": "directory"},
#         )
#         print("✅ Iceberg table created successfully.")
#
#     # ---------- Append to Iceberg ----------
#     append_start = time.time()
#     try:
#         tbl.append(arrow_table)
#         tbl.refresh()
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to append data: {str(e)}")
#
#     print(f"Append time: {round(time.time() - append_start, 2)} sec")
#
#     # ---------- Summary ----------
#     elapsed = round(time.time() - total_start, 2)
#     return {
#         "status": "success",
#         "namespace": namespace,
#         "table": table_name,
#         "rows_written": arrow_table.num_rows,
#         "elapsed_seconds": elapsed,
#         "schema_fields": [f.name for f in schema],
#     }


@router.post("/update")
def update_transaction(
    start_range: int = Query(0, description="Start row (e.g. 0)"),
    end_range: int = Query(100000, description="End row (e.g. 100000)"),
):
    total_start = time.time()
    mysql_creds = MysqlCatalog()  # Your MySQL wrapper

    namespace, table_name = "pos_transactions", "transaction"
    # dbname = "Transaction_pos"
    dbname = "Transaction"

    # ---------- Fetch data from MySQL ----------

    db_fetch_start = time.time()
    try:
        rows = mysql_creds.get_range(dbname, start_range, end_range)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL fetch error: {str(e)}")
    print("MySQL fetch", time.time() - db_fetch_start)
    if not rows:
        raise HTTPException(status_code=400, detail="No data found in the given range.")

    def safe_parse_date(value):
        """Try multiple formats or datetime types for Bill_Date__c"""
        if isinstance(value, datetime):
            return value  # already a datetime object
        if not value:
            return None
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(value[:10], fmt)
            except Exception:
                continue
        return None  # fallback if nothing matches

    # ---------- Convert Bill_Date__c to timestamp ----------
    convert_start = time.time()
    converted_rows = []
    for row in rows:
        bill_date = row.get("Bill_Date__c","")
        dt = safe_parse_date(bill_date)
        if dt:
            row["year"] = dt.year
            row["month"] = dt.month
            row["day"] = dt.day
        else:
            row["year"] = row["month"] = row["day"] = None  # fallback for invalid date
        converted_rows.append(row)



    # print("rows", converted_rows)
    print("Bill_Date__c conversion",  time.time() - convert_start)
    schema_start = time.time()
    # ---------- Infer Iceberg / Arrow schema ----------
    iceberg_schema, arrow_schema = infer_schema_from_record(rows[0])
    print("Schema inference", schema_start)
    # ---------- Convert records to match Arrow schema ----------
    arrow_conv_start = time.time()
    # converted_records = [convert_row(r, arrow_schema) for r in converted_rows]
    converted_records = [convert_column(r, arrow_schema) for r in converted_rows]

    print("Records conversion to Arrow",  time.time() - arrow_conv_start  )
    # ---------- Create Arrow Table ----------
    arrow_table_start = time.time()

    try:
        arrow_table = pa.Table.from_pylist(converted_records, schema=arrow_schema)
    except pa.lib.ArrowTypeError as e:
        # Debug row/field causing error
        for row_idx, row in enumerate(converted_records):
            for field in arrow_schema:
                val = row.get(field.name)
                try:
                    pa.array([val], type=field.type)
                except Exception as field_e:
                    print(f"Row {row_idx}, Field '{field.name}', Value: {val}, Type: {field.type}")
                    print(f"  Error: {field_e}")
        raise HTTPException(status_code=400, detail=f"Arrow conversion error: {str(e)}")
    print("Arrow table creation", time.time() - arrow_table_start)
    catalog_start = time.time()
    # ---------- Iceberg catalog ----------
    catalog = get_catalog_client()
    try:
        catalog.load_namespace_properties(namespace)
    except NoSuchNamespaceError:
        catalog.create_namespace(namespace)

    table_identifier = f"{namespace}.{table_name}"
    print("Iceberg catalog setup", time.time() -catalog_start)
    # ---------- Create table if not exists with partitions ----------
    table_start = time.time()

    try:
        tbl = catalog.load_table(table_identifier)
        print("Table exists. Ready to append data.")
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Iceberg table '{table_identifier}' not found.")



    append_start = time.time()
    # ---------- Append data ----------
    try:
        tbl.append(arrow_table)
        tbl.refresh()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to append data to Iceberg table: {str(e)}")

    print("Append data to Iceberg", time.time() - append_start)
    elapsed = time.time() - total_start
    return {
        "status": "success",
        "namespace": namespace,
        "table": table_name,
        "rows_written": len(converted_records),
        "elapsed_seconds": round(elapsed, 2),
        "schema": [f.name for f in iceberg_schema.columns],
        "table_properties": getattr(tbl, "properties", {}),
    }


@router.get("/filter")
def dynamic_filter(
    namespace: str = Query(..., description="Namespace (e.g. 'crm')"),
    table_name: str = Query(..., description="Table name (e.g. 'serial_number_requests')"),
    columns: list[str] = Query(..., description="Columns to filter on (comma-separated or multiple query params)"),
    values: list[str] = Query(..., description="Values to match (in same order as columns)"),

    # credentials: HTTPAuthorizationCredentials = Depends(security)
):

    # verify_jwt(credentials.credentials)
    # try:
    #     verify_jwt(credentials.credentials)
    # except Exception as e:
    #     raise HTTPException(
    #         status_code=401,
    #         detail=f"Invalid token: {str(e)}"
    #     )

    start_time = time.time()

    # Handle comma-separated inputs
    if len(columns) == 1 and ',' in columns[0]:
        columns = [c.strip() for c in columns[0].split(',')]
    if len(values) == 1 and ',' in values[0] and len(columns) > 1:
        values = [v.strip() for v in values[0].split(',')]

    try:
        # Check lengths
        if len(columns) != len(values):
            raise HTTPException(
                status_code=400,
                detail={
                    "status": "error",
                    "error_code": "MISMATCHED_COLUMNS_VALUES",
                    "message": "Number of columns and values must match",
                    "data": [],
                    "status_code": 400
                }
            )

        # Load table
        # catalog = get_catalog_client()
        # table = catalog.load_table((namespace, table_name))
        try:
            catalog = get_catalog_client()
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail={
                    "status": "error",
                    "error_code": "CATALOG_CONNECTION_FAILED",
                    "message": f"Failed to connect to catalog: {str(e)}",
                    "data": [],
                    "status_code": 500
                }
            )
        try:
            table = catalog.load_table((namespace, table_name))
        except Exception as e:
            raise HTTPException(
                status_code=404,
                detail={
                    "status": "error",
                    "error_code": "TABLE_NOT_FOUND",
                    "message": f"Table '{namespace}.{table_name}' not found or could not be loaded: {str(e)}",
                    "data": [],
                    "status_code": 404
                }
            )

        # Validate columns
        schema_fields = {f.name for f in table.schema().fields}
        for col in columns:
            if col not in schema_fields:
                raise HTTPException(
                    status_code=404,
                    detail={
                        "status": "error",
                        "error_code": "COLUMN_NOT_FOUND",
                        "message": f"Column '{col}' not found in schema",
                        "data": [],
                        "status_code": 404
                    }
                )
        filters = None
        for col, val in zip(columns, values):
            if col == "Bill_Date__c" and ',' in val:
                start_date, end_date = [v.strip() for v in val.split(',')]
                condition = And(
                    GreaterThanOrEqual(col, start_date),
                    LessThanOrEqual(col, end_date)
                )
            else:
                condition = EqualTo(col, val)
            filters = condition if filters is None else And(filters, condition)

        # # Apply filter
        scan = table.scan(row_filter=filters)
        arrow_table = scan.to_arrow()

        rows = []
        for batch in arrow_table.to_batches():
            rows.extend(batch.to_pylist())

        elapsed = round(time.time() - start_time, 2)

        if len(rows) == 0:
            raise HTTPException(
                status_code=404,
                detail={
                    "status": "error",
                    "error_code": "NO_DATA",
                    "message": "No data found",
                    "data": [],
                    "status_code": 404
                }
            )

        return {
            "status": "success",
            "status_code": 200,
            "count": len(scan.to_arrow()),
            # "data": scan.to_arrow().to_pylist(),
            "execution_time_seconds": elapsed
        }

    except HTTPException as http_err:
        # Re-raise known HTTP errors
        raise http_err

    except Exception as e:
        # Catch any unknown errors
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "error_code": "INTERNAL_ERROR",
                "message": str(e),
                "data": [],
                "status_code": 500
            }
        )


@router.get("/filter-business")
def filter_business_logic(
    namespace: str = Query(..., description="Namespace (e.g. 'transactions')"),
    table_name: str = Query(..., description="Table name (e.g. 'pos')"),
    store_codes: list[str] = Query(..., description="List of store codes"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)")
):
    start_time = time.time()

    try:
        # Step 1: Load Iceberg table
        catalog = get_catalog_client()
        table = catalog.load_table((namespace, table_name))

        # Step 2: Build filter
        filters = And(
            In("store_code__c", store_codes),
            GreaterThanOrEqual("Bill_Date__c", start_date),
            LessThanOrEqual("Bill_Date__c", end_date)
        )
        print(filters)
        # Step 3: Scan and load data
        scan = table.scan(row_filter=filters)
        arrow_table = scan.to_arrow()
        df = arrow_table.to_pandas()
        print(df)
        if df.empty:
            raise HTTPException(
                status_code=404,
                detail={
                    "status": "error",
                    "error_code": "NO_DATA",
                    "message": "No records found for the given filters.",
                    "data": [],
                    "status_code": 404
                }
            )

        # Step 4: Business logic aggregation
        agg_df = df.groupby("store_code__c").agg(
            total_transactions=("store_code__c", "count"),
            Mobile_count=("customer_mobile__c", "nunique"),
            Invoice_count=("bill_transaction_no__c", "nunique"),
            Invoice_gross_amount=("item_gross_amount__c", "sum"),
            Invoice_tax_amount=("item_tax__c", "sum"),
        ).reset_index()

        agg_df["Invoice_total_with_tax"] = (
            agg_df["Invoice_gross_amount"] + agg_df["Invoice_tax_amount"]
        )

        elapsed = round(time.time() - start_time, 2)

        # Step 5: Return JSON
        return {
            "status": "success",
            "status_code": 200,
            "execution_time_seconds": elapsed,
            "count": len(agg_df),
            "data": agg_df.to_dict(orient="records")
        }

    except HTTPException as http_err:
        raise http_err
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "error_code": "INTERNAL_ERROR",
                "message": str(e),
                "data": [],
                "status_code": 500
            }
        )