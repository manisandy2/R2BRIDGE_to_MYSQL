from credentials import MysqlCatalog
from mapping import type_mapping,arrow_mapping
from pyiceberg.types import *
import time
import pyarrow as pa
from pyiceberg.schema import Schema
import os
from pyiceberg.catalog.rest import RestCatalog
from tabulate import tabulate
start_time = time.time()

mysqlCreds = MysqlCatalog()
# print(mysqlCreds.get_count())
# print(len(mysqlCreds.get_describe()))
# print(mysqlCreds.get_describe())
description = mysqlCreds.get_describe()
rows = mysqlCreds.get_range(1,100000)

iceberg_fields = []
arrow_fields = []
# tu_rows = []
for idx,column in enumerate(description):
    name = column[0]
    col_type = column[1].split('(')[0].lower()
    is_nullable = column[2].upper() == "YES"

    is_primary = column[3]
    is_key = column[4]

    ice_type = type_mapping.get(col_type, StringType())
    arrow_type = arrow_mapping.get(col_type, pa.string())

    # tu_rows.append([name, col_type,is_nullable,is_key,is_primary, str(ice_type),str(arrow_type)])

    iceberg_fields.append(NestedField(field_id=idx + 1, name=name, field_type=ice_type, required=not is_nullable))

    arrow_fields.append(pa.field(name, arrow_type, nullable=is_nullable))

# print(tabulate(tu_rows, headers=["name", "type", "nullable","primary","key","ice_type","arrow_type"], tablefmt="grid"))

# print(tabulate(iceberg_fields, headers=["field_id",'name','field_type','required'],tablefmt="github"))
# print(tabulate(arrow_fields, headers=['field.name','field.type','field.nullable'],tablefmt="github"))
# arrow_rows = []
# for field in arrow_fields:
#     arrow_rows.append([
#         field.name,
#         str(field.type),
#         field.nullable,
#         field.metadata
#     ])
#
# print(tabulate(
#     arrow_rows,
#     headers=['name', 'arrow_type', 'nullable', 'metadata'],
#     tablefmt="github"
# ))

iceberg_schema = Schema(*iceberg_fields)
arrow_schema = pa.schema(arrow_fields)

column_names = [desc[0] for desc in description]
# # print(column_names)
#
# column_types = [col["type"].lower() for col in arrow_mapping]
# # print(column_types)
#
# converted_rows = [convert_row(row, column_types) for row in rows]
# print(converted_rows)

# arrow_schema = pa.schema(arrow_fields)
# iceberg_schema = Schema(*[
#     (idx + 1, col[0], type_mapping[col[1].split("(")[0].lower()])
#     for idx, col in enumerate(description)
# ])

# print("Arrow Schema:\n", arrow_schema)
# print("\nIceberg Schema:\n", iceberg_schema)

# Convert MySQL rows to a list of dictionaries for PyArrow
# pylist_rows = [dict(zip(column_names, row)) for row in rows]
#
# # Create PyArrow Table
# arrow_table = pa.Table.from_pylist(pylist_rows, schema=arrow_schema)
# print(len(rows))
# # Output
# # print("Arrow Schema:\n", arrow_schema)
# # print("\nIceberg Schema:\n", iceberg_schema)
# # print("\nArrow Table Preview:\n", arrow_table.to_pandas().head())
#
# print(arrow_table.to_pandas().head())

pylist_rows = []
for row in rows:
    converted = {}
    # print(row)
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
# print(arrow_table.to_pandas().head())

CATALOG_URI = os.getenv("CATALOG_URI")
WAREHOUSE = os.getenv("WAREHOUSE")
TOKEN = os.getenv("TOKEN")


catalog = RestCatalog(
    name="test02",
    warehouse=WAREHOUSE,
    uri=CATALOG_URI,
    token=TOKEN,
)


table_identifier = "employees.people46"
tbl = catalog.create_table(table_identifier, schema=iceberg_schema)
tbl.append(arrow_table)
# print(tbl.schema)
elapsed = time.time() - start_time
print(f"✅ Finished loading '{table_identifier}' in {elapsed:.2f} seconds")