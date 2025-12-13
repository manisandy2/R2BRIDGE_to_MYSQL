from pyiceberg.types import *
import pyarrow as pa
import decimal
import json
import datetime
from botocore.exceptions import ClientError, BotoCoreError
from fastapi import HTTPException
from datetime import datetime
from pyiceberg.schema import Schema

type_mapping = {
    "int": LongType(),
    'bigint': LongType(),
    'varchar': StringType(),
    'char': StringType(),
    'text': StringType(),
    'longtext': StringType(),
    'date': DateType(),
    'datetime': TimestampType(),
    'timestamp': TimestampType(),
    'float': FloatType(),
    'double': DoubleType(),
    'boolean': BooleanType(),
    'tinyint': BooleanType()
}

arrow_mapping = {
    # 'int': pa.int32(),
    "int": pa.int64(),
    'bigint': pa.int64(),
    'varchar': pa.string(),
    'char': pa.string(),
    'text': pa.string(),
    'longtext': pa.string(),
    'date': pa.date32(),
    'datetime': pa.timestamp('ms'),
    'timestamp': pa.timestamp('ms'),
    'float': pa.float32(),
    'double': pa.float64(),
    'boolean': pa.bool_(),
    'tinyint': pa.bool_(),
    'bit': pa.bool_(),
    # 'decimal': lambda p=18, s=6: pa.decimal128(p, s)
    'decimal' : pa.decimal128(18, 6)
}

def safe_parse_date(value):
    """Try to parse date in multiple formats."""
    if isinstance(value, datetime):
        return value
    if not value:
        return None

    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(str(value)[:10], fmt)
        except Exception:
            continue
    return None

def build_arrow_table(rows):
    if not rows:
        return None

    fields = []
    first_row = rows[0]
    for col in first_row.keys():
        if col == "pri_id":
            fields.append(pa.field(col, pa.int64()))
        else:
            fields.append(pa.field(col, pa.string()))

    schema = pa.schema(fields)
    converted_rows = [convert_row(r) for r in rows]
    return pa.Table.from_pylist(converted_rows, schema=schema)

# def convert_row(row, column_types):
#     converted = []
#     for idx, value in enumerate(row):
#         col_type = column_types[idx]
#
#         try:
#             # DECIMAL handling
#             if col_type.startswith("decimal"):
#                 if value is None:
#                     converted.append(None)
#                 elif isinstance(value, decimal.Decimal):
#                     converted.append(str(value))
#                 elif isinstance(value, (int, float)):
#                     converted.append(str(value))
#                 elif isinstance(value, str):
#                     converted.append(str(decimal.Decimal(value)))
#                 else:
#                     raise TypeError(f"Unexpected type {type(value)} for decimal column")
#
#             # BIT handling (bytes, int, bool, string)
#             elif col_type == "bit":
#                 if value is None:
#                     converted.append(None)
#                 elif isinstance(value, (bytes, bytearray)):
#                     converted.append(int.from_bytes(value, byteorder="big") != 0)
#                 elif isinstance(value, int):
#                     converted.append(value != 0)
#                 elif isinstance(value, bool):
#                     converted.append(value)
#                 elif isinstance(value, str):
#                     v = value.strip().lower()
#                     if v in ("1", "true", "t", "yes", "y"):
#                         converted.append(True)
#                     elif v in ("0", "false", "f", "no", "n"):
#                         converted.append(False)
#                     else:
#                         raise ValueError(f"Cannot interpret string '{value}' as bit/boolean")
#                 else:
#                     raise TypeError(f"Unexpected type {type(value)} for bit column")
#
#             # Default: pass value as is
#             else:
#                 converted.append(value)
#
#         except Exception as e:
#             raise RuntimeError(
#                 f"Error converting column #{idx + 1} (type '{col_type}') value '{value}': {e}"
#             ) from e
#
#     return converted
# def convert_row(row):
#     numeric_int_fields = {"pri_id", "IsDeleted", "Invoice_Amount__c"}
#     numeric_float_fields = {"Bill_Grant_Total__c"}
#     converted = {}
#     for key, val in row.items():
#         # Handle NULL / None / empty
#         if val in (None, "", "NULL"):
#             converted[key] = None
#             continue
#
#         # Integer fields
#         if key in numeric_int_fields:
#             try:
#                 converted[key] = int(val)
#             except Exception:
#                 converted[key] = None
#
#         # Float fields
#         elif key in numeric_float_fields:
#             try:
#                 converted[key] = float(val)
#             except Exception:
#                 converted[key] = None
#
#         # Everything else as string
#         else:
#             converted[key] = str(val)
#
#     return converted

# def infer_schema(row_sample):
#     """Infer PyArrow schema (pri_id=long, others=string)."""
#     fields = []
#     for key in row_sample.keys():
#         # if key == "pri_id" or key in {"IsDeleted", "Invoice_Amount__c", "year", "month", "day"}:
#         if key == "pri_id" or key in {"IsDeleted", "Invoice_Amount__c"}:
#             fields.append(pa.field(key, pa.int64()))
#         elif key == "Bill_Grant_Total__c":
#             fields.append(pa.field(key, pa.float64()))
#         else:
#             fields.append(pa.field(key, pa.string()))
#     return pa.schema(fields)

# def convert_column(row: dict, arrow_schema: pa.Schema) -> dict:
#     converted = {}
#
#     for field in arrow_schema:
#         name = field.name
#         dtype = field.type
#         val = row.get(name)
#         print("name", name, "dtype", dtype, "val", val)
#         # Handle None / Empty
#         if val in (None, "", "NULL"):
#             converted[name] = None
#             continue
#
#         # ---- Type-based Conversion ----
#         try:
#             # Integer
#             if pa.types.is_integer(dtype):
#                 converted[name] = int(val)
#
#             # Floating point
#             elif pa.types.is_floating(dtype):
#                 converted[name] = float(val)
#
#             # Boolean
#             elif pa.types.is_boolean(dtype):
#                 converted[name] = str(val).lower() in ("true", "1", "yes")
#
#             # Timestamp / Date
#             elif pa.types.is_timestamp(dtype) or "date" in name.lower():
#                 if isinstance(val, str):
#                     val = datetime.fromisoformat(val[:19]) if len(val) >= 10 else None
#                 if isinstance(val, datetime):
#                     converted[name] = val.strftime("%Y-%m-%d %H:%M:%S")
#                     converted[f"{name}_year"] = val.year
#                     converted[f"{name}_month"] = val.month
#                     converted[f"{name}_day"] = val.day
#                 else:
#                     converted[name] = None
#
#             # String / Bytes
#             elif pa.types.is_string(dtype):
#                 converted[name] = str(val)
#
#             else:
#                 converted[name] = str(val)
#
#         except Exception as e:
#             print("Failed to convert column", row)
#
#             print("name", name, "dtype", dtype, "val", val ,{e})
#             converted[name] = None
#
#     return converted

# def convert_row(row,arrow_schema):
#     converted = {}
#     for field in arrow_schema:
#         val = row.get(field.name)
#         if pa.types.is_integer(field.type):
#             converted[field.name] = int(val) if val is not None else None
#         elif pa.types.is_floating(field.type):
#             converted[field.name] = float(val) if val is not None else None
#         elif pa.types.is_boolean(field.type):
#             converted[field.name] = bool(val) if val is not None else None
#         else:
#             converted[field.name] = str(val) if val is not None else None
#     return converted
#
# def convert_column(row: dict, arrow_schema: pa.Schema) -> dict:
#     converted = {}
#
#     for field in arrow_schema:
#         name = field.name
#         dtype = field.type
#         val = row.get(name)
#
#         # Handle None / Empty
#         if val in (None, "", "NULL"):
#             converted[name] = None
#             continue
#
#         # ---- Special Field Handling ----
#         if name == "IsDeleted":
#             converted[name] = int(val) if str(val).isdigit() else 0
#             continue
#
#         if name in ("Invoice_Amount__c", "Bill_Grant_Total__c"):
#             try:
#                 converted[name] = float(val)
#             except (ValueError, TypeError):
#                 converted[name] = None
#             continue
#
#         # ---- Type-based Conversion ----
#         try:
#             # Integer
#             if pa.types.is_integer(dtype):
#                 converted[name] = int(val)
#
#             # Floating point
#             elif pa.types.is_floating(dtype):
#                 converted[name] = float(val)
#
#             # Boolean
#             elif pa.types.is_boolean(dtype):
#                 converted[name] = str(val).lower() in ("true", "1", "yes")
#
#             # Timestamp / Date / Datetime
#             elif pa.types.is_timestamp(dtype) or "date" in name.lower():
#                 if isinstance(val, str):
#                     # Handle ISO or YYYY-MM-DD formats
#                     val = datetime.fromisoformat(val[:19]) if len(val) >= 10 else None
#                 if isinstance(val, datetime):
#                     converted[name] = val.strftime("%Y-%m-%d %H:%M:%S")
#                     converted[f"{name}_year"] = val.year
#                     converted[f"{name}_month"] = val.month
#                     converted[f"{name}_day"] = val.day
#                 else:
#                     converted[name] = None
#
#             # Default → String
#             else:
#                 converted[name] = str(val)
#
#         except Exception:
#             converted[name] = None
#
#     return converted

# def convert_column(row: dict, arrow_schema: pa.Schema) -> dict:
#     converted = {}
#
#     for field in arrow_schema:
#         field_name = field.name
#         field_type = field.type
#         val = row.get(field_name)
#
#         if val in (None, "", "NULL"):
#             converted[field_name] = None
#             continue
#         if field.name == "IsDeleted":
#             converted[field.name] = int(val) if str(val).isdigit() else 0
#
#         elif field.name == "Invoice_Amount__c":
#             try:
#                 converted[field.name] = float(val)
#             except:
#                 converted[field.name] = None
#         elif field.name == "Bill_Grant_Total__c":
#             try:
#                 converted[field.name] = float(val)
#             except:
#                 converted[field.name] = None
#
#         # ---- Integer ----
#         if pa.types.is_integer(field_type):
#             try:
#                 converted[field_name] = int(val)
#             except (ValueError, TypeError):
#                 converted[field_name] = None
#
#         # ---- Float ----
#         elif pa.types.is_floating(field_type):
#             try:
#                 converted[field_name] = float(val)
#             except (ValueError, TypeError):
#                 converted[field_name] = None
#
#         # ---- Boolean ----
#         elif pa.types.is_boolean(field_type):
#             # converted[field_name] = bool(val)
#             converted[field_name] = str(val).lower() in ("true", "1", "yes")
#
#         # ---- Timestamp / DateTime ----
#         elif pa.types.is_timestamp(field_type) or "date" in field_name.lower():
#             try:
#                 # Convert to datetime if string
#                 if isinstance(val, str):
#                     val = datetime.fromisoformat(val[:19]) if len(val) >= 10 else None
#                 # If already datetime
#                 if isinstance(val, datetime):
#                     converted[field_name] = val.strftime("%Y-%m-%d %H:%M:%S")
#                     converted[f"{field_name}_year"] = val.year
#                     converted[f"{field_name}_month"] = val.month
#                     converted[f"{field_name}_day"] = val.day
#                 else:
#                     converted[field_name] = None
#             except Exception:
#                 converted[field_name] = None
#
#         # ---- Default (string) ----
#         else:
#             converted[field_name] = str(val)
#
#     return converted

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="ignore")
        return super().default(obj)



def upload_file(r2_client, bucket, r2_key, body):
    try:
        r2_client.put_object(Bucket=bucket, Key=r2_key, Body=body)
        return r2_key
    except ClientError as e:
        raise HTTPException(status_code=400, detail=f"R2 Client error for {r2_key}: {e.response['Error']['Message']}")

    except BotoCoreError as e:
        raise HTTPException(status_code=500, detail=f"R2 BotoCore error for {r2_key}: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error for {r2_key}: {str(e)}")


# original
# def infer_schema_from_record(record: dict):
#     """
#     Infer PyIceberg and PyArrow schemas from a sample record.
#
#     Args:
#         record (dict): Dictionary representing one record with field names and values.
#
#     Returns:
#         tuple: (iceberg_schema, arrow_schema)
#     """
#     iceberg_fields = []
#     arrow_fields = []
#
#     for idx, (name, value) in enumerate(record.items(), start=1):
#
#         # print(f"Inferring {idx}:{name}: {value}")
#         print(f"Infer Schema")
#         print(f"idx: {idx}, name: {name}, value: {value}")
#
#         if isinstance(value, bool):
#             ice_type = BooleanType()
#             arrow_type = pa.bool_()
#         elif isinstance(value, int):
#             ice_type = LongType()
#             arrow_type = pa.int64()
#         elif isinstance(value, float):
#             ice_type = DoubleType()
#             arrow_type = pa.float64()
#         # elif isinstance(value, datetime):
#         #     ice_type = TimestampType()
#         #     arrow_type = pa.timestamp("ms")
#         # elif isinstance(value, str):
#         #     parsed_dt = None
#         #     # Try to parse date-like strings
#         #     for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y"):
#         #         try:
#         #             parsed_dt = datetime.strptime(value, fmt)
#         #             ice_type = TimestampType()
#         #             arrow_type = pa.timestamp("ms")
#         #             record[name] = parsed_dt
#         #             break
#         #         except ValueError:
#         #             try:
#         #                 parsed_dt = datetime.strptime(value, "%Y-%m-%d")
#         #                 ice_type = TimestampType()
#         #                 arrow_type = pa.timestamp("ms")
#         #                 record[name] = parsed_dt
#         #             except ValueError:
#         #                 ice_type = StringType()
#         #                 arrow_type = pa.string()
#         #
#         #     # If not a date string, treat as plain text
#         #     if not parsed_dt:
#         #         ice_type = StringType()
#         #         arrow_type = pa.string()
#
#         else:
#             ice_type = StringType()
#             arrow_type = pa.string()
#             # Timestamp types
#
#
#         # Iceberg schema field
#         # if name in ["Bill_Date__c","updated_At"]:
#         #     iceberg_fields.append(
#         #         NestedField(field_id=idx,name=name, type=TimestampType(),required=False))
#         #     arrow_fields.append(pa.field(name, pa.timestamp('ms'), nullable=True))
#         # else:
#         #     iceberg_fields.append(
#         #         NestedField(field_id=idx, name=name, field_type=ice_type, required=False)
#         #     )
#         #     arrow_fields.append(pa.field(name, arrow_type, nullable=True))
#
#         iceberg_fields.append(
#             NestedField(field_id=idx, name=name, field_type=ice_type, required=False)
#         )
#         arrow_fields.append(pa.field(name, arrow_type, nullable=True))
#     iceberg_schema = Schema(*iceberg_fields)
#     arrow_schema = pa.schema(arrow_fields)
#
#     # iceberg_schema.
#
#     # print("fun iceberg_schema",iceberg_schema)
#
#     return iceberg_schema, arrow_schema

from pyiceberg.exceptions import NoSuchTableError

def get_or_create_table(catalog, table_identifier,iceberg_schema):

    try:
        tbl = catalog.load_table(table_identifier)
        # print(f"✅ Table '{table_identifier}' loaded successfully.")
    except NoSuchTableError:
        tbl = catalog.create_table(table_identifier, schema=iceberg_schema)
        # print(f"📌 Table '{table_identifier}' created successfully.")

    return tbl


from fastapi import HTTPException

def fetch_mysql_data(mysql_creds, dbname: str, start_range: int, end_range: int):

    try:
        description = mysql_creds.get_describe(dbname)
        rows = mysql_creds.get_range(dbname, start_range, end_range)
        return description, rows
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"MySQL fetch error for db={dbname}, range=({start_range}, {end_range}): {str(e)}"
        )


def build_schemas_from_mysql(description, type_mapping, arrow_mapping):
    iceberg_fields = []
    arrow_fields = []

    for idx, column in enumerate(description):
        name = column["Field"]
        col_type = column["Type"].split("(")[0].lower()   # extract base type
        is_nullable = column["Null"].upper() == "YES"

        # MySQL key info (not used yet, but available)
        is_primary = column["Key"] == "PRI"
        is_unique = column["Key"] == "UNI"

        # Map to Iceberg + Arrow
        ice_type = type_mapping.get(col_type, StringType())
        arrow_type = arrow_mapping.get(col_type, pa.string())

        iceberg_fields.append(
            NestedField(
                field_id=idx + 1,
                name=name,
                field_type=ice_type,
                required=not is_nullable
            )
        )
        arrow_fields.append(
            pa.field(
                name,
                arrow_type,
                nullable=is_nullable
            )
        )

    iceberg_schema = Schema(*iceberg_fields)
    arrow_schema = pa.schema(arrow_fields)

    return iceberg_schema, arrow_schema


def convert_column(row: dict, arrow_schema: pa.Schema) -> dict:
    converted = {}

    for field in arrow_schema:
        name = field.name
        dtype = field.type
        val = row.get(name)
        # print("name", name, "dtype", dtype, "val", val)
        # Handle None / Empty
        if val in (None, "", "NULL"):
            converted[name] = None
            continue

        # ---- Type-based Conversion ----
        try:
            # Integer
            if pa.types.is_integer(dtype):
                converted[name] = int(val)

            # Floating point
            elif pa.types.is_floating(dtype):
                converted[name] = float(val)

            # Boolean
            elif pa.types.is_boolean(dtype):
                converted[name] = str(val).lower() in ("true", "1", "yes")

            # Timestamp / Date
            elif pa.types.is_timestamp(dtype) or "date" in name.lower():
                if isinstance(val, str):
                    val = datetime.fromisoformat(val[:19]) if len(val) >= 10 else None
                if isinstance(val, datetime):
                    converted[name] = val.strftime("%Y-%m-%d %H:%M:%S")
                    converted[f"{name}_year"] = val.year
                    converted[f"{name}_month"] = val.month
                    converted[f"{name}_day"] = val.day
                else:
                    converted[name] = None

            # String / Bytes
            elif pa.types.is_string(dtype):
                converted[name] = str(val)

            else:
                converted[name] = str(val)

        except Exception as e:
            # print("Failed to convert column", row)
            #
            # print("name", name, "dtype", dtype, "val", val ,{e})
            converted[name] = None

    return converted


def infer_schema_from_record(record: dict):

    iceberg_fields = []
    arrow_fields = []

    for idx, (name, value) in enumerate(record.items(), start=1):

        # print(f"Inferring {idx}:{name}: {value}")
        # print(f"Infer Schema")
        # print(f"idx: {idx}, name: {name}, value: {value}")

        # if name in ("pri_id", "tid", "Customer_Code_New"):
        #     ice_type = LongType()
        #     arrow_type = pa.int64()
        # elif name in ("IsDeleted", "Invoice_Amount__c"):
        #     ice_type = LongType()
        #     arrow_type = pa.int64()
        # elif name in ("Bill_Grant_Total__c",):
        #     ice_type = FloatType()
        #     arrow_type = pa.float64()
        # elif name in ("updated_At",):
        #     ice_type = TimestampType()
        #     arrow_type = pa.timestamp('ms')
        # else:
        #     ice_type = StringType()
        #     arrow_type = pa.string()

        if name in ("pri_id",):
            ice_type = LongType()
            arrow_type = pa.int64()
            # NestedField(field_id=idx + 1, name=name, field_type=ice_type, required=True)
            # iceberg_fields.append(
            #     NestedField(field_id=idx, name=name, field_type=ice_type, required=True)
            # )
            # arrow_fields.append(pa.field(name, arrow_type, nullable=False))
        else:
            ice_type = StringType()
            arrow_type = pa.string()

        iceberg_fields.append(
            NestedField(field_id=idx, name=name, field_type=ice_type, required=False)
        )
        arrow_fields.append(pa.field(name, arrow_type, nullable=True))
    iceberg_schema = Schema(*iceberg_fields)
    arrow_schema = pa.schema(arrow_fields)


    return iceberg_schema, arrow_schema