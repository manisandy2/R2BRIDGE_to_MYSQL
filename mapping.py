from pyiceberg.types import *
import pyarrow as pa
import decimal


type_mapping = {
    'int': IntegerType(),
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
    'int': pa.int32(),
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


def convert_row(row, column_types):
    converted = []
    for idx, value in enumerate(row):
        col_type = column_types[idx]

        try:
            # DECIMAL handling
            if col_type.startswith("decimal"):
                if value is None:
                    converted.append(None)
                elif isinstance(value, decimal.Decimal):
                    converted.append(str(value))
                elif isinstance(value, (int, float)):
                    converted.append(str(value))
                elif isinstance(value, str):
                    converted.append(str(decimal.Decimal(value)))
                else:
                    raise TypeError(f"Unexpected type {type(value)} for decimal column")

            # BIT handling (bytes, int, bool, string)
            elif col_type == "bit":
                if value is None:
                    converted.append(None)
                elif isinstance(value, (bytes, bytearray)):
                    converted.append(int.from_bytes(value, byteorder="big") != 0)
                elif isinstance(value, int):
                    converted.append(value != 0)
                elif isinstance(value, bool):
                    converted.append(value)
                elif isinstance(value, str):
                    v = value.strip().lower()
                    if v in ("1", "true", "t", "yes", "y"):
                        converted.append(True)
                    elif v in ("0", "false", "f", "no", "n"):
                        converted.append(False)
                    else:
                        raise ValueError(f"Cannot interpret string '{value}' as bit/boolean")
                else:
                    raise TypeError(f"Unexpected type {type(value)} for bit column")

            # Default: pass value as is
            else:
                converted.append(value)

        except Exception as e:
            raise RuntimeError(
                f"Error converting column #{idx + 1} (type '{col_type}') value '{value}': {e}"
            ) from e

    return converted
