from pyiceberg.types import *
import pyarrow as pa


type_mapping = {
    'int': IntegerType(),
    'bigint': LongType(),
    'varchar': StringType(),
    'char': StringType(),
    'text': StringType(),
    'date': DateType(),
    'datetime': TimestampType(),
    'float': FloatType(),
    'double': DoubleType(),
    'boolean': BooleanType(),
    'tinyint': BooleanType()
    # Add more as needed
}

arrow_mapping = {
    'int': pa.int32(),
    'bigint': pa.int64(),
    'varchar': pa.string(),
    'char': pa.string(),
    'text': pa.string(),
    'date': pa.date32(),
    'datetime': pa.timestamp('ms'),
    'float': pa.float32(),
    'double': pa.float64(),
    'boolean': pa.bool_(),
    'tinyint': pa.bool_()
}
