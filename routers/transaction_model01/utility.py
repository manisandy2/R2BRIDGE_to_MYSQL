from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from ..core.catalog_client import get_catalog_client


catalog = get_catalog_client()
# table = catalog.load_table(f"{namespace}.{table_name}")

namespace = "pos_transaction"
source_table_name = "transaction"
target_table_name = "transaction_copy"
# print(catalog.catalog_valid())
# Load the source table
source_table = catalog.load_table(f"{namespace}.{source_table_name}")

# Create the new table with the same schema and properties
catalog.create_table(
    identifier=f"{namespace}.{target_table_name}",
    schema=source_table.schema(),
    partition_spec=source_table.spec(),
    properties=source_table.properties
)

# Write data to new table
target_table = catalog.load_table(f"{namespace}.{target_table_name}")
for file in source_table.snapshots():
    target_table.append_files(file.manifest_list)