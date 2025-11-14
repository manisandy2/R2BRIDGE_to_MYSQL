# import duckdb
#
# r1 = duckdb.sql("SELECT 42 AS i")
# duckdb.sql("SELECT i * 2 AS k FROM r1").show()

# import duckdb
# import pandas as pd
#
# pandas_df = pd.DataFrame({"a": [42]})
# duckdb.sql("SELECT * FROM pandas_df").show()

# import duckdb
#
# # create a connection to a file called 'file.db'
# con = duckdb.connect("file.db")
# # create a table and load data into it
# con.sql("CREATE TABLE test (i INTEGER)")
# con.sql("INSERT INTO test VALUES (42)")
# # query the table
# con.table("test").show()
# # explicitly close the connection
# con.close()
# # Note: connections also closed implicitly when they go out of scope

# import duckdb
#
# with duckdb.connect("data/file.db") as con:
#     con.sql("CREATE TABLE test01 (i INTEGER)")
#     con.sql("INSERT INTO test VALUES (42)")
#     con.table("test01").show()
    # the context manager closes the connection automatically

import duckdb

# con = duckdb.connect("data/catalog.db")
con = duckdb.connect("data/pos_transactions.duckdb")
print(con.execute("SHOW TABLES").fetchdf())