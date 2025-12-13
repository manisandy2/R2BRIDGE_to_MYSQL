# import pyodbc
#
# print(pyodbc.version)
# print(pyodbc.drivers())
# conn_str = (
#     "DRIVER=ODBC Driver 18 for SQL Server;"
#     "SERVER=10.8.1.18,1433;"
#     "DATABASE=Deal1071;"
#     "UID=Devlog;"
#     "PWD=Zkc4DrUG)$tX9paX;"
#     "Encrypt=no;"
#     "TrustServerCertificate=yes;"
# )
# conn = pyodbc.connect(conn_str)


# import pandas as pd
# from sqlalchemy import create_engine
#
# SERVER="10.8.1.18"
# PORT="1433"
# DATABASE="Deal1071"
# UID="Devlog"
# PWD="Zkc4DrUG)$tX9paX"
#
# # Format: mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+18+for+SQL+Server
# engine = create_engine(
#     f"mssql+pyodbc://{UID}:{PWD}@{SERVER}:{PORT}/{DATABASE}?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=no"
# )
#
# query = f"""SELECT SAFDNO AS INVOICE_NO,SADATE AS INVOICE_DATE,PRPCOD AS ITEM_CODE,PRDESC AS ITEM_NAME,OMVALU AS DISCOUNT_PURPOSE,H5DAMT AS DISCOUNT_AMOUNT FROM  DLSIDP,DLINVO,DLBOTT,DLPROD,DLOMAS WHERE H5INVO = SAINVO AND OMOMAS = H5DPUR  AND OMHARD ='DPUR' AND SAINVO = BOINVO AND BOPROD = PRPROD AND BOPROD = H5PROD
#         AND SADATE >= 20250801 AND SADATE <= 20250831"""
# df = pd.read_sql(query, engine)
#
# print(df.head())