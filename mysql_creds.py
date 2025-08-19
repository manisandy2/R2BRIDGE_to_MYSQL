import mysql.connector
from dotenv import load_dotenv
import os
from mysql.connector import Error
# import mysql

load_dotenv()

def mysql_connect():
    print("Connecting to MySQL database...")
    # print(os.getenv("HOST"))
    # print(os.getenv("MYSQL_USER"))
    # print(os.getenv("PASSWORD"))
    # print(os.getenv("DATABASE"))
    try:
        conn = mysql.connector.connect(
            host=os.getenv("HOST"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("PASSWORD"),
            database=os.getenv("DATABASE"),
            port=3306
        )
        if conn.is_connected():
            print("✅ MySQL connection established")
            return conn
        else:
            raise ConnectionError("❌ MySQL connection could not be established")
    except Error as e:
        print(f"❌ Error connecting to MySQL: {e}")
        return None

ALLOWED_TABLES = ["Transaction", "employees","POS_Transactions"]

class MysqlCatalog:
    def __init__(self):
        self.conn = mysql_connect()
        # self.cursor = self.conn.cursor()
        self.cursor = self.conn.cursor(dictionary=True)
        # self.table_name = "employees"
        # self.table_name = "Transaction"

    def _validate_table(self, table_name: str):
        if table_name not in ALLOWED_TABLES:
            raise ValueError(f"Invalid table name: {table_name}")

    def get_all_value(self,table_name):
        self._validate_table(table_name)
        self.cursor.execute(f"SELECT * FROM {table_name}")
        return self.cursor.fetchall()

    def get_count(self,table_name:str):
        self._validate_table(table_name)
        self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        # print("Count :",self.cursor.fetchone()["COUNT(*)"])
        # print(self.cursor.fetchone()["COUNT(*)"])
        return self.cursor.fetchone()["COUNT(*)"]

    def get_describe(self,table_name:str):
        self._validate_table(table_name)
        self.cursor.execute(f"DESCRIBE {table_name}")
        # print("Describe:",self.cursor.fetchall())
        # return self.cursor.fetchall()
        result = self.cursor.fetchall()
        # print("Describe:", result)
        return result

    def get_range(self,table_name:str, start: int, end: int):
        self._validate_table(table_name)
        self.cursor.execute(f"SELECT * FROM {table_name} LIMIT {start}, {end - start}")
        return self.cursor.fetchall()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

