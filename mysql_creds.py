import mysql.connector
# import mysql.connector
from dotenv import load_dotenv
import os
import json

load_dotenv()

def mysql_connect():
    return mysql.connector.connect(
        host=os.getenv("HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("PASSWORD"),
        database=os.getenv("DATABASE"),
    )


class MysqlCatalog:
    def __init__(self):
        self.conn = mysql_connect()
        self.cursor = self.conn.cursor()
        # return self

    def close(self):
        self.cursor.close()
        self.conn.close()

    def get_employees(self):
        self.cursor.execute("SELECT * FROM employees")
        return self.cursor.fetchall()

    def get_count(self):
        self.cursor.execute("SELECT COUNT(*) FROM employees")
        return self.cursor.fetchone()[0]

    def get_describe(self):
        self.cursor.execute("DESCRIBE employees")
        return self.cursor.fetchall()

    # def get_range(self, start: int, end: int):
    #     # cursor = self.connection.cursor()
    #     self.cursor.execute(f"SELECT * FROM employees LIMIT {start}, {end - start}")
    #     print(self.cursor.fetchall())
    #     return self.cursor.fetchall()

    
    def get_range(self, start: int, end: int):
        self.cursor.execute(f"SELECT * FROM employees LIMIT {start}, {end - start}")
        # return  json.dumps(self.cursor.fetchall(), indent=2)
        return self.cursor.fetchall()

    def get_limits(self, limit: int):
        query = "SELECT * FROM employees LIMIT %s"
        self.cursor.execute(query, (limit,))
        return self.cursor.fetchall()

    def close(self):
        self.cursor.close()
        self.conn.close()

sql = MysqlCatalog()
# employees = mysql.get_employees()
# print(employees)
count = sql.get_count()
print(count)
describe = sql.get_describe()
print(describe)