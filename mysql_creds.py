import mysql.connector
from dotenv import load_dotenv
import os
from mysql.connector import Error
# import mysql

load_dotenv()

def mysql_connect():
    print("Connecting to MySQL database...")
    print(os.getenv("HOST"))

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



class MysqlCatalog:
    def __init__(self):
        self.conn = mysql_connect()
        # self.cursor = self.conn.cursor()
        self.cursor = self.conn.cursor(dictionary=True)


    # def _validate_table(self, table_name: str):
    #     if table_name not in ALLOWED_TABLES:
    #         raise ValueError(f"Invalid table name: {table_name}")

    def get_all_value(self,table_name):
        # self._validate_table(table_name)
        self.cursor.execute(f"SELECT * FROM {table_name}")
        return self.cursor.fetchall()

    def get_count(self,table_name:str):
        # self._validate_table(table_name)
        self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        # print("Count :",self.cursor.fetchone()["COUNT(*)"])
        # print(self.cursor.fetchone()["COUNT(*)"])
        return self.cursor.fetchone()["COUNT(*)"]

    def get_describe(self,table_name:str):
        # self._validate_table(table_name)
        self.cursor.execute(f"DESCRIBE {table_name}")
        # print("Describe:",self.cursor.fetchall())
        # return self.cursor.fetchall()
        result = self.cursor.fetchall()
        # print("Describe:", result)
        return result

    def get_range(self,table_name:str, start: int, end: int):
        # self._validate_table(table_name)
        # self.cursor.execute(f"SELECT * FROM {table_name} LIMIT {start}, {end - start}")
        query = f"""
                SELECT * 
                FROM {table_name}
                ORDER BY pri_id ASC
                LIMIT {start}, {end - start}
            """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    # def get_range_ph_bi(self, table_name: str, start: int, end: int):
    #     # Validate table name (optional)
    #     # self._validate_table(table_name) Bill_Date__c
    #     if end <= start:
    #         raise ValueError("`end` must be greater than `start`")
    #
    #     query = f"""
    #             SELECT
    #                 # pri_id,
    #                 store_code__c,
    #                 customer_mobile__c,
    #                 Bill_No__c,
    #                 Invoice_Amount__c,
    #                 Invoice_Date__c,
    #                 Bill_Date__c
    #             FROM {table_name}
    #             ORDER BY pri_id ASC
    #             LIMIT %s, %s
    #     """
    #     # self.cursor.execute(query)
    #     self.cursor.execute(query, (start, end - start))
    #     return self.cursor.fetchall()
    def get_range_ph_bi(self, table_name: str, start: int, end: int):

        try:
            # self.cursor.execute(f"USE {dbname};")
            query = f"""
                SELECT
                    pri_id,
                    store_code__c,
                    Branch_Name__c,
                    customerId,
                    customer_mobile__c,
                    Customer_Name__c,
                    Bill_No__c,
                    Bill_Date__c,
                    Invoice_Amount__c,
                    bill_status__c,
                    bill_transaction_no__c,
                    Item_Code__c,
                    Item_Name__c,
                    bill_tax__c,
                    bill_grand_total__c,
                    CreatedDate

                FROM {table_name}
                ORDER BY pri_id ASC
                LIMIT %s, %s
            """


            self.cursor.execute(query, (start, end - start))
            return self.cursor.fetchall()
        except Exception as e:
            print(f"MySQL fetch error in get_range_ph_bi: {e}")
            return []

    def get_one_pri_id(self, table_name: str, pri_id_value: int):
        try:
            query = f"""
                SELECT
                    pri_id,
                    store_code__c,
                    Branch_Name__c,
                    customer_mobile__c,
                    Customer_Name__c,
                    Bill_No__c,
                    Bill_Date__c,
                    Invoice_Date__c,
                    Invoice_Amount__c,
                    bill_status__c,
                    bill_transaction_no__c,
                    Item_Code__c,
                    Item_Name__c,
                    bill_tax__c,
                    bill_grand_total__c,
                    CreatedDate
                FROM {table_name}
                WHERE pri_id = %s
                LIMIT 1
            """

            self.cursor.execute(query, (pri_id_value,))
            return self.cursor.fetchone()
        except Exception as e:
            print(f"MySQL fetch error in get_one_ph_bi: {e}")
            return None




# cc = MysqlCatalog()
#
# print(cc.get_one_pri_id("Transaction",10000000))

