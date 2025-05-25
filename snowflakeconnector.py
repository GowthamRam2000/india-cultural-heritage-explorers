import snowflake.connector
from config import SNOWFLAKE_CONFIG
import pandas as pd

class SnowflakeConnection:
    def __init__(self):
        self.config = SNOWFLAKE_CONFIG
        self.connection = None

    def connect(self):
        try:
            self.connection = snowflake.connector.connect(
                account=self.config['account'],
                user=self.config['user'],
                password=self.config['password'],
                warehouse=self.config['warehouse'],
                database=self.config['database'],
                schema=self.config['schema']
            )
            return self.connection
        except Exception as e:
            print(f"Error connecting to Snowflake: {e}")
            return None

    def execute_query(self, query):
        if not self.connection:
            self.connect()

        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            return cursor
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def fetch_dataframe(self, query):
        cursor = self.execute_query(query)
        if cursor:
            df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
            cursor.close()
            return df
        return pd.DataFrame()

    def close(self):
        if self.connection:
            self.connection.close()


def test_connection():
    snow = SnowflakeConnection()
    conn = snow.connect()
    if conn:
        print("Successfully connected to Snowflake!")
        cursor = snow.execute_query("SELECT CURRENT_VERSION()")
        print(f"Snowflake version: {cursor.fetchone()[0]}")
        snow.close()
    else:
        print("Failed to connect to Snowflake")


if __name__ == "__main__":
    test_connection()