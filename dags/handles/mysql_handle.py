import mysql.connector
from loguru import logger
import os
from dotenv import load_dotenv

class MySQLHandle:
    def __init__(self, host, port, user, passwd, database):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.database = database
        self.connection = None
        self.engine = None
       
    def create_connection(self):
        self.connection = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.passwd,
            database=self.database
        )
        if self.connection:
            logger.info(f"Connected to MySql")
        else:
            logger.error("Something went wrong!")
    
    def query_data(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        cursor.close()
    
if __name__ == "__main__":

    load_dotenv()

    host = os.getenv('HOST')
    port = os.getenv('PORT')
    user = os.getenv('USER')
    passwd = os.getenv('PASSWORD')
    database = os.getenv('DATABASE')
    
    creds = MySQLHandle(host, port, user, passwd, database)

    creds.create_connection()
