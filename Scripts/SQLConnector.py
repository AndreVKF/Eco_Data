import pandas as pd

from sqlalchemy import create_engine

class mySQLConnector:
    """
    Object to connect and workaround with mySQL DB
    """    
    def __init__(self, schema, host='localhost:3308', user='root', password='1234'):
        """Builder function

        Args:
            schema (str): Database name
            host (str, optional): Host connection. Defaults to '127.0.0.1'.
            user (str, optional): User name. Defaults to 'root'.
            password (str, optional): User password. Defaults to ''.
        """        
        self.schema = schema
        self.host = host
        self.user = user
        self.password = password

        self.mySQLConnectStr = f"mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.schema}"

    def createEngine(self):
        """Function to generate engine object to work with mySQL DB

        Returns:
            [sqlalchemy engine]: Object for connecting with mySQLDB
        """        

        return create_engine(self.mySQLConnectStr)

