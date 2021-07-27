import pandas as pd
import numpy as np

from YFinanceWrapper import YFinanceWrapper
from AnbimaWrapper import AnbimaWrapper

from SQLConnector import mySQLConnector

class Controller():
    
    def __init__(self, dbEngine):
        """Constructor

        Args:
            dbEngine (dbEngine): dbEngine object
        """
        self.dbEngine = dbEngine
        
        self.YFinanceWrapper = YFinanceWrapper(self.dbEngine)
        self.AnbimaWrapper = AnbimaWrapper(self.dbEngine)
        
    def updateLastData(self):
        """Function to update last available data for:

            * YFinance Equities Data
            * Anbima Funds Data
        """
        # Update Yahoo finance data
        try:
            self.YFinanceWrapper.updatePriceHistory(byPeriodType="period", tickers="All", period="1d")
        except:
            print("Error while updating YFinance data!!!")
            pass
        
        # Update Anbima data
        try:
            self.AnbimaWrapper.updateFundsData(lastAvailable=True)
        except:
            print("Error while updating Anbima data!!!")
            pass
