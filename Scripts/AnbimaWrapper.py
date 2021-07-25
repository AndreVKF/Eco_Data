import pandas as pd
import numpy as np

from dateutil.relativedelta import relativedelta
import datetime as dt

class AnbimaWrapper:
    def __init__(self, dbEngine):
        """Anbima Wrapper Constructor

        Args:
            dbEngine (sql connector engine): Database engine
        """
        self.dbEngine = dbEngine
        self.conn = self.dbEngine.connect()
        
        # Variables from database
        self.Funds = pd.read_sql("SELECT * FROM Funds", con=self.dbEngine)
        
        # Class variables
        self.monthDict = {
            1: "01"
            ,2: "02"
            ,3: "03"
            ,4: "04"
            ,5: "05"
            ,6: "06"
            ,7: "07"
            ,8: "08"
            ,9: "09"
            ,10: "10"
            ,11: "11"
            ,12: "12"
        }
        
    def updateFundsData(self, iniDate=dt.date.today(), endDate=dt.date.today(), lastAvailable=False, returnDataFrame=False):
        """Function to update fund data on database

        Args:
            iniDate (str("%Y%m%d") || datetime.date): Initial date to update data
            endDate (str("%Y%m%d") || datetime.date): Final date to update data
            lastAvailable (bool, optional): Update only last available data. Defaults to False.
            returnDataFrame (bool, optional): Flag to return dataframe with fund data
        """
        
        # Adjust date in case for update lastAvailable only
        if lastAvailable:
            endDate = dt.date.today()
            iniDate = (dt.date(year=endDate.year, month=endDate.month, day=1)-relativedelta(months=1))
        else:
            if isinstance(iniDate, str): iniDate = dt.datetime.strptime(iniDate, "%Y%m%d").date()
            if isinstance(endDate, str): endDate = dt.datetime.strptime(endDate, "%Y%m%d").date()
            
        # Adjust for first date available on Anbima
        if iniDate < dt.date(2017, 1, 1): iniDate = dt.date(2017, 1, 1)
        if endDate < dt.date(2017, 1 , 1): return print("Date previous of 2017-01-01 not available!!")
        
        # DF to be inserted to DB
        funds_dataDF = pd.DataFrame(columns=["Refdate", "Id_Fund", "Quote_Value", "Captc_Dia", "Resg_Dia"])
        
        # Loop through periods in order to get fund data
        for countYear in range(iniDate.year, endDate.year+1, 1):
            # Check if month from iniDate
            if countYear==iniDate.year:
                monthIni = iniDate.month
            else:
                monthIni = 1
                
            for countMonth in range(monthIni, 13, 1):
                # Loop date overpass current date
                if dt.date(countYear, countMonth, 1)>dt.date(endDate.year, endDate.month, 1): break
                
                # Creat url and retrieve data
                url = f"http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{str(countYear)}{self.monthDict[countMonth]}.csv"
                print(f"Retrieving data for: {str(countYear)}-{self.monthDict[countMonth]}")
                anbimaData = pd.read_csv(url, delimiter=";", parse_dates=["DT_COMPTC"])
                
                # Data handling
                rawData = anbimaData.merge(self.Funds[["CNPJ", "Id"]].rename(columns={"Id": "Id_Fund", "CNPJ": "CNPJ_FUNDO"}), how="left", on="CNPJ_FUNDO")
                rawData = rawData.loc[~rawData["Id_Fund"].isnull()].rename(columns={"DT_COMPTC": "Refdate", "VL_QUOTA": "Quote_Value", "CAPTC_DIA": "Captc_Dia", "RESG_DIA": "Resg_Dia"})
                rawData = rawData[["Refdate", "Id_Fund", "Quote_Value", "Captc_Dia", "Resg_Dia"]]
                
                funds_dataDF = pd.concat([funds_dataDF, rawData])
        
        # Adjusta dates on dataframe
        if lastAvailable:
            lastDate = funds_dataDF.sort_values(by=["Refdate"], ascending=False).iloc[0, 0]
            funds_dataDF = funds_dataDF.loc[funds_dataDF["Refdate"]==lastDate]
        else:
            funds_dataDF = funds_dataDF.loc[(funds_dataDF["Refdate"]>=iniDate.strftime("%Y-%m-%d"))&(funds_dataDF["Refdate"]<=endDate.strftime("%Y-%m-%d"))]
        
        # Delete previous data
        RefdateLst = funds_dataDF['Refdate'].drop_duplicates().to_list()
        deleteQuery = f"""
            DELETE FROM 
                funds_data 
            WHERE
                Refdate IN ({str(RefdateLst).replace("[", "").replace("]", "")})
            """
        self.conn.execute(deleteQuery)
        
        # Insert data into DataBase
        funds_dataDF.to_sql(name="funds_data", con=self.dbEngine, if_exists="append", index=False)
        
        # Return data frame
        if returnDataFrame: return funds_dataDF