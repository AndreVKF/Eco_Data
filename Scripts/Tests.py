import pandas as pd
import mysql.connector as sql

import yfinance as yf

from dateutil.relativedelta import relativedelta
import datetime as dt

import pymysql
from sqlalchemy import create_engine

from SQLConnector import mySQLConnector
from YFinanceWrapper import YFinanceWrapper
from AnbimaWrapper import AnbimaWrapper

mySQLConnector = mySQLConnector(schema='eco_data')
engine = mySQLConnector.createEngine()
yFinWrapper = YFinanceWrapper(dbEngine=engine)
AnbimaWrapper = AnbimaWrapper(dbEngine=engine)

productsList = yFinWrapper.Products["yFinanceCode"].to_list()
yFinWrapper.updatePriceHistory(tickers=productsList, byPeriodType="period", period="2d")

ret_DF = AnbimaWrapper.updateFundsData(lastAvailable=True, returnDataFrame=True)

lastDate = ret_DF.sort_values(by=["Refdate"], ascending=False).iloc[0, 0]
ret_DF.loc[ret_DF["Refdate"]==lastDate]

iniDate="20170101"
dt.datetime.strptime(iniDate, "%Y%m%d")

url = "http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_202107.csv"
df = pd.read_csv(url, delimiter=";")

td = dt.date.today()

tdStr = '20210723'
dt.datetime.strptime(tdStr, '%Y%m%d')

td - relativedelta(days=3)

# Variaveis
dtType = "years"
dtDiff = 1

# Fund List
fundsDB = pd.read_sql("SELECT * FROM Funds", con=engine)

# Variables
td = dt.date.today()
monthDict = {
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

if dtDiff<0: print(1/0)

if dtType == "years":
    dtIni = td - relativedelta(years=dtDiff)
elif dtType == "months":
    dtIni = td - relativedelta(months=dtDiff)
elif dtType == "days":
    dtIni = td - relativedelta(days=dtDiff)
else:
    print(1/0)

if dtIni < dt.date(2017, 1, 1): dtIni = dt.date(2017, 1, 1)

# DF to be inserted to DB
funds_dataDF = pd.DataFrame(columns=["Refdate", "Id_Fund", "Quote_Value", "Captc_Dia", "Resg_Dia"])
    
for countYear in range(dtIni.year, td.year+1, 1):
    # Check if month from dtIni
    if countYear==dtIni.year:
        monthIni = dtIni.month
    else:
        monthIni = 1
        
    for countMonth in range(monthIni, 13, 1):
        # Loop date overpass current date
        if dt.date(countYear, countMonth, 1)>dt.date(td.year, td.month, 1): break
        
        # Creat url and retrieve data
        url = f"http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{str(countYear)}{monthDict[countMonth]}.csv"
        print(url)
        anbimaData = pd.read_csv(url, delimiter=";", parse_dates=["DT_COMPTC"])
        
        # Data handling
        rawData = anbimaData.merge(fundsDB[["CNPJ", "Id"]].rename(columns={"Id": "Id_Fund", "CNPJ": "CNPJ_FUNDO"}), how="left", on="CNPJ_FUNDO")
        rawData = rawData.loc[~rawData["Id_Fund"].isnull()].rename(columns={"DT_COMPTC": "Refdate", "VL_QUOTA": "Quote_Value", "CAPTC_DIA": "Captc_Dia", "RESG_DIA": "Resg_Dia"})
        rawData = rawData[["Refdate", "Id_Fund", "Quote_Value", "Captc_Dia", "Resg_Dia"]]
        
        funds_dataDF = pd.concat([funds_dataDF, rawData])
        
# Data for inserting
funds_dataDF = funds_dataDF.loc[(funds_dataDF["Refdate"]>=dtIni.strftime("%Y-%m-%d"))&(funds_dataDF["Refdate"]<=td.strftime("%Y-%m-%d"))]

# Delete previous data
RefdateLst = funds_dataDF['Refdate'].drop_duplicates().to_list()
deleteQuery = f"""
    DELETE FROM 
        funds_data 
    WHERE
        Refdate IN ({str(RefdateLst).replace("[", "").replace("]", "")})
    """
engine.execute(deleteQuery)

# Insert data into DataBase
funds_dataDF.to_sql(name="funds_data", con=engine, if_exists="append", index=False)
