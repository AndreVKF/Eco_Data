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

from Controller import Controller

mySQLConnector = mySQLConnector(schema='eco_data')
engine = mySQLConnector.createEngine()

yFinWrapper = YFinanceWrapper(dbEngine=engine)
AnbimaWrapper = AnbimaWrapper(dbEngine=engine)

Controller = Controller(dbEngine=engine)

Controller.updateLastData()


