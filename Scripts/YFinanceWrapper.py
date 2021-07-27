import pandas as pd
import yfinance as yf

class YFinanceWrapper:
    def __init__(self, dbEngine):
        """YFinance Wrapper Constructor

        Args:
            dbEngine (sql connector engine): Database engine
        """        
        self.dbEngine = dbEngine
        self.conn = self.dbEngine.connect()

        # Variables from database
        self.GICS_Sector = pd.read_sql("SELECT * FROM GICS_Sector", con=self.dbEngine)
        self.GICS_Industry = pd.read_sql("SELECT * FROM GICS_Industry", con=self.dbEngine)
        self.Companies = pd.read_sql("SELECT * FROM Companies", con=self.dbEngine)
        self.Products = pd.read_sql("SELECT * FROM Products", con=self.dbEngine)

    def addProducts(self, tickers):
        """Function to insert products into database

        Args:
            tickers (list or str): Product(s) to be inserted on database
        """
        insertDB_DF = pd.DataFrame(columns=['Name', 'Id_Company', 'yFinanceCode'])
        # Adjust products to list type
        tickerList = tickers if type(tickers)==list else [tickers]

        # Loop through products to fill insertDB_DF 
        for ticker in tickerList:
            # Product data from yFinance
            prodYFinData = yf.Ticker(ticker)
            
            # Check if ticker already on database
            dbProduct = self.Products.loc[self.Products['yFinanceCode']==ticker]
            if not dbProduct.empty:
                print(f"Product with ticker {ticker} already in database!!")
                continue
            
            # Err handler on get data
            try:
                # Try to get data from yFinance
                companyName = prodYFinData.info['longName']
                GICS_Sector = prodYFinData.info['sector']
                GICS_Industry = prodYFinData.info['industry']
                
            except:
                # In case of error iterate to next ticker
                print(f"Error while collecting data of: {ticker}")
                continue
            
            # Check if companyName, GICS_Sector, GICS_Industry
            # Already registered on database
            dbCompany = self.Companies.loc[self.Companies['Name']==companyName]
            dbGICS_Sector = self.GICS_Sector.loc[self.GICS_Sector['Name']==GICS_Sector]
            dbGICS_Industry = self.GICS_Industry.loc[self.GICS_Industry['Name']==GICS_Industry]

            # Check if it's a new company data
            if dbCompany.empty:
                # Check if it's new GICS_Sector
                if dbGICS_Sector.empty:
                    self.insertGICS_SectortoDB(name=GICS_Sector)
                    dbGICS_Sector = self.GICS_Sector.loc[self.GICS_Sector['Name']==GICS_Sector]

                # Check if it's new GICS_Industry
                if dbGICS_Industry.empty:
                    self.insertGICS_IndustrytoDB(name=GICS_Industry)
                    dbGICS_Industry = self.GICS_Industry.loc[self.GICS_Industry['Name']==GICS_Industry]

                # Insert new company info
                self.insertCompanytoDB(name=companyName, GICS_Sector=GICS_Sector, GICS_Industry=GICS_Industry)
                dbCompany = self.Companies.loc[self.Companies['Name']==companyName]

            currentDF = pd.DataFrame(
                {
                    "Name": [ticker]
                    ,"Id_Company": [dbCompany['Id'].iloc[0]]
                    ,"yFinanceCode": [ticker]
                }
            )

            insertDB_DF = pd.concat([insertDB_DF, currentDF])

        # Insert products into database
        if not insertDB_DF.empty: 
            insertDB_DF.to_sql(name="products", con=self.dbEngine, if_exists="append", index=False)
            # Atualiza products table
            self.Products = pd.read_sql("SELECT * FROM Products", con=self.dbEngine)
        
    def updatePriceHistory(self, byPeriodType, tickers="All", period=False, start=False, end=False, returnDataDF=False):
        """Function to update prices to database

        Args:
            byPeriodType (str): Should be "period" or "date"
            tickers (str ||list of str, optional): List of tickers to be updated. If "All" update all tickers from self.Products
            period (bool, optional): Period description. Defaults to False.
            start (bool, optional): Start date. Defaults to False.
            end (bool, optional): End date. Defaults to False.
            returnDataDF (bool, optional): Flag to return dataframe. Defaults to False
        """
        # Adjust list of tickers to retrieve data from yahoo finance
        if tickers=="All":
            tickerList = self.Products["yFinanceCode"].to_list()
            tickersStr = ' '.join(tickerList)
        else:
            tickersStr = ' '.join(tickers)
        
        # Check "byPeriodType" entry
        if byPeriodType=="period":
            if not period: return print("Argumento para periodo invalido!!")
            
            # Make request for yFinance API
            data = yf.download(
                tickers = tickersStr
                ,period=period
                ,group_by="column"
            )
            
        elif byPeriodType=="date":
            if not start or not end: return print("Argumento para data invalido!!")
            # Make request for yFinance API
            data = yf.download(
                tickers = tickersStr
                ,start=start
                ,end=end
                ,group_by="column"
            )
            
        else: return print("Argumento byPeriodType deve ser 'period' or 'date'!!")
        
        pxData = data['Close']
        
        # DataFrame to prepared to be inserted on database
        insertDF = pd.DataFrame()
        
        # Loop through columns
        ###### FIND BETTER SOLUTION IN THE FUTURE ######
        for col in pxData.columns:
            # Retrieve data for each product
            dataCol = pxData[[col]].dropna()
            dataCol.loc[:, "yFinanceCode"] = col
            dataCol.rename(columns={col: "Px_Close"}, inplace=True)
            
            insertDF = pd.concat([insertDF, dataCol])
        
        # Adjust DF to be inserted to database
        insertDF = insertDF.reset_index().rename(columns={"Date": "Refdate"})
        insertDF = insertDF.merge(self.Products[["yFinanceCode", "Id"]].rename(columns={"Id": "Id_Product"}), how="left", on="yFinanceCode")
        insertDF['Px_Close'] = insertDF['Px_Close'].round(decimals=2)
        
        ##### Delete previous data before inserting #####
        idProductsList = list(dict.fromkeys(insertDF["Id_Product"].astype(int).to_list()))
        idRefdateList = list(dict.fromkeys(insertDF["Refdate"].astype(str).to_list()))
        
        deleteQuery = f"""
            DELETE FROM 
                Prices 
            WHERE
                Refdate IN ({str(idRefdateList).replace("[", "").replace("]", "")})
                AND Id_Product IN ({str(idProductsList).replace("[", "").replace("]", "")})
            """
        self.conn.execute(deleteQuery)
        
        ##### Insert Data #####
        insertDF[["Refdate", "Id_Product", "Px_Close"]].to_sql(name="prices", con=self.dbEngine, if_exists="append", index=False)
        
        #Return insertDF dataframe
        if returnDataDF: return insertDF
        
    ######### Auxiliar Functions #########
    def insertCompanytoDB(self, name, GICS_Sector, GICS_Industry):
        """Function to insert new company to database

        Args:
            name (string): Company name
            GICS_Sector (string): Company sector
            GICS_Industry (string): Company industry
        """

        idGICS_Sector = self.GICS_Sector.loc[self.GICS_Sector['Name']==GICS_Sector]['Id'].iloc[0]
        idGICS_Industry = self.GICS_Industry.loc[self.GICS_Industry['Name']==GICS_Industry]['Id'].iloc[0]

        insertDF = pd.DataFrame(
            {
                "Name": [name]
                ,"Id_GICS_Sector": [idGICS_Sector]
                ,"Id_GICS_Industry": [idGICS_Industry]
            }
        )
        
        try:
            # Insert to database
            insertDF.to_sql(name="companies", con=self.dbEngine, if_exists="append", index=False)
            print(f"Added Company: {name}")
            print(insertDF)
            # Update YFinanceWrapper self.Companies
            self.Companies = pd.read_sql("SELECT * FROM Companies", con=self.dbEngine)
        except:
            print(f"Error while appending company to DB: {name}!!")

    def insertGICS_SectortoDB(self, name):
        """Function to insert new GICS_Sector to DB

        Args:
            name (str): GICS Sector name
        """
        insertDF = pd.DataFrame(
            {
            "Name": [name]
            }
        )

        try:
            # Insert to database
            insertDF.to_sql(name="gics_sector", con=self.dbEngine, if_exists="append", index=False)
            print(f"Added GICS_Sector {name}")
            print(insertDF)
            self.GICS_Sector = pd.read_sql("SELECT * FROM GICS_Sector", con=self.dbEngine)
        except:
            print(f"Error while appending GICS_Sector to DB: {name}!!")

    def insertGICS_IndustrytoDB(self, name):
        """Function to insert new GICS_Industry to DB

        Args:
            name (str): GICS Industry name
        """
        insertDF = pd.DataFrame(
            {
            "Name": [name]
            }
        )

        try:
            # Insert to database
            insertDF.to_sql(name="gics_industry", con=self.dbEngine, if_exists="append", index=False)
            print(f"Added GICS_Industry {name}")
            self.GICS_Industry = pd.read_sql("SELECT * FROM GICS_Industry", con=self.dbEngine)
        except:
            print(f"Error while appending GICS_Industry to DB: {name}!!")
