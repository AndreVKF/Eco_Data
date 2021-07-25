USE eco_data;

CREATE TABLE GICS_Sector (
	Id INT NOT NULL AUTO_INCREMENT
    ,Name NVARCHAR(55) NOT NULL
    ,PRIMARY KEY(Id)
);

CREATE TABLE GICS_Industry (
	Id INT NOT NULL AUTO_INCREMENT
    ,Name NVARCHAR(55) NOT NULL
    ,PRIMARY KEY(Id)
);

CREATE TABLE Companies (
	Id INT NOT NULL AUTO_INCREMENT
    ,Name NVARCHAR(255) NOT NULL
    ,Id_GICS_Sector INT NOT NULL
    ,Id_GICS_Industry INT NULL
    ,PRIMARY KEY(Id)
    ,FOREIGN KEY (Id_GICS_Sector) REFERENCES GICS_Sector(Id)
    ,FOREIGN KEY (Id_GICS_Industry) REFERENCES GICS_Industry(Id)
);

CREATE TABLE IbovEquities (
	Id INT NOT NULL AUTO_INCREMENT
    ,Name NVARCHAR(255) NOT NULL
    ,Id_Company NVARCHAR(255)
    ,PRIMARY KEY(Id)
    ,FOREIGN KEY (Id_Company) REFERENCES Companies(Id)
);

CREATE TABLE EquitiesPrices (
	Refdate DATE NOT NULL
    ,Id_Equity INT NOT NULL
    ,Price REAL NOT NULL
	, FOREIGN KEY (Id_Equity) REFERENCES IbovEquities(Id)
    ,CONSTRAINT UC_EquitiesPrices UNIQUE (Refdate, Id_Equity)
);

SELECT * FROM IbovEquities
SELECT * FROM Companies