--Getting data from 1st datasource
DROP TABLE IF EXISTS #temp1;
CREATE TABLE #temp1
(
    [Share Price] DECIMAL(4, 2)
);

BULK INSERT #temp1
FROM '\\server\c$\download\Computershare - Coding Challenge\ChallengeSampleDataSet1.txt'
WITH
(
    ROWTERMINATOR = ','
);
GO

DROP TABLE IF EXISTS #temp2;
CREATE TABLE #temp2
(
    [Share Price] DECIMAL(4, 2)
);

--Getting data from 2nd datasource
BULK INSERT #temp2
FROM '\\server\c$\download\Computershare - Coding Challenge\ChallengeSampleDataSet2.txt'
WITH
(
    ROWTERMINATOR = ','
);
GO

--Load data from 2 sources into 1 temporaray table
DROP TABLE IF EXISTS #inputs
CREATE TABLE #inputs
(
    [Id] [INT] IDENTITY(1, 1) NOT NULL,
    [Share Price] DECIMAL(4, 2) NOT NULL,
	[Month] INT NULL
);


INSERT INTO #inputs
(
    [Share Price],
	[Month]
)
SELECT [Share Price]
		,1 AS [Month] --To indate First Dataset (1st monthly data)
FROM #temp1

UNION ALL

SELECT [Share Price]
		,2 AS [Month] --To indate Second Dataset (2nd monthly data)
FROM #temp2

--Adding DayOfMonth data to share prices
DROP TABLE IF EXISTS #tradePrices

	SELECT Id,
           [Share Price],
           [Month],
		   ROW_NUMBER() OVER (PARTITION BY [Month] ORDER BY Id ) AS [DayOfMonth]
	INTO #tradePrices
	FROM #inputs

--Get the data for 1st data set
SELECT TOP 1 CONCAT(CONCAT(buyPrice.[DayOfMonth], '(', CONCAT(buyPrice.[Share Price] , '),')) ,CONCAT(sellPrice.[DayOfMonth], '(', CONCAT(sellPrice.[Share Price] , ')'))) AS [Output] 
			,buyPrice.[Month]
FROM #tradePrices buyPrice
    LEFT JOIN #tradePrices sellPrice
        ON sellPrice.[DayOfMonth] > buyPrice.[DayOfMonth]
WHERE buyPrice.[Month] = 1 AND sellPrice.[Month] = 1
ORDER BY (sellPrice.[Share Price] - buyPrice.[Share Price]) DESC;

--Get the data for 2nd data set
SELECT TOP 1 CONCAT(CONCAT(buyPrice.[DayOfMonth], '(', CONCAT(buyPrice.[Share Price] , '),')) ,CONCAT(sellPrice.[DayOfMonth], '(', CONCAT(sellPrice.[Share Price] , ')'))) AS [Output] 
			,buyPrice.[Month]
FROM #tradePrices buyPrice
    LEFT JOIN #tradePrices sellPrice
        ON sellPrice.[DayOfMonth] > buyPrice.[DayOfMonth]
WHERE buyPrice.[Month] = 2 AND sellPrice.[Month] = 2
ORDER BY (sellPrice.[Share Price] - buyPrice.[Share Price]) DESC;




