--Getting data from 1st datasource
/* --If we were to have a string of input data instead of text or csv file
DECLARE @dataset VARCHAR(MAX)
SET @dataset = '18.93,20.25,17.05,16.59,21.09,16.22,21.43,27.13,18.62,21.31,23.96,25.52,19.64,23.49,15.28,22.77,23.1,26.58,27.03,23.75,27.39,15.93,17.83,18.82,21.56,25.33,25,19.33,22.08,24.03'
SELECT value AS [Share Price]
INTO #temp1
FROM STRING_SPLIT(@dataset, ',')
*/

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


--Preparing output data
DROP TABLE IF EXISTS #output
SELECT CONCAT(CONCAT(buyPrice.[DayOfMonth], '(', CONCAT(buyPrice.[Share Price] , '),')) ,CONCAT(sellPrice.[DayOfMonth], '(', CONCAT(sellPrice.[Share Price] , ')'))) AS [Output] 
			,buyPrice.[Month]
			,(sellPrice.[Share Price] - buyPrice.[Share Price]) AS [Profit]
INTO #output
FROM #tradePrices buyPrice
    LEFT JOIN #tradePrices sellPrice
        ON sellPrice.[DayOfMonth] > buyPrice.[DayOfMonth]
		AND buyPrice.[Month] = sellPrice.[Month]



SELECT a.[Output],a.[Month]
FROM (
SELECT *,
	   ROW_NUMBER() OVER (PARTITION BY Month ORDER BY Profit DESC ) AS RN
FROM #output
WHERE Profit IS NOT NULL
)a
WHERE a.RN = 1




