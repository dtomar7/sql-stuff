USE [DW]
GO
/****** Object:  StoredProcedure [dbo].[ETL1A_CODING_CHALLENGE]    Script Date: 01/10/2019 16:39:17 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


-- =============================================
-- Author:		Deepak Tomar
-- Create date: 01/10/2019
-- Description:	Coding Challenge
-- =============================================
CREATE PROCEDURE [dbo].[ETL1A_CODING_CHALLENGE]
(
@datasetOveride VARCHAR(MAX) NULL = NULL --Provide the file path on the server.

)
AS
BEGIN
	--Declaring variables to be used in stored procedure
	DECLARE @sql VARCHAR(MAX),@output VARCHAR(MAX) ,@dataset VARCHAR(MAX)

	--Default Dataset file location
	SET @dataset = '\\nhqtwra1\c$\testfolder\Learning\ChallengeSampleDataSet1.txt' 

	--Overriding Dataset Value by given input dataset path
	IF @datasetOveride IS NOT NULL
			SET @dataset = @datasetOveride

	--Dynamic Sql query for loading data from text file to temp table
	SET @sql = 'BULK INSERT #temp FROM ''' + @dataset + ''' WITH ( ROWTERMINATOR = ' + ''',''' + ' )'

	--Creating temporary table to do bulk upload data from text file (Source Level Stage)
	DROP TABLE IF EXISTS #src;

	CREATE TABLE #src
	(
		[Share Price] DECIMAL(4, 2)
	);

	--Inserting Share Price values in #src table
	EXEC(@sql) 

	--Creating temporary table add date of the month to share price data (Pre Staging Level Data)
	DROP TABLE IF EXISTS #inputs
	CREATE TABLE #inputs
	(
		[DayOfMonth] [INT] IDENTITY(1, 1) NOT NULL,
		[Share Price] DECIMAL(4, 2) NOT NULL,
	
	);

	
	INSERT INTO #inputs
	(
		[Share Price]
	
	)
	SELECT [Share Price]
	FROM #src

	--Creating temp table to get final required data 
	DROP TABLE IF EXISTS #output

	SELECT TOP 1 CONCAT(CONCAT(buyPrice.[DayOfMonth], '(', CONCAT(buyPrice.[Share Price] , '),')) ,CONCAT(sellPrice.[DayOfMonth], '(', CONCAT(sellPrice.[Share Price] , ')'))) AS [Output]
	INTO #output
	FROM #inputs buyPrice
		LEFT JOIN #inputs sellPrice
			ON sellPrice.[DayOfMonth] > buyPrice.[DayOfMonth]
	ORDER BY (sellPrice.[Share Price] - buyPrice.[Share Price]) DESC

	SELECT * FROM #output

END 



