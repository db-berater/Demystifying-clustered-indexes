/*============================================================================
	File:		0120 - GUID vs INT - IDENTITY.sql

	Summary:	This script creates a demo database which will be used for
				the future demonstration scripts


	Date:		März 2014

	SQL Server Version: 2008 / 2012 / 2014 / 2016
------------------------------------------------------------------------------
	Written by Uwe Ricken, db Berater GmbH

	This script is intended only as a supplement to demos and lectures
	given by Uwe Ricken.  
  
	THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF 
	ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED 
	TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
	PARTICULAR PURPOSE.
============================================================================*/
USE master;
GO

SET NOCOUNT ON;
SET LANGUAGE us_english;
USE master;

EXEC sp_create_demo_db;
GO

-- Modify the database to avoid file growth when the tasks are running
ALTER DATABASE demo_db
MODIFY FILE
(
	NAME = N'demo_db',
	SIZE = 2048MB
);
GO

ALTER DATABASE demo_db
MODIFY FILE
(
	NAME = N'demo_db_log',
	SIZE = 512MB
);
GO

USE demo_db;
GO

-- table with contigious numbers as clustered key
IF OBJECT_ID(N'dbo.numeric_table', N'U') IS NOT NULL
	DROP TABLE dbo.numeric_table;
	GO

CREATE TABLE dbo.numeric_table
(
	Id	INT			NOT NULL	IDENTITY(1, 1),
	c1	CHAR(100)	NOT NULL	DEFAULT ('just a filler'),
	
	CONSTRAINT pk_numeric_table PRIMARY KEY CLUSTERED (Id)
);
GO

-- table with random guid as clustered key
IF OBJECT_ID(N'dbo.guid_table', N'U') IS NOT NULL
	DROP TABLE dbo.guid_table;
	GO

CREATE TABLE dbo.guid_table
(
	Id	UNIQUEIDENTIFIER	NOT NULL	ROWGUIDCOL	DEFAULT(NEWID()),
	c1	CHAR(88)			NOT NULL	DEFAULT ('just a filler'),
	
	CONSTRAINT pk_guid_table PRIMARY KEY CLUSTERED (Id)
);
GO

-- heap with NO ordering element
IF OBJECT_ID(N'dbo.heap_table', N'U') IS NOT NULL
	DROP TABLE dbo.heap_table;
	GO

CREATE TABLE dbo.heap_table
(
	Id	INT			NOT NULL	IDENTITY (1, 1),
	c1	CHAR(100)	NOT NULL	DEFAULT ('just a filler')
);
GO

-- Procedure for insertion of 1,000 records in numeric_table
IF OBJECT_ID(N'dbo.proc_insert_data', N'U') IS NOT NULL
	DROP PROC dbo.proc_insert_data;
	GO

CREATE PROC dbo.proc_insert_data
	@type		VARCHAR(10),
	@num_recs	INT	=	1000
AS
	SET NOCOUNT ON

	DECLARE	@i INT = 1;

	IF @type = 'numeric'
	BEGIN
		WHILE @i <= @num_recs
		BEGIN
			INSERT INTO dbo.numeric_table DEFAULT VALUES
			SET @i += 1;
		END

		RETURN;
	END
	
	IF @type = 'guid'
	BEGIN
		WHILE @i <= @num_recs
		BEGIN
			INSERT INTO dbo.guid_table DEFAULT VALUES
			SET @i += 1;
		END

		RETURN;
	END
			
	IF @type = 'heap'
	BEGIN
		WHILE @i <= @num_recs
		BEGIN
			INSERT INTO dbo.heap_table DEFAULT VALUES
			SET @i += 1;
		END
	END

	SET NOCOUNT OFF;
GO

CHECKPOINT;
GO

/*
	ostress -E -SNB-LENOVO-I\SQL_2016 -Q"EXEC dbo.proc_insert_data @type = 'numeric', @num_recs = 1000;" -n20 -ddemo_db -q
	ostress -E -SNB-LENOVO-I\SQL_2016 -Q"EXEC dbo.proc_insert_data @type = 'guid', @num_recs = 1000;" -n20 -ddemo_db -q
	ostress -E -SNB-LENOVO-I\SQL_2016 -Q"EXEC dbo.proc_insert_data @type = 'heap', @num_recs = 1000;" -n20 -ddemo_db -q
*/

DBCC SQLPERF('sys.dm_os_wait_stats', 'CLEAR');
GO

WITH [Waits] AS
(
	SELECT	[wait_type],
			[wait_time_ms] / 1000.0 AS [WaitS],
			([wait_time_ms] - [signal_wait_time_ms]) / 1000.0 AS [ResourceS],
			[signal_wait_time_ms] / 1000.0 AS [SignalS],
			[waiting_tasks_count] AS [WaitCount],
			100.0 * [wait_time_ms] / SUM ([wait_time_ms]) OVER() AS [Percentage],
			ROW_NUMBER() OVER(ORDER BY [wait_time_ms] DESC) AS [RowNum]
    FROM	sys.dm_os_wait_stats
    WHERE	[wait_type] IN
	(
		N'WRITELOG', N'PAGELATCH_SH', N'PAGELATCH_EX',
		N'SOS_SCHEDULER_YIELD', N'PAGEIOLATCH_SH', N'PAGEIOLATCH_EX'
	)
	AND	waiting_tasks_count > 0
)
SELECT	MAX ([W1].[wait_type]) AS [WaitType],
		CAST (MAX ([W1].[WaitS]) AS DECIMAL (16,2)) AS [Wait_S],
		CAST (MAX ([W1].[ResourceS]) AS DECIMAL (16,2)) AS [Resource_S],
		CAST (MAX ([W1].[SignalS]) AS DECIMAL (16,2)) AS [Signal_S],
		MAX ([W1].[WaitCount]) AS [WaitCount],
		CAST (MAX ([W1].[Percentage]) AS DECIMAL (5,2)) AS [Percentage],
		CAST ((MAX ([W1].[WaitS]) / MAX ([W1].[WaitCount])) AS DECIMAL (16,4)) AS [AvgWait_S],
		CAST ((MAX ([W1].[ResourceS]) / MAX ([W1].[WaitCount])) AS DECIMAL (16,4)) AS [AvgRes_S],
		CAST ((MAX ([W1].[SignalS]) / MAX ([W1].[WaitCount])) AS DECIMAL (16,4)) AS [AvgSig_S],
		'http://documentation.red-gate.com/display/SM4/' + W1.wait_type AS [DocumentLink]
FROM	[Waits] AS [W1] INNER JOIN [Waits] AS [W2]
		ON ([W2].[RowNum] <= [W1].[RowNum])
GROUP BY
		[W1].[RowNum],
		W1.wait_type
HAVING	SUM ([W2].[Percentage]) - MAX ([W1].[Percentage]) < 99.9;
GO

SELECT	OBJECT_NAME(object_id),
		index_type_desc,
		fragment_count,
		page_count,
		record_count,
		avg_fragmentation_in_percent,
		avg_page_space_used_in_percent
FROM	sys.dm_db_index_physical_stats
(
	DB_ID(),
	OBJECT_ID('dbo.numeric_table', 'U'),
	NULL,
	NULL,
	'DETAILED'
) AS DDIPS;
GO

SELECT	OBJECT_NAME(object_id),
		index_type_desc,
		fragment_count,
		page_count,
		record_count,
		avg_fragmentation_in_percent,
		avg_page_space_used_in_percent
FROM sys.dm_db_index_physical_stats
(
	DB_ID(),
	OBJECT_ID('dbo.guid_table', 'U'),
	1,
	NULL,
	'DETAILED'
) AS DDIPS;
GO

SELECT	OBJECT_NAME(object_id),
		index_type_desc,
		fragment_count,
		page_count,
		record_count,
		avg_fragmentation_in_percent,
		avg_page_space_used_in_percent
FROM	sys.dm_db_index_physical_stats
(
	DB_ID(),
	OBJECT_ID('dbo.heap_table', 'U'),
	0,
	NULL,
	'DETAILED'
) AS DDIPS;
GO