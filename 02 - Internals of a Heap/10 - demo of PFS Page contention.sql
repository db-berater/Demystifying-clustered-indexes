/*
	============================================================================
	File:		10 - demo of PFS Page contention.sql

	Summary:	This demo will show the problematic behavior of HEAP tables
				and massive insert operations.

				THIS SCRIPT IS PART OF THE TRACK:
					Session - Demystifying Clustered Indexes

	Date:		June 2025

	SQL Server Version: >= 2016
	------------------------------------------------------------------------------
	Written by Uwe Ricken, db Berater GmbH

	This script is intended only as a supplement to demos and lectures
	given by Uwe Ricken.  
  
	THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF 
	ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED 
	TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
	PARTICULAR PURPOSE.
	============================================================================
*/
SET XACT_ABORT ON;
SET NOCOUNT ON;
GO

USE master;
GO

EXEC ERP_Demo.dbo.sp_create_demo_db
	@num_of_files = 1,	/* replay with 4 and 8 files */
	@initial_size_mb = 1024;
GO

USE demo_db;
GO

/* Let's create a HEAP which can store max 4 rows */
DROP TABLE IF EXISTS dbo.demo_table;
GO

CREATE TABLE dbo.demo_table
(
	Id	INT			NOT NULL	IDENTITY (1, 1),
	c1	CHAR(2000)	NOT NULL
);
GO

/* A stored procedure is the wrapper for the insert process */
CREATE OR ALTER PROCEDURE dbo.insert_heap_data
AS
BEGIN
	SET NOCOUNT ON;

	INSERT INTO dbo.demo_table (c1) VALUES (NEWID());
END
GO

/*
	Now we install the extended event which records any latch contention
	on a PFS page in the ERP_Demo database

	97 - Extended Events\03 - monitor Latch Contention.sql
*/

DROP TABLE IF EXISTS #result;
GO

CREATE TABLE #result
(
	[timestamp]		DATETIME2(0)	NOT NULL,
	[mode]			VARCHAR(10)		NOT NULL,
	[duration]		BIGINT			NOT NULL,
	[has_waiters]	VARCHAR(10)		NOT NULL,
	[page_id]		BIGINT			NOT NULL,
	[page_type_id]	SMALLINT		NOT NULL,
	[map_value]		NVARCHAR(64)	NOT NULL
);
GO

INSERT INTO #result WITH (TABLOCK)
([timestamp], [mode], [duration], [has_waiters], [page_id], [page_type_id], [map_value])
EXEC ERP_Demo.dbo.sp_read_xevent_contention
	@xevent_name = N'monitor_latch_contention';
GO

SELECT * FROM #result;
GO

/*
	Analysis of the execution phase
*/
SELECT	r.map_value,
		COUNT_BIG(*)				AS	row_count,
		FORMAT(MAX(r.duration) / 1000.0, N'#,##0.000 ms', N'en-us')	AS	max_duration_ms,
		FORMAT(MIN(r.duration) / 1000.0, N'#,##0.000 ms', N'en-us')	AS	min_duration_ms,
		FORMAT(AVG(r.duration) / 1000.0, N'#,##0.000 ms', N'en-us')	AS	avg_duration_ms
FROM	#result AS r
GROUP BY
		r.map_value;

/*
	Clean the kitchen....
*/
DROP TABLE IF EXISTS #result;
GO

USE master;
GO

ALTER DATABASE [demo_db] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
GO
DROP DATABASE [demo_db];
GO

IF EXISTS (SELECT * FROM sys.server_event_sessions WHERE name = N'monitor_latch_contention')
BEGIN
	RAISERROR (N'dropping existing extended event session [monitor_latch_contention]...', 0, 1) WITH NOWAIT;
	DROP EVENT SESSION [monitor_latch_contention] ON SERVER;
END
GO