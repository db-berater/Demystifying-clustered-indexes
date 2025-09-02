/*============================================================================
	File:		0110 - DELETE in a HEAP.sql

	Summary:	This script demonstrates the drawbacks of DELETE-Operations
				in HEAPS

				THIS SCRIPT IS PART OF THE TRACK: "Clustered Indexes - Pro and Con"

	Date:		Juni 2016

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
USE demo_db;
SET LANGUAGE us_english;
SET NOCOUNT ON;
SET ROWCOUNT 0;
GO

IF OBJECT_ID(N'dbo.demo_table', N'U') IS NOT NULL
	DROP TABLE dbo.demo_table;
	GO

-- Step 1: Create a table wich holds 1 record on one page!
RAISERROR ('demo table will be created...', 0, 1) WITH NOWAIT;
CREATE TABLE dbo.demo_table
(
	Id	INT			NOT NULL	IDENTITY (1, 1),
	C1	CHAR(8000)	NOT NULL
);
GO

-- Step 2: We insert a huge amount of data into the table for the demos!
RAISERROR ('20,000 records ~ 20,000 pages ~ will be inserted into demo_table', 0, 1) WITH NOWAIT;
INSERT INTO dbo.demo_table WITH (TABLOCK) (C1)
SELECT	TOP 20000
		text
FROM	sys.messages;
GO

CHECKPOINT;
GO

-- what pages have been allocated by the table
SELECT	DDIPS.index_id,
		DDIPS.index_type_desc,
		DDIPS.page_count,
		DDIPS.record_count
FROM	sys.dm_db_index_physical_stats
(
	DB_ID(),
	OBJECT_ID(N'dbo.demo_table', N'U'),
	0,
	NULL,
	'DETAILED'
) AS DDIPS
GO

-- what resource of the table dbo.demo_table are in the buffer pool now!
;WITH db_pages
AS
(
	SELECT	DDDPA.page_type,
			DDDPA.allocated_page_file_id,
			DDDPA.allocated_page_page_id,
			DDDPA.page_level,
			DDDPA.page_free_space_percent,
			DDDPA.is_allocated
	FROM	sys.dm_db_database_page_allocations
			(
				DB_ID(),
				OBJECT_ID(N'dbo.demo_table', N'U'),
				0,
				NULL,
				'DETAILED'
			) AS DDDPA
)
SELECT	DOBD.file_id,
		DOBD.page_id,
		DOBD.page_level,
		DOBD.page_type,
		DOBD.row_count,
		DOBD.free_space_in_bytes,
		DP.page_free_space_percent,
		DP.is_allocated
FROM	db_pages AS DP LEFT JOIN sys.dm_os_buffer_descriptors AS DOBD
		ON
		(
			DOBD.database_id = DB_ID()
			AND DOBD.file_id = DP.allocated_page_file_id
			AND DOBD.page_id = DP.allocated_page_page_id
			AND DOBD.page_level = DP.page_level
		)
ORDER BY
		DP.page_type DESC,
		DP.page_level DESC,
		DOBD.page_id,
		DOBD.file_id;
GO

-- delete 2,000 records
SET ROWCOUNT 2000;
GO

DELETE	dbo.demo_table
WHERE	Id % 2 = 0;
GO

-- what pages have been allocated by the table
SELECT	DDIPS.index_id,
		DDIPS.index_type_desc,
		DDIPS.page_count,
		DDIPS.record_count
FROM	sys.dm_db_index_physical_stats
(
	DB_ID(),
	OBJECT_ID(N'dbo.demo_table', N'U'),
	0,
	NULL,
	'DETAILED'
) AS DDIPS
GO

-- Now we delete half of the records
SET ROWCOUNT 0;
GO

BEGIN TRANSACTION demo;
GO

	DELETE	dbo.demo_table
	WHERE	Id % 2 = 0
	GO

	-- What objects have been locked?
	SELECT	resource_type,
			request_mode,
			request_status,
			COUNT_BIG(*)		AS	Locks
	FROM	sys.dm_tran_locks AS DTL
	WHERE	DTL.request_session_id = @@SPID
	GROUP BY
			resource_type,
			request_mode,
			request_status;
	GO

COMMIT TRANSACTION demo;
GO

-- what transactions have occured
SELECT	Operation,
		Context,
		[Page ID],
		[Lock Information]
FROM	sys.fn_dblog(NULL, NULL)
WHERE	[Transaction ID] IN
		(
			SELECT	[Transaction ID]
			FROM	sys.fn_dblog(NULL, NULL)
			WHERE	[Transaction Name] = N'demo'
		);
GO

-- what pages have been allocated by the table
SELECT	DDIPS.index_id,
		DDIPS.index_type_desc,
		DDIPS.page_count,
		DDIPS.record_count
FROM	sys.dm_db_index_physical_stats
(
	DB_ID(),
	OBJECT_ID(N'dbo.demo_table', N'U'),
	0,
	NULL,
	'DETAILED'
) AS DDIPS
GO

CHECKPOINT;
GO

-- what resource of the table dbo.demo_table are in the buffer pool now!
;WITH db_pages
AS
(
	SELECT	DDDPA.page_type,
			DDDPA.allocated_page_file_id,
			DDDPA.allocated_page_page_id,
			DDDPA.page_level,
			DDDPA.page_free_space_percent,
			DDDPA.is_allocated
	FROM	sys.dm_db_database_page_allocations
			(
				DB_ID(),
				OBJECT_ID(N'dbo.demo_table', N'U'),
				0,
				NULL,
				'DETAILED'
			) AS DDDPA
)
SELECT	DOBD.file_id,
		DOBD.page_id,
		DOBD.page_level,
		DOBD.page_type,
		DOBD.row_count,
		DOBD.free_space_in_bytes,
		DP.page_free_space_percent,
		DP.is_allocated
FROM	db_pages AS DP LEFT JOIN sys.dm_os_buffer_descriptors AS DOBD
		ON
		(
			DOBD.database_id = DB_ID()
			AND DOBD.file_id = DP.allocated_page_file_id
			AND DOBD.page_id = DP.allocated_page_page_id
			AND DOBD.page_level = DP.page_level
		)
ORDER BY
		DP.page_type DESC,
		DP.page_level DESC,
		DOBD.page_id,
		DOBD.file_id;
GO

-- now we rebuild the table
ALTER TABLE dbo.demo_table REBUILD;
GO