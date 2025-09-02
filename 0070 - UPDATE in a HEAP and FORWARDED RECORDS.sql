/*============================================================================
	File:		0090 - UPDATE in a HEAP and FORWARDED RECORDS.sql

	Summary:	This script uses data in a HEAP and after an expansion of the
				data length it produces a FORWARDED record

				THIS SCRIPT IS PART OF THE TRACK: "Clustered Indexes - Pro and Con"

	Date:		July 2015

	SQL Server Version: 2008 / 2012 / 2014
------------------------------------------------------------------------------
	Written by Uwe Ricken, db Berater GmbH

	This script is intended only as a supplement to demos and lectures
	given by Uwe Ricken.  
  
	THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF 
	ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED 
	TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
	PARTICULAR PURPOSE.
============================================================================*/
SET NOCOUNT ON;
SET LANGUAGE us_english;
USE master;
GO

EXEC sp_create_demo_db;
GO

USE demo_db;
GO

IF OBJECT_ID('dbo.demo_table', 'U') IS NOT NULL
	DROP TABLE dbo.demo_table;
	GO

CREATE TABLE dbo.demo_table
(
	c1	int		IDENTITY(1, 1),
	c2	varchar(8000)
);
GO

INSERT INTO dbo.demo_table (c2) VALUES (REPLICATE('A', 3500));
INSERT INTO dbo.demo_table (c2) VALUES (REPLICATE('B', 3500));
GO

-- get the physical and logical location of the data
SELECT sys.fn_PhysLocFormatter(%%physloc%%) AS Location, * FROM dbo.demo_table
GO

CHECKPOINT;
GO

-- Check the space allocation of the allocated page in the PFS (because you have a HEAP!)
DBCC TRACEON (3604);
DBCC PAGE (demo_db, 1, 1, 3);
DBCC PAGE (demo_db, 1,147, 1);
GO

-- Update the record 1 and write a text of 5000 chars into col2
BEGIN TRANSACTION UpdateRecord
GO
	UPDATE	dbo.demo_table SET c2 = REPLICATE ('A', 5000)
	WHERE c1 = 1;
	GO

	-- see the amount of operations which took place!
	SELECT	database_transaction_log_bytes_used,
			database_transaction_log_record_count
	FROM	sys.dm_tran_database_transactions
	WHERE	database_id = db_id();
	GO

COMMIT TRANSACTION UpdateRecord
GO

-- What happend inside the named transaction?
SELECT	[Current LSN],
		Operation,
		Context,
		[Log Record Length],
		[Page ID],
		AllocUnitName,
		Description
FROM	sys.fn_dblog(NULL, NULL)
WHERE	Context != 'LCX_NULL' AND
		LEFT([Current LSN], LEN([Current LSN]) - 5) IN
		(
			SELECT	LEFT([Current LSN], LEN([Current LSN]) - 5)
			FROM	sys.fn_dblog(NULL, NULL)
			WHERE	[Transaction Name] = 'UpdateRecord')
ORDER BY
		[Current LSN];

CHECKPOINT;
GO

SET STATISTICS IO ON;
GO

SELECT * FROM dbo.demo_table AS h;
GO

SET STATISTICS IO OFF;
GO

-- How many pages do we have?
SELECT	database_id,
		OBJECT_NAME(object_id)	AS	object_name,
		index_type_desc,
		page_count
FROM	sys.dm_db_index_physical_stats
(
	DB_ID(),
	OBJECT_ID(N'dbo.demo_table'),
	0,
	NULL,
	'DETAILED'
);
GO

-- What pages have been allocated by the table
SELECT	p.*, h.*
FROM	dbo.demo_table AS h
		CROSS APPLY sys.fn_PhysLocCracker(%%physloc%%) AS p;
GO

-- see the physical situation on the data page
DBCC TRACEON(3604);
DBCC PAGE (demo_db, 1, 162, 3);
DBCC PAGE (demo_db, 1, 187, 3);

-- system internals of the data structure of dbo.demo_table
SELECT	DDDPA.page_type,
		DDDPA.allocation_unit_type_desc,
		DDDPA.is_mixed_page_allocation,
		DDDPA.allocated_page_iam_page_id,
		DDDPA.previous_page_page_id,
		DDDPA.allocated_page_page_id,
		DDDPA.next_page_page_id,
		DDDPA.page_free_space_percent
FROM	sys.dm_db_database_page_allocations
		(
			DB_ID(),
			OBJECT_ID(N'dbo.demo_table', N'U'),
			0,
			NULL,
			N'DETAILED'
		) AS DDDPA
WHERE	DDDPA.is_allocated = 1
		-- AND DDDPA.page_type != 10
ORDER BY
		DDDPA.page_type DESC,
		DDDPA.allocated_page_page_id ASC;
GO

-- Scan the IAM to know your path you have to walk!
DBCC PAGE ('demo_db', 1, 150, 3);
GO

-- Go to the first page on your route...
DBCC PAGE (0, 1, 147, 3);	-- 1 IO
-- now you'll find a forward stub where you have to go to!
DBCC PAGE (0, 1, 156, 3);	-- 2 IO
-- but you didn't read the full FIRST page so please go back
-- and read the rest of the first page!
-- This will not count as additional IO because you accessed
-- the page already!
DBCC PAGE (0, 1, 147, 3);	-- 0 IO
-- but your route description mention to go to the second page!
DBCC PAGE (0, 1, 156, 3);	-- 1 IO
-----------------------------------
							-- 3 IO
GO

CHECKPOINT;
GO

-- Clean the kitchen
IF OBJECT_ID('dbo.demo_table', 'U') IS NOT NULL
	DROP TABLE dbo.demo_table;
	GO
