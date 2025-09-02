/*============================================================================
	File:		0100 - UPDATE in a CLUSTERED INDEX and PAGE SPLIT.sql

	Summary:	This script creates a relation dbo.demo_table for the demonstration
				of UPDATE-Internals for Clustered Indexes

				THIS SCRIPT IS PART OF THE TRACK: "Clustered Indexes - Pro and Con"

	Date:		July 2015

	SQL Server Version: 2008 / 2012
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
GO

SET LANGUAGE us_english;
SET NOCOUNT ON;
GO

/*
	Demo 1:	Simple insert of data into a brand new Clustered index
*/
IF OBJECT_ID('dbo.demo_table', 'U') IS NOT NULL
	DROP TABLE dbo.demo_table;
	GO

CREATE TABLE dbo.demo_table
(
	Id		int				NOT NULL,
	col1	char(200)		NOT NULL	DEFAULT ('some stuff'),
	col2	varchar(2000)	NOT NULL	DEFAULT ('some more stuff'),
	col3	datetime		NOT NULL	DEFAULT (getdate()),
	OrdPos	int				NOT NULL	IDENTITY (1, 1)
);
GO

CREATE UNIQUE CLUSTERED INDEX cix_tbl_cluster_Id ON dbo.demo_table (Id);
GO

-- Insert 66 records to match exactly 2 data pages
DECLARE	@i int = 1;
WHILE @i <= 66
BEGIN
	INSERT INTO dbo.demo_table (Id, col1, col2, col3) VALUES (@i, DEFAULT, DEFAULT, DEFAULT);
	SET @i += 1;
END
GO

-- get the physical and logical location of the data
SELECT sys.fn_PhysLocFormatter(%%physloc%%) AS Location, * FROM dbo.demo_table;
GO

CHECKPOINT;
GO

-- Update a record and check the amount of produced transaction log
BEGIN TRANSACTION UpdateRecord
GO

	UPDATE	dbo.demo_table
	SET		col1 = 'This is a brand new text for me!'
	WHERE	Id = 3;
	GO

	-- what resources are blocked?
	SELECT	resource_type,
			resource_description,
			resource_associated_entity_id,
			u.type,
			u.type_desc,
			OBJECT_NAME(ISNULL(p.object_id, l.resource_associated_entity_id))	AS	object_name,
			request_mode,
			request_type
	FROM	sys.dm_tran_locks l LEFT JOIN sys.allocation_units u
			ON	(l.resource_associated_entity_id = u.container_id) LEFT JOIN sys.partitions p
			ON	(
					u.container_id = 
						CASE WHEN u.type IN (1, 3)
							THEN p.hobt_id
							ELSE p.partition_id
						END
				)
	WHERE	resource_database_id = db_id() AND
			request_session_id = @@SPID;
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
		AllocUnitId,
		AllocUnitName,
		[RowLog Contents 0]		AS	OldData,
		[RowLog Contents 1]		AS	NewData
FROM	sys.fn_dblog(NULL, NULL)
WHERE	Context != 'LCX_NULL' AND
		LEFT([Current LSN], LEN([Current LSN]) - 5) =
		(
			SELECT	LEFT([Current LSN], LEN([Current LSN]) - 5)
			FROM	sys.fn_dblog(NULL, NULL)
			WHERE	[Transaction Name] = 'UpdateRecord')
ORDER BY
		[Current LSN];
GO

-- although we have a char(200) only the exact length of the replaced stuff will be logged!
SELECT	CAST (0x736F6D6520737475666620202020202020202020202020202020202020202020 AS varchar(200)),
		CAST (0x546869732069732061206272616E64206E6577207465787420666F72206D6521 AS varchar(200))

CHECKPOINT;
GO

SELECT	p.*, c.*
FROM	dbo.demo_table AS c CROSS APPLY sys.fn_PhysLocCracker(%%physloc%%) AS p;

-- If the variable length is to long for the data page
-- a page split will occur!
BEGIN TRANSACTION UpdateRecord
GO
	UPDATE	dbo.demo_table
	SET		col2 = REPLICATE ('A', 1000)
	WHERE	Id = 1;

	-- see the amount of operations which took place!
	SELECT	database_transaction_log_bytes_used,
			database_transaction_log_record_count
	FROM	sys.dm_tran_database_transactions
	WHERE	database_id = db_id();
	GO

COMMIT TRANSACTION UpdateRecord
GO

SELECT	p.*, c.*
FROM	dbo.demo_table AS c CROSS APPLY sys.fn_PhysLocCracker(%%physloc%%) AS p;

-- Clean the kitchen
IF OBJECT_ID('dbo.demo_table', 'U') IS NOT NULL
	DROP TABLE dbo.demo_table;
	GO