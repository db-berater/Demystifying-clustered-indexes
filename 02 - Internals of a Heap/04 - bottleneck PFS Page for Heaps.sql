/*
	============================================================================
	File:		03 - Inserting data into a heap.sql

	Summary:	This demo shows how internal allocation of resources will work
				when data are inserted into a heap table.

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

USE ERP_Demo;
GO

DROP TABLE IF EXISTS heap.customers;
GO

/*
	We create a new table [heap].[customers] which can store max 3 rows on one data page.
	Note, that you can store 8,060 bytes on one single data page!
*/
CREATE TABLE heap.customers
(
	id	INT			NOT NULL	IDENTITY (1, 1),
	c1	CHAR(2500)	NOT NULL
);
GO

CHECKPOINT;
GO

-- Calculation for the amount of data for one page:
/*
	Pagesize:	8192 Bytes
	Header:		  96 Bytes
	SlotArray:	  36 Bytes
	Data:		8060 Bytes

	1 Record has a size of 2510 Bytes (4 + 2500 + 4 (RowHeader) + 2 (Slot Array))
	8060 Bytes / 2510 Bytes = 3 records + 1556 Bytes!
*/ 

-- Let's insert 3 rows!
BEGIN TRANSACTION InsertRecords
GO

	INSERT INTO heap.customers (c1)
	VALUES
	('Company 001'),
	('Company 002'),
	('Company 003');
	GO

	SELECT	[Current LSN],
            Operation,
            Context,
            [Log Record Length],
            [Log Reserve],
            AllocUnitName,
            [Page ID],
            [Slot ID],
            PartitionId,
            [Lock Information]
	FROM	dbo.get_transaction_info(N'InsertRecords');

COMMIT TRANSACTION InsertRecords
GO

-- What pages will be allocated by the table
SELECT	p.page_id,
		h.id,
		h.c1,
		dpi.pfs_page_id,
        FORMAT
		(
			dpi.pfs_alloc_percent / 100.0,
			N'0.00%',
			N'en-us'
		)	AS	pfs_allocation,
		dpi.free_bytes				AS	real_free_bytes,
		CAST
		(
			8060 * (1.0 - dpi.pfs_alloc_percent / 100.0)
			AS	SMALLINT
		)							AS	calc_free_bytes
FROM	heap.customers AS h
		CROSS APPLY sys.fn_PhysLocCracker(%%physloc%%) AS p
		CROSS APPLY
		(
			SELECT	pfs_page_id,
					pfs_alloc_percent,
					free_bytes
			FROM	sys.dm_db_page_info
					(
						DB_ID(),
						p.file_id,
						p.page_id,
						N'DETAILED'
					)
		)	AS	dpi;
GO

/* look into the transaction protocol! */
SELECT	[Current LSN],
		Operation,
		Context,
		[Log Record Length],
		AllocUnitName,
		Description
FROM	sys.fn_dblog(NULL, NULL)
WHERE	Context != 'LCX_NULL' AND
		AllocUnitName NOT LIKE 'sys.%' AND
		LEFT([Current LSN], LEN([Current LSN]) - 5) IN
		(
			SELECT	LEFT([Current LSN], LEN([Current LSN]) - 5)
			FROM	sys.fn_dblog(NULL, NULL)
			WHERE	[Transaction Name] = 'InsertRecords'
		)
ORDER BY
		[Current LSN];
GO

-- flush the changes to disk
CHECKPOINT;
GO

BEGIN TRANSACTION InsertRecords;
GO

	INSERT INTO heap.customers (c1)
	VALUES
	('Company 004')
	GO

	INSERT INTO heap.customers (c1)
	VALUES
	('Company 004');
	GO

	INSERT INTO heap.customers (c1)
	VALUES
	('Company 004');
	GO

COMMIT TRANSACTION InsertRecords;
GO

-- What pages will be allocated by the table
SELECT	p.page_id,
		h.id,
		h.c1,
		dpi.pfs_page_id,
        FORMAT
		(
			dpi.pfs_alloc_percent / 100.0,
			N'0.00%',
			N'en-us'
		)	AS	pfs_allocation,
		dpi.free_bytes				AS	real_free_bytes,
		CAST
		(
			8060 * (1.0 - dpi.pfs_alloc_percent / 100.0)
			AS	SMALLINT
		)							AS	calc_free_bytes
FROM	heap.customers AS h
		CROSS APPLY sys.fn_PhysLocCracker(%%physloc%%) AS p
		CROSS APPLY
		(
			SELECT	pfs_page_id,
					pfs_alloc_percent,
					free_bytes
			FROM	sys.dm_db_page_info
					(
						DB_ID(),
						p.file_id,
						p.page_id,
						N'DETAILED'
					)
		)	AS	dpi;
GO


-- look into the transaction protocol!
SELECT	[Current LSN],
		Operation,
		Context,
		[Log Record Length],
		AllocUnitId,
		AllocUnitName,
		Description
FROM	sys.fn_dblog(NULL, NULL)
WHERE	Context != 'LCX_NULL' AND
		AllocUnitName NOT LIKE 'sys.%' AND
		LEFT([Current LSN], LEN([Current LSN]) - 5) IN
		(
			SELECT	LEFT([Current LSN], LEN([Current LSN]) - 5)
			FROM	sys.fn_dblog(NULL, NULL)
			WHERE	[Transaction Name] = 'InsertRecords'
		)
ORDER BY
		[Current LSN];
GO

-- Clean the kitchen!
DROP TABLE IF EXISTS heap.customers;
GO