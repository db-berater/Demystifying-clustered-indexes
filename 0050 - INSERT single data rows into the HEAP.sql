/*============================================================================
	File:		0070 - INSERT single data rows into the HEAP.sql

	Summary:	This script creates a relation dbo.demo_table for the demonstration
				of INSERT-Internals for HEAPS

				THIS SCRIPT IS PART OF THE TRACK: "CLUSTERED INDEXES vs HEAPS"

	Date:		Juli 2015

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
SET NOCOUNT ON;
SET LANGUAGE us_english;
USE master;
GO

EXEC sp_create_demo_db;
GO

USE demo_db;
GO

/*
	===================================================================================
	Demo: Storage by using PFS for checking free space!
	===================================================================================
*/

-- Create a table dbo.demo_table as the HEAP and fill it with a few data
IF OBJECT_ID('dbo.demo_table', 'U') IS NOT NULL
	DROP TABLE dbo.demo_table;
	GO

CREATE TABLE dbo.demo_table
(
	Id	int			NOT NULL	IDENTITY (1, 1),
	c1	char(2500)	NOT NULL
);

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

	INSERT INTO dbo.demo_table (c1)
	VALUES
	('Uwe Ricken'),
	('Beate Ricken'),
	('Alicia Ricken');
	GO

COMMIT TRANSACTION InsertRecords
GO

-- What pages will be allocated by the table
SELECT	p.*, h.*
FROM	dbo.demo_table AS h
		CROSS APPLY sys.fn_PhysLocCracker(%%physloc%%) AS p;
GO

DBCC TRACEON (3604);
DBCC PAGE (0, 1, 1, 3);

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

-- flush the changes to disk
CHECKPOINT;
GO

BEGIN TRANSACTION InsertRecords;
GO

	INSERT INTO dbo.demo_table (c1)
	VALUES
	('Katharina Ricken')
	GO

	INSERT INTO dbo.demo_table (c1)
	VALUES
	('Emma Ricken');
	GO

	INSERT INTO dbo.demo_table (c1)
	VALUES
	('Josie Ricken');
	GO

COMMIT TRANSACTION InsertRecords;
GO

-- What pages will be allocated by the table
SELECT	p.*, h.*
FROM	dbo.demo_table AS h
		CROSS APPLY sys.fn_PhysLocCracker(%%physloc%%) AS p;
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

-- what is the free space on the data page of the first two records?
DBCC TRACEON (3604);
DBCC PAGE ('demo_db', 1, 1, 3);
GO

-- Look on the page header of both affected pages
DBCC TRACEON (3604);
DBCC PAGE ('demo_db', 1, 289, 0);
DBCC PAGE ('demo_db', 1, 290, 0);
GO

-- Clean the kitchen!
IF OBJECT_ID('dbo.demo_table', 'U') IS NOT NULL
	DROP TABLE dbo.demo_table;
	GO