/*
	============================================================================
	File:		04 - Updating data into a heap.sql

	Summary:	This demo shows the workload for Microsoft SQL Server
				when records are updated in a HEAP

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
SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE ERP_Demo;
GO

/* Let's drop the demo table if it exists */
DROP TABLE IF EXISTS heap.customers;
GO

CREATE TABLE heap.customers
(
	c_custkey		BIGINT			NOT NULL,
	c_mktsegment	CHAR(10)		NULL,
	c_nationkey		INT				NOT NULL,
	c_name			VARCHAR(25)		NULL,
	c_address		VARCHAR(40)		NULL,
	c_phone			CHAR(15)		NULL,
	c_acctbal		MONEY			NULL,
	c_comment		VARCHAR(118)	NULL
);
GO

/* Let's insert 10,000 rows as sample data */
INSERT INTO heap.customers
(c_custkey, c_mktsegment, c_nationkey, c_name, c_address, c_phone, c_acctbal, c_comment)
SELECT c_custkey, c_mktsegment, c_nationkey, c_name, c_address, c_phone, c_acctbal, c_comment
FROM	dbo.customers
WHERE	c_custkey <= 1000
OPTION	(MAXDOP 1);
GO

/*
	Now we check the logical fragmentation of the data pages
*/
SELECT	pi.pfs_page_id				AS	pfs_page_id,
        pa.allocated_page_page_id	AS	page_id,
        pa.page_free_space_percent	AS	free_space_percent,
        pa.page_type_desc			AS	page_desc,
		pi.slot_count				AS	num_records,
		pi.free_bytes				AS	free_bytes
FROM	sys.dm_db_database_page_allocations
		(
			DB_ID(),
			OBJECT_ID(N'heap.customers', N'U'),
			0,
			NULL,
			N'DETAILED'
		) AS pa
		CROSS APPLY sys.dm_db_page_info
		(
			DB_ID(),
			pa.allocated_page_file_id,
			pa.allocated_page_page_id,
			N'DETAILED'
		) AS pi;
GO

/*
	When we update a fixed length attribute nothing will happen
	with the allocation of the records
*/