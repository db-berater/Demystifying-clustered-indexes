/*
	============================================================================
	File:		01 - Management of data in a heap.sql

	Summary:	This script demonstrates the organisation of Leaf Pages in a Heap.
				It shows the organisation/management of Leaf Pages in the IAM Page.

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

IF SCHEMA_ID(N'heap') IS NULL
	EXEC sp_executesql N'CREATE SCHEMA [heap] AUTHORIZATION dbo;';
GO

DROP TABLE IF EXISTS heap.customers;
GO

/*
	We use MAXDOP 1 because the table dbo.customers is a HEAP, too!
	For an explanation see the section "Problems"	
*/
SELECT	c_custkey,
        c_mktsegment,
        c_nationkey,
        c_name,
        c_address,
        c_phone,
        c_acctbal,
        c_comment
INTO	heap.customers
FROM	dbo.customers
WHERE	c_custkey <= 10
OPTION	(MAXDOP 1);
GO

SET STATISTICS IO ON;
GO

SELECT sys.fn_PhysLocFormatter(%%physloc%%)	AS record_location, * FROM heap.customers;
GO


/*
	Information about the allocated pages in a dedicated table
	Note:	The function is part of the framework of the demo database
			https://www.db-berater.de/downloads/ERP_DEMO_2012.BAK		
*/
SELECT	index_id,
        rows,
        total_pages,
        used_pages,
        data_pages,
        space_mb,
        root_page,
        first_iam_page
FROM	dbo.get_table_pages_info(N'heap.customers', NULL)
GO

/*
	Check the IAM Page
	(1:2516991:0)
*/
DBCC TRACEON (3604);
DBCC PAGE (0, 1, 2516991, 3);
GO

/*
	From the IAM Page we can go to the first Leaf Page
*/
DBCC PAGE (0, 1, 182304, 3) WITH TABLERESULTS;
GO

/* What data pages belong to the table? */
SELECT	dpa.index_id,
		dpa.allocation_unit_type_desc,
		dpa.extent_page_id,
		dpa.allocated_page_page_id,
		dpa.is_allocated,
		dpa.page_free_space_percent,
		dpa.page_type_desc
FROM	sys.tables AS t
		INNER JOIN sys.indexes AS i
		ON (t.object_id = i.object_id)
		CROSS APPLY sys.dm_db_database_page_allocations
		(
			DB_ID(),
			t.object_id,
			i.index_id,
			NULL,
			N'DETAILED'
		) AS dpa
WHERE	t.object_id = OBJECT_ID(N'heap.customers')
		AND dpa.is_iam_page = 0;
GO

/*
	Now we can clean the kitchen for the next demos
*/
DROP TABLE IF EXISTS heap.customers;
GO
