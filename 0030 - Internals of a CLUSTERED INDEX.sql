/*============================================================================
	File:		0020 - Internals of a CLUSTERED INDEX.sql

	Summary:	This script demonstrates the internal differences between
				a HEAP and a CLUSTERED INDEX.

	Attention:	This script will use undocumented functions of Microsoft SQL Server.
				Use this script not in a productive environment!

	Date:		June 2015

	SQL Server Version: 2012 / 2014 / 2016
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
USE CustomerOrders;
GO

-- make the former heap [dbo].[Customers] a clustered index!
CREATE UNIQUE CLUSTERED INDEX cuix_Customers_Id
ON dbo.Customers (Id);
GO

/*
	former distribution of pages in heap:
			total_pages	used_pages	data_pages
	HEAP:	689			683			682
*/
SELECT	IAU.total_pages,
		IAU.used_pages,
		IAU.data_pages
FROM	sys.partitions AS P INNER JOIN sys.system_internals_allocation_units AS IAU
		ON (P.partition_id = IAU.container_id)
WHERE	P.object_id = OBJECT_ID(N'dbo.Customers', N'U');
GO

-- activate statistics for the info
SET STATISTICS IO ON;
GO

-- a clustered index scan requires access to the intermediate level pages!
SELECT	*
FROM	dbo.Customers AS C;
GO

-- a filter predicate turns into a seek predicate if it is the
-- clustered key!
SELECT * FROM dbo.Customers WHERE Id = 10;
GO

-- How does Microsoft SQL Server produce 3 IO?
-- system internals of the data structure of dbo.Customers
SELECT	DDDPA.allocation_unit_type_desc,
		DDDPA.page_type_desc,
		DDDPA.is_mixed_page_allocation,
		DDDPA.allocated_page_iam_page_id,
		DDDPA.page_level,
		DDDPA.previous_page_page_id,
		DDDPA.allocated_page_page_id,
		DDDPA.next_page_page_id,
		DDDPA.page_free_space_percent
FROM	sys.dm_db_database_page_allocations
		(
			DB_ID(),
			OBJECT_ID(N'dbo.Customers', N'U'),
			1,
			NULL,
			N'DETAILED'
		) AS DDDPA
WHERE	DDDPA.is_allocated = 1
ORDER BY
		DDDPA.page_type DESC,
		DDDPA.page_level DESC,
		DDDPA.allocated_page_page_id ASC;
GO

-- Look into the ROOT-node of the clustered index!
DBCC TRACEON (3604);
DBCC PAGE (CustomerOrders, 1, 30736, 3);
DBCC PAGE (CustomerOrders, 1, 30904, 3);
GO

-- Look into the intermediate level of the clustered index!
DBCC PAGE (CustomerOrders, 1, 30848, 3);
GO

-- Look into the leaf level of the clustered index!
DBCC PAGE (CustomerOrders, 1, 30776, 3) WITH TABLERESULTS;
GO