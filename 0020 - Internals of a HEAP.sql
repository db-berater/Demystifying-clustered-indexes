/*============================================================================
	File:		0020 - Internals of a HEAP.sql

	Summary:	This script demonstrates the internal differences between
				a HEAP and a CLUSTERED INDEX.

	Attention:	This script will use undocumented functions of Microsoft SQL Server.
				Use this script not in a productive environment!

	Date:		June 2015

	SQL Server Version: 2012 / 2014 / 2016 / 2017
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
GO

USE CustomerOrders;
GO

/*
	total_pages	used_pages	data_pages
	689			683			682
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

SELECT * FROM dbo.Customers AS C
GO

-- even if a filter predicate is used the complete table has to be scanned!
SELECT * FROM dbo.Customers WHERE Id = 10;
GO

SELECT * FROM dbo.Customers WHERE Id = 10
OPTION (QUERYTRACEON 9130);
GO

-- what will happen if the option TOP will be used?
SELECT TOP 1 * FROM dbo.Customers WHERE Id = 10;
GO

SELECT TOP 1 * FROM dbo.Customers WHERE Id = 10205
OPTION (QUERYTRACEON 9130);
GO

-- system internals of the data structure of dbo.Customers
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
			OBJECT_ID(N'dbo.Customers', N'U'),
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

-- Look into the IAM and the first data page for the Id = 10
DBCC TRACEON (3604);
DBCC PAGE (CustomerOrders, 1, 40152, 3);
DBCC PAGE (CustomerOrders, 1, 24519, 3);
GO

-- even if a filter predicate is used the complete table has to be scanned!
SELECT * FROM dbo.Customers WHERE Id = 10
OPTION (QUERYTRACEON 9130);
GO

-- what will happen if the option TOP will be used?
SELECT TOP 1 * FROM dbo.Customers WHERE Id = 10 OPTION (QUERYTRACEON 9130);
SELECT TOP 1 * FROM dbo.Customers WHERE Id = 74000 OPTION (QUERYTRACEON 9130);
GO