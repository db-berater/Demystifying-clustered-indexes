/*============================================================================
	File:		0040 - Internals of a NON Clustered Index.sql

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

SET STATISTICS IO ON;
GO

-- SELECT all data from CustomerOrders
-- the query operation will be a TABLE SCAN!
SELECT	Customer_Id,
		OrderNumber,
		InvoiceNumber,
		OrderDate
FROM	dbo.CustomerOrders
WHERE	OrderDate = '2008-12-20'
OPTION (QUERYTRACEON 9130, RECOMPILE);
GO

-- Create an index on the OrderDate column
CREATE INDEX ix_CustomerOrders_OrderDate
ON dbo.CustomerOrders(OrderDate);
GO

-- SELECT all data from CustomerOrders
-- the query operation will be an INDEX SEEK!
SELECT	* FROM dbo.CustomerOrders
WHERE	OrderDate = '2008-12-20';
GO

-- This operation will force a RID-Lookup because of missing details in the index
SELECT	Customer_Id,
		OrderNumber,
		InvoiceNumber,
		OrderDate,
		Employee_Id
FROM	dbo.CustomerOrders
WHERE	OrderDate = '2008-12-20'
OPTION (QUERYTRACEON 9130, RECOMPILE);
GO

SELECT	*
FROM	sys.indexes
WHERE	object_id = OBJECT_ID('dbo.CustomerOrders');

-- how does the non clustered index look like?
-- system internals of the data structure of dbo.Customers
SELECT	DDDPA.allocation_unit_type_desc,
		DDDPA.page_type_desc,
		DDDPA.is_mixed_page_allocation,
		DDDPA.page_level,
		DDDPA.previous_page_page_id,
		DDDPA.allocated_page_page_id,
		DDDPA.next_page_page_id,
		DDDPA.page_free_space_percent
FROM	sys.dm_db_database_page_allocations
		(
			DB_ID(),
			OBJECT_ID(N'dbo.CustomerOrders', N'U'),
			3,
			NULL,
			N'DETAILED'
		) AS DDDPA
WHERE	DDDPA.is_allocated = 1
		AND DDDPA.is_iam_page = 0
ORDER BY
		DDDPA.page_type DESC,
		DDDPA.page_level DESC,
		DDDPA.allocated_page_page_id ASC;
GO

-- Look into the structure!
DBCC TRACEON (3604);
DBCC PAGE (CustomerOrders, 1, 31946, 3);
DBCC PAGE (CustomerOrders, 1, 32008, 3);
DBCC PAGE (CustomerOrders, 1, 32490, 3);
GO

-- now we calculate the position of the very first record in the table
--DECLARE	@RID varbinary(8) = 0x005C0000 0100 1D00
/*
	The RID is a HEX-Value which points directly to the record!
	0x0A4F0000	=	Page	=	20234
	01 00		=	File	=	1
	25 00		=	Slot	=	37
*/
DECLARE	@RID BINARY(8) = 0x0508000001002300

SELECT	CONVERT (VARCHAR(5),
		CONVERT(INT, SUBSTRING(@rid, 6, 1)
		+ SUBSTRING(@rid, 5, 1)) )
		+ ':' +
		CONVERT(VARCHAR(10),
		CONVERT(INT, SUBSTRING(@rid, 4, 1)
		+ SUBSTRING(@rid, 3, 1)
		+ SUBSTRING(@rid, 2, 1)
		+ SUBSTRING(@rid, 1, 1)) )
		+ ':' +
	CONVERT(VARCHAR(5),
		CONVERT(INT, SUBSTRING(@rid, 8, 1)
					+ SUBSTRING(@rid, 7, 1)))
GO

DBCC PAGE (CustomerOrders, 1, 2053, 3) WITH TABLERESULTS;
GO

CREATE UNIQUE CLUSTERED INDEX cix_CustomerOrders_Id
ON dbo.CustomerOrders (Id);
GO

-- rerun the above query for improvement checks
SELECT	Customer_Id,
		OrderNumber,
		InvoiceNumber,
		OrderDate
FROM	dbo.CustomerOrders
WHERE	OrderDate = '2008-12-20'
OPTION	(QUERYTRACEON 9130, RECOMPILE)
GO

-- let's check the index structure
SELECT	P.index_id,
		IAU.*
FROM	sys.partitions AS P INNER JOIN sys.system_internals_allocation_units AS IAU
		ON (P.partition_id = IAU.container_id)
WHERE	P.object_id = OBJECT_ID(N'dbo.CustomerOrders', N'U');
GO

-- move down to the leaf level of the nonclustered index
DBCC TRACEON (3604);
-- Root Level
DBCC PAGE (CustomerOrders, 1, 31690, 3);
GO

-- Intermediate Level
DBCC PAGE (CustomerOrders, 1, 32640, 3);
GO

-- Leaf Level
DBCC PAGE (CustomerOrders, 1, 34182, 3);
GO
