/*
	============================================================================
	File:		02 - Reading data from a heap.sql

	Summary:	This demo shows how Microsoft SQL Server will read data from
				a heap table.

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

/*
	In the first step we create again the table heap.customers
	and fill it with 1.6 mio rows
*/
DROP TABLE IF EXISTS heap.customers;
GO

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
WHERE	c_custkey <= 5000
OPTION	(MAXDOP 1);
GO

/*
	Before we start the demos let's figure out what data pages
	have been allocated by the demo table [heap].[customers].

	Note:	The function is part of the framework of the demo database
			https://www.db-berater.de/downloads/ERP_DEMO_2012.BAK	
*/
SELECT	type_desc,
        rows,
        total_pages,
        used_pages,
        data_pages,
        space_mb,
        root_page,
        first_iam_page
FROM	dbo.get_table_pages_info(N'heap.customers', 0);
GO

/*
	In the first step we check the execution plan and the IO
*/
SET STATISTICS IO, TIME ON;
GO

SELECT * FROM heap.customers;
GO

SELECT * FROM heap.customers WHERE c_custkey = 10;
GO

SET STATISTICS IO, TIME OFF;
GO

/*
	Let's execute the script 01 - read data pages in a heap.sql
	from the folder [97 - Extended Events] to trace all
	locks from a dedicated session!

	Afterwards we run the SELECT command again and check the
	locks on the heap.customers table
*/
SELECT * FROM heap.customers;
GO

ALTER EVENT SESSION [read_heap_pages] ON SERVER
	DROP EVENT sqlserver.lock_acquired;
GO

EXEC dbo.sp_read_xevent_locks
		@xevent_name = N'read_heap_pages';
GO

/* Now we can drop the extended event */
IF EXISTS (SELECT * FROM sys.server_event_sessions WHERE name = N'read_heap_pages')
BEGIN
	RAISERROR (N'dropping existing extended event session [read_heap_pages]...', 0, 1) WITH NOWAIT;
	DROP EVENT SESSION [read_heap_pages] ON SERVER;
END
GO