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

/*
	Execute the script "02 - insert records in a heap.sql to create an
	extended event "write_heap_data" for the detailed analysis
*/
INSERT INTO heap.customers
(c_custkey, c_mktsegment, c_nationkey, c_name, c_address, c_phone, c_acctbal, c_comment)
VALUES
(1, 'IT SERVICE', 6, 'db Berater GmbH', 'Buechenweg 4, 64390 Erzhausen', '01234-9876', 0, 'best SQL Expert :)');
GO

/* After the record has been inserted we stop the recording ... */
ALTER EVENT SESSION [write_heap_pages] ON SERVER
	DROP EVENT sqlserver.lock_acquired;
GO

/* ... and run the analysis of the process */
EXEC dbo.sp_read_xevent_locks
		@xevent_name = N'write_heap_pages'
		, @filter_condition = N'activity_id LIKE N''ECF7DE81-E42A-456C-BB67-87D24D5B8004%''';
GO

/*
	When we insert a new record to the table the locking is a bit different

	Execute the script "02 - insert records in a heap.sql to create an
	extended event "write_heap_data" for the detailed analysis
*/
INSERT INTO heap.customers
(c_custkey, c_mktsegment, c_nationkey, c_name, c_address, c_phone, c_acctbal, c_comment)
VALUES
(1, 'IT SERVICE', 6, 'db Berater GmbH', 'Buechenweg 4, 64390 Erzhausen', '01234-9876', 0, 'best SQL Expert :)');
GO

/* After the record has been inserted we stop the recording ... */
ALTER EVENT SESSION [write_heap_pages] ON SERVER
	DROP EVENT sqlserver.lock_acquired;
GO

/* ... and run the analysis of the process */
EXEC dbo.sp_read_xevent_locks
		@xevent_name = N'write_heap_pages'
		, @filter_condition = N'activity_id LIKE N''AA3D9DAE-87ED-446C-A9A6-340FE3A14969%''';
GO

IF EXISTS (SELECT * FROM sys.server_event_sessions WHERE name = N'write_heap_pages')
BEGIN
	RAISERROR (N'dropping existing extended event session [write_heap_pages]...', 0, 1) WITH NOWAIT;
	DROP EVENT SESSION [write_heap_pages] ON SERVER;
END
GO
