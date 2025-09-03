/*
	============================================================================
	File:		01 - read data pages in a heap.sql

	Summary:	creates an Extended Event to track, what pages will
				SQL Server read when it scans a heap
				
				THIS SCRIPT IS PART OF THE TRACK:
					Session: Demystifying clustered indexes

	Date:		January 2025

	SQL Server Version: >= 2016
	============================================================================
*/
USE master;
GO

/*
	NOTE:		RUN THIS SCRIPT IN SQLCMD MODUS!!!

	Explanation of variables:
	EventName:	Name of the Extended Event session

	session_id:		session_id to track
*/
:SETVAR EventName			read_heap_pages
:SETVAR	session_id			52

PRINT N'-------------------------------------------------------------';
PRINT N'| Installation script by db Berater GmbH                     |';
PRINT N'| https://www.db-berater.de                                  |';
PRINT N'| Uwe Ricken - uwe.ricken@db-berater.de                      |';
PRINT N'-------------------------------------------------------------';
GO

IF EXISTS (SELECT * FROM sys.server_event_sessions WHERE name = N'$(EventName)')
BEGIN
	RAISERROR (N'dropping existing extended event session $(EventName)...', 0, 1) WITH NOWAIT;
	DROP EVENT SESSION [$(EventName)] ON SERVER;
END
GO

RAISERROR (N'creating extended event session $(EventName)...', 0, 1) WITH NOWAIT;
CREATE EVENT SESSION [$(EventName)] ON SERVER 
ADD EVENT sqlserver.sql_batch_starting
(
	ACTION (sqlserver.session_id)
	WHERE
	(
		sqlserver.is_system = 0
		AND sqlserver.session_id = $(session_id)
	)
),
ADD EVENT sqlserver.sql_batch_completed
(
	ACTION
	(sqlserver.session_id)
	WHERE
	(
		sqlserver.is_system = 0
		AND sqlserver.session_id = $(session_id)
	)
),
ADD EVENT sqlserver.lock_acquired
(
	ACTION (sqlserver.session_id)
	WHERE 
		sqlserver.is_system = 0
		AND sqlserver.session_id = $(session_id)
		AND	resource_type = N'PAGE'
		AND
		(
			mode = N'IS'
			OR mode = 'S'
		)
)
ADD TARGET package0.ring_buffer (SET max_events_limit = 0, max_memory = 10240)
WITH
(
	MAX_MEMORY=4096 KB,
	EVENT_RETENTION_MODE=NO_EVENT_LOSS,
	MAX_DISPATCH_LATENCY= 5 SECONDS,
	MAX_EVENT_SIZE=0 KB,
	MEMORY_PARTITION_MODE=NONE,
	TRACK_CAUSALITY=ON,
	STARTUP_STATE=OFF
);
GO

ALTER EVENT SESSION $(EventName) ON SERVER STATE = START;
GO