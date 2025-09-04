/*
	============================================================================
	File:		03 - monitor Latch Contention in ERP_Demo.sql

	Summary:	creates an Extended Event to track, every LATCH Contention in
				the ERP_Demo database.
				This extended event should show the massive impact on a PFS Page
				when a big amount of data will be inserted into a HEAP.
				
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

	Check the database_id of ERP_Demo with the following line:
	SELECT	DB_ID(N'demo_db');

	Explanation of variables:
	EventName:		Name of the Extended Event session
	database_Id:	database_id of demo database ERP_Demo
*/

:SETVAR EventName			monitor_latch_contention
:SETVAR	database_id			15

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

CREATE EVENT SESSION [$(EventName)]
ON SERVER 
ADD EVENT sqlserver.latch_suspend_end
(
    WHERE	sqlserver.database_id = $(database_id)
			AND duration >= 1000	/* microseconds! */
			AND
			(
				page_type_id = 'PFS_PAGE'
				OR page_type_id = 'GAM_PAGE'
				OR page_type_id = 'SGAM_PAGE'
			)
)
ADD TARGET package0.ring_buffer,
ADD TARGET package0.histogram
(
	SET	filtering_event_name = N'sqlserver.latch_suspend_end',
		source=N'page_type_id',
		source_type = 0
)
WITH
(
	MAX_MEMORY=4096 KB,
	EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS,
	MAX_DISPATCH_LATENCY = 5 SECONDS,
	MAX_EVENT_SIZE = 0 KB,
	MEMORY_PARTITION_MODE = NONE,
	TRACK_CAUSALITY = OFF,
	STARTUP_STATE = OFF
)
GO

ALTER EVENT SESSION $(EventName) ON SERVER STATE = START;
GO