/*
	============================================================================
	This script restores a big database "ERP_demo"
	from a backup path to your currently connected
	Microsoft SQL Server.

	The database has been created with Microsoft SQL Server 2016
	and can only be used with >= 2016

	Before you run the script you have to make sure that you...

	-- run this script in SQLCMD modues
	-- change the path definition in the SQLCMD variables

	-- DataPath:	Path to the file location of database files
	-- LogPath:		Path to the file location of the log files
	-- BackupPath:	Path to the backup file which needs to be restored
	============================================================================
*/
-- Do not change this parameter!
:SETVAR	DatabaseName	ERP_Demo
:SETVAR BackupPath		S:\Backup\ERP_DEMO_2012.BAK

USE master;
GO

IF OBJECT_ID(N'dbo.sp_restore_ERP_demo', N'P') IS NOT NULL
	DROP PROCEDURE dbo.sp_restore_ERP_demo;
	GO

CREATE OR ALTER PROCEDURE dbo.sp_restore_ERP_demo
	@query_store	BIT = 1
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @activity_result TABLE
	(
		id			INT				NOT NULL	IDENTITY (1, 1) PRIMARY KEY CLUSTERED,
		activity	NVARCHAR(256)	NOT NULL,
		result		NVARCHAR(256)	NULL,
		start_time	DATETIME2(0)	NOT NULL	DEFAULT (GETDATE()),
		finish_time	DATETIME2(0)	NULL
	);

	IF DB_NAME(DB_ID()) = N'$(DatabaseName)'
	BEGIN
		RAISERROR (N'Get out of database $(DatabaseName)', 0, 1) WITH NOWAIT;
		RETURN;
	END

	IF DB_ID(N'$(DatabaseName)') IS NOT NULL
	BEGIN
		INSERT INTO @activity_result (activity) VALUES (N'delete database backup history');
		RAISERROR (N'delete database backup history...', 0, 1) WITH NOWAIT;
		EXEC msdb.dbo.sp_delete_database_backuphistory @database_name = N'$(DatabaseName)'

		UPDATE	@activity_result
		SET		finish_time = GETDATE()
		WHERE	id = (SELECT MAX(id) FROM @activity_result);

		INSERT INTO @activity_result (activity) VALUES (N'Dropping existing database $(DatabaseName)');		
		RAISERROR (N'Dropping existing database $(DatabaseName)...', 0, 1) WITH NOWAIT;
		BEGIN TRY
			EXEC sp_executesql N'ALTER DATABASE $(DatabaseName) SET SINGLE_USER WITH ROLLBACK IMMEDIATE;';
			EXEC sp_executesql N'DROP DATABASE $(DatabaseName);';
		END TRY
		BEGIN CATCH
			SELECT	ERROR_NUMBER()	AS	error_number,
					ERROR_MESSAGE()	AS	error_message;
			GOTO ExitCode;
		END CATCH

		UPDATE	@activity_result
		SET		finish_time = GETDATE()
		WHERE	id = (SELECT MAX(id) FROM @activity_result);
	END

	INSERT INTO @activity_result (activity) VALUES (N'Restoring database $(DatabaseName)');
	RAISERROR (N'Restoring database $(DatabaseName)', 0, 1) WITH NOWAIT;
	DECLARE	@DataPath	NVARCHAR(256) = CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS NVARCHAR(256)) + N'$(DatabaseName)_01.mdf';;
	DECLARE @LogPath	NVARCHAR(256) = CAST(SERVERPROPERTY('InstanceDefaultLogPath') AS NVARCHAR(256)) + N'$(DatabaseName).ldf';

	RESTORE DATABASE $(DatabaseName)
	FROM DISK = N'$(BackupPath)'
	WITH
		MOVE N'$(DatabaseName)' TO @DataPath,
		MOVE N'$(DatabaseName)_Log' TO @LogPath,
		STATS = 10,
		REPLACE,
		RECOVERY;

	UPDATE	@activity_result
	SET		finish_time = GETDATE()
	WHERE	id = (SELECT MAX(id) FROM @activity_result);

	INSERT INTO @activity_result (activity) VALUES (N'modifying database settings for $(DatabaseName)');
	ALTER AUTHORIZATION ON DATABASE::$(DatabaseName) TO sa;
	ALTER DATABASE $(DatabaseName) SET RECOVERY SIMPLE;

	IF @query_store = 1
	BEGIN TRY
		ALTER DATABASE $(DatabaseName) SET QUERY_STORE = ON;
		ALTER DATABASE $(DatabaseName) SET QUERY_STORE
		(
			OPERATION_MODE = READ_WRITE,
			QUERY_CAPTURE_MODE = AUTO,
			CLEANUP_POLICY = (STALE_QUERY_THRESHOLD_DAYS = 7),
			MAX_STORAGE_SIZE_MB = 1024
		);
	END TRY
	BEGIN CATCH
		SELECT	ERROR_NUMBER() AS ErrorNumber,
				ERROR_MESSAGE() AS ErrorMessage
	END CATCH

	UPDATE	@activity_result
	SET		finish_time = GETDATE()
	WHERE	id = (SELECT MAX(id) FROM @activity_result);

	-- Output if information about database
	SELECT	D.database_id,
			D.name,
			SUSER_SNAME(D.owner_sid)	AS	owner,
			D.create_date,
			D.compatibility_level,
			SUM (CASE WHEN MF.type_desc = N'LOG' THEN 0 ELSE MF.size END / 128.0) AS data_file_size_mb,
			SUM (CASE WHEN MF.type_desc = N'ROWS' THEN 0 ELSE MF.size END / 128.0) AS log_file_size_mb,
			D.collation_name
	FROM	sys.databases AS D INNER JOIN
			sys.master_files AS MF
			ON (D.database_id = MF.database_id)
	WHERE	D.name = N'$(DatabaseName)'
	GROUP BY
			D.database_id,
			D.name,
			D.owner_sid,
			D.create_date,
			D.compatibility_level,
			D.collation_name;

	/* Output of database properties */
	IF DB_ID(N'$(DatabaseName)') > 0
		SELECT	name,
				value
		FROM	$(DatabaseName).sys.extended_properties
		WHERE	class_desc = N'DATABASE'
		ORDER BY
				name;

ExitCode:
	SELECT	*
	FROM	@activity_result
	ORDER BY
			id;
END
GO

EXEC sp_ms_marksystemobject 'dbo.sp_restore_ERP_Demo';
GO

