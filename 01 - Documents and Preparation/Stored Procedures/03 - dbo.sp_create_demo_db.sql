USE ERP_Demo;
GO

CREATE OR ALTER PROCEDURE dbo.sp_create_demo_db
	@num_of_files		SMALLINT	= 1,
	@initial_size_MB	INT			= 1024
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	/*
		If the initial size is smaller than the default we quit the procedure
	*/
	DECLARE	@default_size_mb	INT;
	SELECT	@default_size_mb = size
	FROM	sys.master_files
	WHERE	database_id = DB_ID(N'model')
			AND file_id = 1;

	IF (@initial_size_MB < @default_size_mb)
	BEGIN
		RAISERROR ('The initial_size_mb must be at least %i MB', 0, 1, @default_size_mb) WITH NOWAIT;
		RETURN 1;
	END

	DECLARE	@data_path	NVARCHAR(256)	= CAST(SERVERPROPERTY(N'InstanceDefaultDataPath') AS NVARCHAR(256));
	DECLARE	@log_path	NVARCHAR(256)	= CAST(SERVERPROPERTY(N'InstanceDefaultLogPath') AS NVARCHAR(256));

	IF DB_ID(N'demo_db') IS NOT NULL
	BEGIN
		RAISERROR ('dropping existing database [demo_db]', 0, 1) WITH NOWAIT;
		ALTER DATABASE [demo_db] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
		DROP DATABASE [demo_db];
	END

	DECLARE	@sql_cmd			NVARCHAR(MAX) = N'CREATE DATABASE [demo_db]
ON PRIMARY
';
	DECLARE	@file_specs			NVARCHAR(4000) = N'';
	DECLARE	@const_file_name	NVARCHAR(128) = N'demo_db_%';
	DECLARE	@var_file_name		NVARCHAR(128);
	DECLARE	@counter	INT = 1;

	WHILE @counter <= @num_of_files
	BEGIN
		SET	@var_file_name = REPLACE(@const_file_name, '%', CAST(@counter AS NVARCHAR(3)));
		SET	@file_specs = N'(NAME = ' + QUOTENAME(@var_file_name, '''') + N', SIZE = ' + CAST(@initial_size_MB AS NVARCHAR(16)) + N'MB, FILENAME = ''' + @data_path + @var_file_name + N'.mdf''),'

		SET	@sql_cmd = @sql_cmd + @file_specs + CHAR(10)
		SET	@counter += 1;
	END

	SET	@sql_cmd = LEFT(@sql_cmd, LEN(@sql_cmd) - 2) + CHAR(10);

	/* Add the log file information */
	SET	@sql_cmd = @sql_cmd + N'LOG ON
(
	NAME = ''demo_db'',
	SIZE = 256MB,
	FILENAME = ''' + @log_path + N'demo_db.ldf''
);'

	PRINT @sql_cmd;
	BEGIN TRY
		EXEC sp_executesql @sql_cmd;
		EXEC sp_executesql N'ALTER DATABASE [demo_db] SET RECOVERY SIMPLE;';
		EXEC sp_executesql N'ALTER AUTHORIZATION ON DATABASE::[demo_db] TO sa;';
	END TRY
	BEGIN CATCH
		SELECT	ERROR_NUMBER()	AS	ERROR_NUMBER,
				ERROR_MESSAGE()	AS	ERROR_MESSAGE;

		RETURN 1;
	END CATCH

	RETURN 0;
END
GO

EXEC sp_create_demo_db
	@num_of_files = 4,
	@initial_size_mb = 1024;