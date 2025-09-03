/*
	============================================================================
	File:		01 - Preparation of demo databases.sql

	Summary:	This script restores the database ERP_Demo from
				the backup medium for distribution of data.
				
				THIS SCRIPT IS PART OF THE TRACK:
					Session - Demystifying Clustered Indexes

	Date:		October 2024
	Revion:		August 2025

	SQL Server Version: >= 2016
	============================================================================
*/
USE master;
GO

/*
	Make sure you've executed the script 00 - dbo.sp_restore_erp_demo.sql
	before you run this code!
*/
EXEC dbo.sp_restore_ERP_demo @query_store = 1;
GO

/* reset the sql server default settings for the demos */
EXEC ERP_Demo.dbo.sp_set_sql_server_defaults;
GO

SELECT * FROM ERP_Demo.dbo.get_database_help_info();
SELECT * FROM ERP_Demo.dbo.get_object_help_info(NULL);
GO
