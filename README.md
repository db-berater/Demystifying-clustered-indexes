<a href="URL_REDIRECT" target="blank"><img align="center" src="https://www.db-berater.de/wp-content/uploads/2015/03/db-berater-gmbh-logo.jpg" height="100" /></a>
# Session - Partitioning for Beginners
This repository contains all codes for my session "Demystifying Clustered Indexes". The target group for this session are experienced database programmers who want to use partitioning for their databases.

This session provides an overview of the basics of partitioning with Microsoft SQL Server. The session is always run with the latest version of Microsoft SQL Server.
The repository consists of several folders that are split up by topic.

All scripts are created for the use of Microsoft SQL Server (Version 2016 or higher)
To work with the scripts it is required to have the workshop database [ERP_Demo](https://www.db-berater.de/downloads/ERP_DEMO_2012.BAK) installed on your SQL Server Instance.
The last version of the demo database can be downloaded here:

**https://www.db-berater.de/downloads/ERP_DEMO_2012.BAK**

> Written by
>	[Uwe Ricken](https://www.db-berater.de/uwe-ricken/), 
>	[db Berater GmbH](https://db-berater.de)
> 
> All scripts are intended only as a supplement to demos and lectures
> given by Uwe Ricken.  
>   
> **THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF 
> ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED 
> TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
> PARTICULAR PURPOSE.**

**Note**
The demo database contains a framework for all workshops / sessions from db Berater GmbH
+ Stored Procedures
+ User Definied Inline Functions

**Version:** 2025-01-20

# Folder structure
+ Each topic is stored in a separate folder (e.g. 01 - Documents and Preparation)
+ All scripts have numbers and basically the script with the prefix 01 is for the preparation of the environment
+ The folder **SQL ostress** contains .cmd files as substitute for SQL Query Stress.
   To use ostress you must download and install the **[RML Utilities](https://learn.microsoft.com/en-us/troubleshoot/sql/tools/replay-markup-language-utility)**
   
+ The folder **Windows Admin Center** contains json files with the configuration of performance counter. These files can only be used with Windows Admin Center
  - [Windows Admin Center](https://www.microsoft.com/en-us/windows-server/windows-admin-center)
+ The folder **SQL Query Stress** contains prepared configuration settings for each scenario which produce load test with SQLQueryStress from Adam Machanic
  - [SQLQueryStress](https://github.com/ErikEJ/SqlQueryStress)
+ The folder **SQL Extended Events** contains scripts for the implementation of extended events for the different scenarios
  All extended events are written for "LIVE WATCHING" and will have target file for saving the results.

# 01 - Documents and Preparation
This folder contains the accompanying PowerPoint presentation for the session. Script 00 - dbo.sp_restore_erp_demo.sql can also be used to install a stored procedure in the master database that is used in the scripts for restoring the database.
Script 01 - Preparation of demo database.sql restores the database on the local Microsoft SQL Server and resets the server's properties to the default settings.

# 02 - Partitioning - as it begun
The scripts in this folder are used to demonstrate partitioning in the classic sense by creating a separate table for each year. The tables are then merged into a view using UNION ALL.

# 03 - components of partitioning
The folder provides scripts for demonstrating the use of partition functions and partition schemes.

# 04 - Partitioning - Implementation
This folder contains all scripts used to successfully partition the dbo.orders table. Both the partition function and the partition scheme are explained in more detail.

# 05 - Partitioning - how to use
The folder provides scripts for using the dbo.orders table partitioned in the previously created schema. It demonstrates which requirements the table must meet in order for partitioning to be successful.

# ß6 - Querying Partitioned Data
Scripts in this folder covers topics about partition elimination when using values from the partition boundaries
It demonstrates that partitioning is not about improvement of query performance.
Therefore we demonstrate the bad behavior of partitioned tables when it comes to aggregations

# 07 - Partitioning - Maintenance
This order provides scripts that demonstrate how to add new partition groups and how to remove existing partitions from the table.
It is required that the dbo.orders table has been partitioned beforehand using the scripts.

# 08 - Database - Maintenance
Very large databases must be online very quick in case of Disaster Recovery.
This folder contains demo scripts for an implementation strategy for backup restore and other maintenance work

# 98 - Query Stress
json template for Workload test.

# 99 - Windows Admin Center
json template for Windows Admin Center for the demonstration of resource consumption.
