USE ERP_Demo;
GO

CREATE OR ALTER PROCEDURE dbo.sp_read_xevent_locks
	@xevent_name		NVARCHAR(128),
	@filter_condition	NVARCHAR(1024) = NULL
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	RAISERROR ('Catching the data from the ring_buffer for extended event [%s]', 0, 1, @xevent_name) WITH NOWAIT;
	SELECT	CAST(target_data AS XML) AS target_data
	INTO	#xe_data
	FROM	sys.dm_xe_session_targets AS t
			INNER JOIN sys.dm_xe_sessions AS s
			ON (t.event_session_address = s.address)
	WHERE	s.name = @xevent_name
			AND t.target_name = N'ring_buffer';

	RAISERROR ('Analyzing the data from the ring buffer', 0, 1) WITH NOWAIT;

	SELECT	x.event_data.value ('(action[@name="attach_activity_id"]/value)[1]', 'VARCHAR(40)')			AS	activity_id,
			x.event_data.value ('(@timestamp)[1]', N'DATETIME')											AS	[timestamp],
			x.event_data.value('(@name)[1]', 'VARCHAR(25)')												AS	event_name,
			x.event_data.value ('(data[@name="batch_text"]/value)[1]', 'VARCHAR(MAX)')					AS	batch_text,
			x.event_data.value('(data[@name="resource_type"]/text)[1]', 'VARCHAR(25)')					AS	resource_type,
			x.event_data.value('(data[@name="mode"]/text)[1]', 'VARCHAR(10)')							AS	lock_mode,
			x.event_data.value('(data[@name="resource_0"]/value)[1]', 'NVARCHAR(25)')					AS	resource_0,
			x.event_data.value('(data[@name="resource_1"]/value)[1]', 'NVARCHAR(25)')					AS	resource_1,
			x.event_data.value('(data[@name="resource_2"]/value)[1]', 'NVARCHAR(25)')					AS	resource_2,
			OBJECT_NAME
			(
				CASE WHEN ISNULL(x.event_data.value('(data[@name="object_id"]/value)[1]', 'INT'), 0) = 0
					 THEN i.object_id
					 ELSE x.event_data.value('(data[@name="object_id"]/value)[1]', 'INT')
				END
			)																					AS	object_name,
			x.event_data.value('(data[@name="associated_object_id"]/value)[1]', 'NVARCHAR(25)')	AS	associated_object_id,
			i.index_id,
			i.name																				AS	index_name
	INTO	#temp_result
	FROM	#xe_data AS txe
			CROSS APPLY txe.target_data.nodes('//RingBufferTarget/event') AS x (event_data)
			LEFT JOIN sys.partitions AS p
			ON
			(
				TRY_CAST(x.event_data.value('(data[@name="associated_object_id"]/value)[1]', 'NVARCHAR(25)') AS BIGINT) = p.hobt_id
			)
			LEFT JOIN sys.indexes AS i
			ON
			(
				p.object_id = i.object_id
				AND p.index_id = i.index_id
			)

	IF @filter_condition IS NOT NULL
	BEGIN
		DECLARE	@sql_stmt NVARCHAR(MAX) = N'SELECT	r.activity_id,
		r.[timestamp],
		r.event_name,
		r.batch_text,
		r.resource_type,
		CASE WHEN r.resource_type = N''PAGE''
				THEN dpi.page_type_desc
				ELSE NULL
		END			AS	page_type,
		r.lock_mode,
		r.resource_0,
		r.resource_1,
		r.resource_2,
		r.object_name,
		r.associated_object_id,
		r.index_id,
		r.index_name
FROM	#temp_result AS r
		OUTER APPLY sys.dm_db_page_info
					(
						DB_ID(),
						1,
						CASE WHEN r.resource_type = N''PAGE''
								THEN CAST(r.resource_0 AS BIGINT)
								ELSE 0
						END,
						N''DETAILED''
					) AS dpi
WHERE ' + @filter_condition + N' 
ORDER BY
		timestamp,
		TRY_CAST(SUBSTRING(r.activity_id, 38, 255) AS INT);';
		EXEC sp_executesql @sql_stmt;
	END
	ELSE
		SELECT	r.activity_id,
				r.[timestamp],
				r.event_name,
				r.batch_text,
				r.resource_type,
				CASE WHEN r.resource_type = N'PAGE'
					 THEN dpi.page_type_desc
					 ELSE NULL
				END			AS	page_type,
				r.lock_mode,
				r.resource_0,
				r.resource_1,
				r.resource_2,
				r.object_name,
				r.associated_object_id,
				r.index_id,
				r.index_name
		FROM	#temp_result AS r
				OUTER APPLY sys.dm_db_page_info
							(
								DB_ID(),
								1,
								CASE WHEN r.resource_type = N'PAGE'
									 THEN CAST(r.resource_0 AS BIGINT)
									 ELSE 0
								END,
								N'DETAILED'
							) AS dpi
		ORDER BY
				r.timestamp ASC,
				TRY_CAST(SUBSTRING(r.activity_id, 38, 255) AS INT);
END
GO