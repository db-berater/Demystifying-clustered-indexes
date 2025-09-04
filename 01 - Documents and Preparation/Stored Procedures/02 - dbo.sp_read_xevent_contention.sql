USE ERP_Demo;
GO

CREATE OR ALTER PROCEDURE dbo.sp_read_xevent_contention
	@xevent_name		NVARCHAR(128)
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	DECLARE	@Target_Data XML =
	(
		SELECT	CAST(xet.target_data AS XML) AS targetdata
		FROM	sys.dm_xe_session_targets AS xet
				INNER JOIN sys.dm_xe_sessions AS xes
				ON (xes.address = xet.event_session_address)
		WHERE	xes.name = @xevent_name
				  AND xet.target_name = 'ring_buffer'
	);

	SELECT	f.*, mv.map_value
	FROM 
	(
	  SELECT	CONVERT
				(
					DATETIME2,
					SWITCHOFFSET
					(
						CAST
						(
							x.event_data.value ('(@timestamp)[1]', N'DATETIME2')
							AS DATETIMEOFFSET
						),
						DATENAME(TZOFFSET, SYSDATETIMEOFFSET())
					)
				)																				AS	[timestamp],
				x.event_data.value ('(data[@name = "mode"]/text)[1]', 'VARCHAR(10)')			AS	[mode],
				x.event_data.value('(data[@name = "duration"]/value)[1]', 'INT')				AS	[duration],
				x.event_data.value('(data[@name = "has_waiters"]/value)[1]', 'VARCHAR(10)')		AS	[has waiters],
				x.event_data.value('(data[@name = "page_id"]/value)[1]', 'BIGINT')				AS	[page_id],
				x.event_data.value('(data[@name = "page_type_id"]/value)[1]', 'VARCHAR(10)')	AS	[page_type_id]
	  FROM @Target_Data.nodes('//RingBufferTarget/event') AS x (event_data)
	)	AS f INNER JOIN sys.dm_xe_map_values AS mv
		ON
		(
			f.page_type_id = mv.map_key
			AND mv.name = N'page_type'
		)
	ORDER BY
		[f].[timestamp]
END
GO