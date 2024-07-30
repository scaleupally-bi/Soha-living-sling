
		MERGE INTO user_groups AS target
		USING #user_groups_temp AS source
		ON target.userId = source.userId and target.groupId = source.groupId
		WHEN MATCHED THEN
			UPDATE SET
				target.groupId = source.groupId,
				target.ETLUpdatedAt = GETDATE()
		WHEN NOT MATCHED THEN
			INSERT (
				userId,
				groupId,
				ETLCreatedAt
			)
			VALUES (
				source.userId,
				source.groupId,
				GETDATE()
			);

