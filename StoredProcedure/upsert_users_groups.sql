

	TRUNCATE TABLE user_groups;

	INSERT INTO user_groups(
		userId,
		groupId,
		ETLCreatedAt
	)
	SELECT 
		userId,
		groupId,
		GETDATE()
	FROM #user_groups_temp;

