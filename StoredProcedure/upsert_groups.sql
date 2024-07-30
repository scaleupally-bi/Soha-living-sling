MERGE INTO groups AS target
USING #groups_temp AS source
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET
        target.id = source.id,
        target.type = source.type,
        target.origin = source.origin,
        target.name = source.name,
        target.externalId = source.externalId,
        target.archivedAt = source.archivedAt,
        target.memberCount = source.memberCount,
        target.ETLUpdatedAt = GETDATE()
WHEN NOT MATCHED THEN
    INSERT (
        id,
        type,
        origin,
        name,
        externalId,
        archivedAt,
        memberCount,
        ETLCreatedAt
    )
    VALUES (
        source.id,
        source.type,
        source.origin,
        source.name,
        source.externalId,
        source.archivedAt,
        source.memberCount,
        GETDATE()
    );

