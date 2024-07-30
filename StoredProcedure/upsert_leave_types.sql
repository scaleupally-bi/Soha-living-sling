
MERGE INTO leave_types AS target
USING #leave_types_temp AS source
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET
        target.id = source.id,
        target.type = source.type,
        target.name = source.name,
        target.paid = source.paid,
        target.enabled = source.enabled,
        target.cap = source.cap,
        target.available = source.available,
        target.ETLUpdatedAt = GETDATE()
WHEN NOT MATCHED THEN
    INSERT (
        id,
        type,
        name,
        paid,
        enabled,
        cap,
        available,
        ETLCreatedAt
    )
    VALUES (
        source.id,
        source.type,
        source.name,
        source.paid,
        source.enabled,
        source.cap,
        source.available,
        GETDATE()
    );

