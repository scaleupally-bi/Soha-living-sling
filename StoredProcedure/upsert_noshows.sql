MERGE INTO noshows AS target
USING #noshows_temp AS source
ON target.userId = source.userId and target.date = source.date and target.locationId = source.locationId and target.positionId = source.positionId
WHEN MATCHED THEN
    UPDATE SET
        target.userId = source.userId,
        target.date = source.date,
        target.locationId = source.locationId,
        target.positionId = source.positionId,
        target.scheduled = source.scheduled,
        target.scheduledMinutes = source.scheduledMinutes,
        target.lateCount = source.lateCount,
        target.actual = source.actual,
        target.actualMinutes = source.actualMinutes,
        target.sickCallout = source.sickCallout,
        target.noShow = source.noShow,
        target.ETLUpdatedAt = GETDATE()
WHEN NOT MATCHED THEN
    INSERT (
        userId,
        date,
        locationId,
        positionId,
        scheduled,
        scheduledMinutes,
        lateCount,
        actual,
        actualMinutes,
        sickCallout,
        noShow,
        ETLCreatedAt
    )
    VALUES (
        source.userId,
        source.date,
        source.locationId,
        source.positionId,
        source.scheduled,
        source.scheduledMinutes,
        source.lateCount,
        source.actual,
        source.actualMinutes,
        source.sickCallout,
        source.noShow,
        GETDATE()
    );

