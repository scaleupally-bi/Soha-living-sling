    DECLARE @MinDaily DATE, @MaxDaily DATE;

    -- Get the minimum and maximum Daily values from #ReportTemp
    SELECT @MinDaily = MIN(date), @MaxDaily = MAX(date)
    FROM #noshows_temp;

    -- Delete records from Report where Daily is between @MinDaily and @MaxDaily
    DELETE FROM noshows
    WHERE date BETWEEN @MinDaily AND @MaxDaily;

    INSERT INTO noshows(
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
    select
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
        GETDATE()
    from #noshows_temp;

