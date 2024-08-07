    DECLARE @MinDaily DATE, @MaxDaily DATE;

    -- Get the minimum and maximum Daily values from #ReportTemp
    SELECT @MinDaily = MIN(date), @MaxDaily = MAX(date)
    FROM #leave_report_temp;

    -- Delete records from Report where Daily is between @MinDaily and @MaxDaily
    DELETE FROM leave_report
    WHERE date BETWEEN @MinDaily AND @MaxDaily;

    INSERT INTO leave_report(
        userId,
        date,
        leaveTypeId,
        approved,
        approvedMinutes,
        unpaid,
        unpaidMinutes,
        deniedMinutes,
        deniedDays,
        ptoCost,
        pending,
        pendingMinutes,
        remaining,
        ETLCreatedAt
    )
    
    select 
        userId,
        date,
        leaveTypeId,
        approved,
        approvedMinutes,
        unpaid,
        unpaidMinutes,
        deniedMinutes,
        deniedDays,
        ptoCost,
        pending,
        pendingMinutes,
        remaining,
        GETDATE()
    from #leave_report_temp;

