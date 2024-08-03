
MERGE INTO leave_report AS target
USING #leave_report_temp AS source
ON target.userId = source.userId and target.leaveTypeId = source.leaveTypeId
WHEN MATCHED THEN
    UPDATE SET
        target.userId = source.userId,
        target.leaveTypeId = source.leaveTypeId,
        target.approved = source.approved,
        target.approvedMinutes = source.approvedMinutes,
        target.unpaid = source.unpaid,
        target.unpaidMinutes = source.unpaidMinutes,
        target.deniedMinutes = source.deniedMinutes,
        target.deniedDays = source.deniedDays,
        target.ptoCost = source.ptoCost,
        target.pending = source.pending,
        target.pendingMinutes = source.pendingMinutes,
        target.remaining = source.remaining,
        target.ETLUpdatedAt = GETDATE()
WHEN NOT MATCHED THEN
    INSERT (
        userId,
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
    VALUES (
        source.userId,
        source.leaveTypeId,
        source.approved,
        source.approvedMinutes,
        source.unpaid,
        source.unpaidMinutes,
        source.deniedMinutes,
        source.deniedDays,
        source.ptoCost,
        source.pending,
        source.pendingMinutes,
        source.remaining,
        GETDATE()
    );

