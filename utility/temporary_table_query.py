users_temporary_table_query = '''
create table #users_temp(
	systemGeneratedId int identity(1,1),
	id int not null,
	type nvarchar(255) null,
	origin nvarchar(255) null,
	name nvarchar(255) null,
	legalName nvarchar(255) null,
	preferredName nvarchar(255) null,
	lastname nvarchar(255) null,
	avatar nvarchar(4000) null,
	email nvarchar(255) null,
	timezone nvarchar(255) null,
	hoursCap float null,
	active bit null,
	deactivatedAt datetime null,
	timeclockEnabled bit null,
	phone nvarchar(255) null,
	hireDate date null,
	birthdayDate date null,
	countryCode nvarchar(10) null,
	countryISOCode nvarchar(10) null,
	role nvarchar(255) null,
	employeeId int null,
	address nvarchar(255) null,
	additionalEmail nvarchar(255) null,
	emergencyContact nvarchar(255) null,
	emergencyContactRelationship nvarchar(255) null,
	emergencyContactPhone nvarchar(255) null,
	emergencyContactCountryCode nvarchar(10) null,
	emergencyContactCountryISOCode nvarchar(10) null,
	status nvarchar(255) null,
	pending nvarchar(255) null,
	defaultAvatar nvarchar(255) null,
	deleted bit null,
	createdAt datetime null,
	joinedAt datetime null,
	hidden bit null
);
'''

users_groups_temporary_table_query = '''
create table #user_groups_temp(
	systemGeneratedId int identity(1,1) not null,
	userId int not null,
	groupId int null
);

'''

labor_report_temporary_table_query = '''
CREATE TABLE #labor_report_temp(
	[systemGeneratedId] [int] IDENTITY(1,1) NOT NULL,
	[userId] [int] NULL,
	[date] [date] NULL,
	[locationId] [int] NULL,
	[positionId] [int] NULL,
	[isSalary] [bit] NULL,
	[estimatedWages] [float] NULL,
	[hours] [float] NULL,
	[overtimeHours] [float] NULL,
	[overtimeCost] [float] NULL,
	[overtimeDayHours] [float] NULL,
	[overtimeDayCost] [float] NULL,
	[overtimeDoubleDailyHours] [float] NULL,
	[overtimeDoubleDailyCost] [float] NULL,
	[shiftsCount] [int] NULL,
	[scheduledHolidayHours] [float] NULL,
	[scheduledHolidayOvertimeHours] [float] NULL,
	[scheduledHolidayRegularCost] [float] NULL,
	[scheduledHolidayOvertimeCost] [float] NULL,
	[scheduledSpreadOfHoursCost] [float] NULL,
	[ptoHours] [float] NULL,
	[ptoCost] [float] NULL,
	[actualWages] [float] NULL,
	[actualHours] [float] NULL,
	[actualShiftsCount] [float] NULL,
	[actualOvertimeHours] [float] NULL,
	[actualOvertimeCost] [float] NULL,
	[actualOvertimeDayHours] [float] NULL,
	[actualOvertimeDayCost] [float] NULL,
	[actualOvertimeDoubleDailyHours] [float] NULL,
	[actualOvertimeDoubleDailyCost] [float] NULL,
	[actualHolidayHours] [float] NULL,
	[actualHolidayOvertimeHours] [float] NULL,
	[actualHolidayRegularCost] [float] NULL,
	[actualHolidayOvertimeCost] [float] NULL,
	[actualSpreadOfHoursCost] [float] NULL,
	[estimatedWagesStr] [float] NULL,
	[hoursStr] [float] NULL,
	[overtimeHoursStr] [float] NULL,
	[overtimeCostStr] [float] NULL,
	[overtimeDayHoursStr] [float] NULL,
	[overtimeDayCostStr] [float] NULL,
	[overtimeDoubleDailyHoursStr] [float] NULL,
	[overtimeDoubleDailyCostStr] [float] NULL,
	[scheduledHolidayHoursStr] [float] NULL,
	[scheduledHolidayOvertimeHoursStr] [float] NULL,
	[scheduledHolidayRegularCostStr] [float] NULL,
	[scheduledHolidayOvertimeCostStr] [float] NULL,
	[scheduledSpreadOfHoursCostStr] [float] NULL,
	[ptoHoursStr] [float] NULL,
	[ptoCostStr] [float] NULL,
	[actualWagesStr] [float] NULL,
	[actualHoursStr] [float] NULL,
	[actualOvertimeHoursStr] [float] NULL,
	[actualOvertimeCostStr] [float] NULL,
	[actualOvertimeDayHoursStr] [float] NULL,
	[actualOvertimeDayCostStr] [float] NULL,
	[actualOvertimeDoubleDailyHoursStr] [float] NULL,
	[actualOvertimeDoubleDailyCostStr] [float] NULL,
	[actualHolidayHoursStr] [float] NULL,
	[actualHolidayOvertimeHoursStr] [float] NULL,
	[actualHolidayRegularCostStr] [float] NULL,
	[actualHolidayOvertimeCostStr] [float] NULL,
	[actualSpreadOfHoursCostStr] [float] NULL,
	[ETLCreatedAt] [datetime] NULL,
	[ETLUpdatedAt] [datetime] NULL
);
'''

leave_report_temporary_table_query = '''

CREATE TABLE #leave_report_temp(
	[systemGeneratedId] [int] IDENTITY(1,1) NOT NULL,
	[userId] [int] NOT NULL,
    [date] [date] NULL,
	[leaveTypeId] [int] NOT NULL,
	[approved] [int] NULL,
	[approvedMinutes] [int] NULL,
	[unpaid] [int] NULL,
	[unpaidMinutes] [int] NULL,
	[deniedMinutes] [float] NULL,
	[deniedDays] [float] NULL,
	[ptoCost] [float] NULL,
	[pending] [int] NULL,
	[pendingMinutes] [int] NULL,
	[remaining] [float] NULL
);
'''

payroll_report_temporary_table_query = '''

CREATE TABLE #payroll_report_temp(
	[systemGeneratedId] [int] IDENTITY(1,1) NOT NULL,
	[userId] [int] NOT NULL,
    [date] [date] NULL,
	[locationId] [int] NULL,
	[positionId] [int] NULL,
	[actualHours] [float] NULL,
	[actualHoursStr] [float] NULL,
	[status] [nvarchar](255) NULL,
	[actualHolidayHours] [float] NULL,
	[actualHolidayHoursStr] [float] NULL,
	[actualHolidayCost] [float] NULL,
	[actualHolidayCostStr] [float] NULL,
	[actualSpreadOfHoursCost] [float] NULL,
	[actualSpreadOfHoursCostStr] [float] NULL,
	[isSalary] [bit] NULL,
	[isOnSalary] [bit] NULL,
	[ptoHours] [float] NULL,
	[ptoHoursStr] [float] NULL,
	[ptoCost] [float] NULL,
	[ptoCostStr] [float] NULL,
	[premiumPayCost] [float] NULL,
	[actualOvertimeHours] [float] NULL,
	[actualOvertimeHoursStr] [float] NULL,
	[actualOvertimeDoubleDailyHours] [float] NULL,
	[actualOvertimeDoubleDailyHoursStr] [float] NULL,
	[actualOvertimeCost] [float] NULL,
	[actualOvertimeCostStr] [float] NULL,
	[actualHolidayOvertimeHours] [float] NULL,
	[actualHolidayOvertimeHoursStr] [float] NULL,
	[actualHolidayOvertimeCost] [float] NULL,
	[actualHolidayOvertimeCostStr] [float] NULL,
	[shiftId] [nvarchar](255) NULL,
	[shiftDuration] [float] NULL,
	[shiftActualDuration] [float] NULL,
	[shiftBreakDuration] [float] NULL,
	[shiftActualBreakDuration] [float] NULL,
	[wage] [float] NULL,
	[wageStr] [float] NULL,
	[actualWages] [float] NULL,
	[actualWagesStr] [float] NULL
); 

'''


noshows_temporary_table_query = '''

CREATE TABLE #noshows_temp(
	[systemGeneratedId] [int] IDENTITY(1,1) NOT NULL,
	[userId] [int] NOT NULL,
	[locationId] [int] NOT NULL,
	[positionId] [int] NOT NULL,
	[scheduled] [int] NULL,
	[scheduledMinutes] [int] NULL,
	[lateCount] [int] NULL,
	[actual] [int] NULL,
	[actualMinutes] [int] NULL,
	[sickCallout] [int] NULL,
	[noShow] [int] NULL
);

'''

groups_temporary_table_query = '''

CREATE TABLE #groups_temp(
	[systemGeneratedId] [int] IDENTITY(1,1) NOT NULL,
	[id] [int] NOT NULL,
	[type] [nvarchar](255) NULL,
	[origin] [nvarchar](255) NULL,
	[name] [nvarchar](255) NULL,
	[externalId] [nvarchar](255) NULL,
	[archivedAt] [datetime] NULL,
	[memberCount] [int] NULL
);

'''

leave_types_temporary_table_query = '''

CREATE TABLE #leave_types_temp(
	[systemGeneratedId] [int] IDENTITY(1,1) NOT NULL,
	[id] [int] NOT NULL,
	[type] [nvarchar](255) NOT NULL,
	[name] [nvarchar](255) NOT NULL,
	[paid] [bit] NULL,
	[enabled] [bit] NULL,
	[cap] [nvarchar](255) NULL,
	[available] [bit] NULL
);

'''