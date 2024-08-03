
from views.users import *
from views.users_groups import *
from views.labor_report import *
from views.leave_report import *
from views.payroll_report import *
from views.noshows import *
from views.groups import *
from views.leave_types import *


def run():
    
    users = Users()
    users.extract_users()

    users_groups = UsersGroups()
    users_groups.extract_users_groups()

    # labor_report = LaborReportClass()
    # labor_report.extract_labor_report_daily()
    # labor_report.extract_labor_report_one_time()

    # leave_report = LeaveReportClass()
    # leave_report.extract_leave_report()

    payroll_report = PayrollReportClass()
    payroll_report.extract_payroll_report_daily()
    # payroll_report.extract_payroll_report_one_time()

    noshows = NoshowsClass()
    noshows.extract_noshows()

    groups = GroupsClass()
    groups.extract_groups()

    leave_types = LeaveTypesClass()
    leave_types.extract_leave_types()

if __name__ == '__main__':
    run()