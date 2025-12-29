from calendar_flow import calendar_backfill_flow
from prefect.schedules import Cron

if __name__ == "__main__":
    calendar_backfill_flow.serve(schedule=Cron("0 2 * * *", timezone="America/Denver"))
