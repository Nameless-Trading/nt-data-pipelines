from calendar_flow import calendar_backfill_flow

if __name__ == "__main__":
    calendar_backfill_flow.serve(cron="* * * * *")
