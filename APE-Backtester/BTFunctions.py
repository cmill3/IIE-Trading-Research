from datetime import datetime, timedelta, date

def end_date_trading_days_only(from_date, add_days):
    trading_days_to_add = add_days
    current_date = from_date
    while trading_days_to_add > 0:
        current_date += timedelta(days=1)
        weekday = current_date.weekday()
        if weekday >= 5: # sunday = 6
            continue
        trading_days_to_add -= 1
    print(current_date)

starting_date = date(2023,1,26)
n_days = 3
current_date = []

end_date_trading_days_only(starting_date, n_days)