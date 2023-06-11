from datetime import timezone
from datetime import datetime
import datetime

#datetime.strptime(datetime_str, '%Y_%m_%d_%H_%M_%S')

def time_now_utc():
    return datetime.datetime.now(timezone.utc)

def format_time_now_utc():
    dt = time_now_utc()
    return dt.strftime("%Y_%m_%d_%H_%M_%S")
    