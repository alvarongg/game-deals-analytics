from datetime import timezone
import datetime



def time_now_utc():
    return datetime.datetime.now(timezone.utc)

def format_time_now_utc():
    dt = time_now_utc()
    return dt.strftime("%Y_%m_%d_%H_%M_%S")
    