from datetime import timezone
from datetime import datetime
import datetime

# datetime.strptime(datetime_str, '%Y_%m_%d_%H_%M_%S')


def time_now_utc():
    return datetime.datetime.now(timezone.utc)


def format_time_now_utc():
    dt = time_now_utc()
    return dt.strftime("%Y_%m_%d_%H_%M_%S")


def ambiguous_column_tagger(df):
    """ambiguous_column_tagger

    This function helps label ambiguous columns in pyspark dataframe.
    Receives a dataframe with anbiguous columns and adds an index to each repeating column starting with _0

    Args:
        df (Spark Data Frame): Dataframe with ambiguous columns

    Returns:
        df (Spark Data Frame)  : Dataframe without ambiguous columns
    """
    lst = []
    df_cols = df.columns

    for i in df_cols:
        if df_cols.count(i) == 2:
            ind = df_cols.index(i)
            lst.append(ind)

    lst1 = list(set(lst))
    for i in lst1:
        df_cols[i] = df_cols[i] + "_0"

    df = df.toDF(*df_cols)

    return df
