#This is the make-a-list-of-consecutive-dates function
from datetime import date, datetime, timedelta
def datetime_range(start, end, delta):
    current = start
    if not isinstance(delta, timedelta):
        delta = timedelta(**delta)
    while current < end:
        yield current
        current += delta
		
		

  
def xls_cleaner(file_2_read, datetime_name = ["Date and Time", "Timestamp",  "TIMESTAMP"] ):

    data_xls = pd.read_excel(file_2_read,  header=None, index_col=None)                        # read  xlsx initially with no cols or index
    data_xls_trimmed = data_xls.apply(lambda x: x.str.strip() if x.dtype == "object" else x)   # remove whitespace from all strigs
    Col_true = data_xls_trimmed[data_xls_trimmed.isin(datetime_name)]                          # find the cell that has the start of the header   Note the datetime col has to always be ahead of data columns
    Col_true = Col_true.dropna(how='all')                                                      # retain and select the index and col numbers of the cell with the start of the header
    finally_frame = Col_true.dropna(axis = 1, how='all')
    rows_to_skip = finally_frame.index[0]
    index_col_number = finally_frame.columns[0]

    data = pd.read_excel(file_2_read,  skiprows=rows_to_skip,  index_col=index_col_number)  # read  xlsx to memory

    return data