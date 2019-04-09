import time


def format_value(value):
    if value.find(',') > -1:
        value_list = value.split(',')
        value = ''
        for s in value_list:
            value = value + s
    return value


def date_to_timestamp(date):
    st = time.strptime(date, '%Y-%m-%d %H:%M:%S')
    return time.mktime(st)


def timestamp_to_date(st):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(st))
