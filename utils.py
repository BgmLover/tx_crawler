import time
import csv


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


def dump_csv_file(tx_info_list, file_name):
    with open(file_name, 'a+') as file:
        filed_names = ['hash', 'size', 'receive_time', 'block_time', 'total_input', 'total_output', 'fees', 'fee_rate']
        writer = csv.DictWriter(file, fieldnames=filed_names)
        writer.writeheader()
        for tx_info in tx_info_list:
            writer.writerow(
                {
                    'hash': tx_info[0],
                    'size': tx_info[1],
                    'receive_time': tx_info[6],
                    'block_time': tx_info[7],
                    'total_input': tx_info[2],
                    'total_output': tx_info[3],
                    'fees': tx_info[4],
                    'fee_rate': tx_info[5]
                }
            )
