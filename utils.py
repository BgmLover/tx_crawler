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
        filed_names = ['hash', 'size', 'weight', 'lock_time', 'confirmations', 'receive_time', 'block_time',
                       'total_input', 'total_output', 'fees', 'fee_rate', 'fee_wrate', 'transacted']
        writer = csv.DictWriter(file, fieldnames=filed_names)
        writer.writeheader()
        for tx_info in tx_info_list:
            writer.writerow(
                {
                    'hash': tx_info[0],
                    'size': tx_info[1],
                    'weight': tx_info[2],
                    'lock_time': tx_info[3],
                    'confirmations': tx_info[4],
                    'receive_time': tx_info[11],
                    'block_time': tx_info[12],
                    'total_input': tx_info[5],
                    'total_output': tx_info[6],
                    'fees': tx_info[7],
                    'fee_rate': tx_info[8],
                    'fee_wrate': tx_info[9],
                    'transacted': tx_info[10],
                }
            )
