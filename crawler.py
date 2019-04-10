import requests
import re
from bs4 import BeautifulSoup
import db
import threading
import os
import time
import utils
import config


def get_new_tx():
    print("starting to get new raw transaction ...")
    raw_table = db.Tx(config.db_name, config.raw_tx_table_name)
    block_table = db.Tx(config.db_name, config.block_tx_table_name)
    while True:
        try:
            r = requests.get("https://www.blockchain.com/btc/unconfirmed-transactions")
            soup = BeautifulSoup(r.content, features='lxml')
            for tx in soup.find_all(href=re.compile("/btc/tx*")):
                get_new_tx_info(tx.string, raw_table, block_table)
        except Exception as e:
            print(e)


def get_new_tx_info(hash_str, raw_table, block_table):
    url = 'https://www.blockchain.com/btc/tx/' + hash_str
    r = requests.get(url)
    soup = BeautifulSoup(r.content, features='lxml')
    button = soup.find(id=re.compile("tx-*"))
    try:
        is_confirmed = button.find_all('button')[0].string != 'Unconfirmed Transaction!'

        row = soup.find_all("tr")
        size = row[3].find_all("td")[1].string.split("(")[0]
        # print('size = ' + size)

        receive_time = row[5].find_all("td")[1].string.lstrip()
        # print(receive_time)

        receive_time = utils.date_to_timestamp(receive_time)

        total_input_index = 8

        block_time = 0

        if is_confirmed:
            block_time = row[6].find_all("td")[1].text.split('(')[1].lstrip().split(' +')[0]
            block_time = utils.date_to_timestamp(block_time)
            # print(blockinfo)
            total_input_index = 10

        if row.__len__() == 16 or row.__len__() == 18:
            total_input_index = total_input_index + 1

        total_input = row[total_input_index].find_all("td")[1].find("span").string.split(' ')[0]
        total_input = utils.format_value(total_input)
        # print(total_input)

        total_output = row[total_input_index + 1].find_all("td")[1].find("span").string.split(' ')[0]
        total_output = utils.format_value(total_output)
        # print(total_output)

        fees = row[total_input_index + 2].find_all("td")[1].find("span").string.split(' ')[0]
        fees = utils.format_value(fees)
        # print(fees)

        feerate = row[total_input_index + 3].find_all("td")[1].string.lstrip().rstrip().split(' ')[0]
        feerate = utils.format_value(feerate)
        # print(feerate)

        tx = dict()
        tx['hash'] = hash_str
        tx['size'] = size
        tx['receive_time'] = float(receive_time)
        tx['block_time'] = float(block_time)
        tx['total_input'] = float(total_input)
        tx['total_output'] = float(total_output)
        tx['fees'] = float(fees)
        tx['fee_rate'] = float(feerate)

        if is_confirmed:
            block_table.add_block_tx(tx)
        else:
            raw_table.add_raw_tx(tx)

    except Exception as e:
        print(str(e))
        print(hash_str)
        raw_table.delete_tx(hash_str)
        block_table.delete_tx(hash_str)


def update_tx(tx, raw_table, delta_table):
    url = 'https://www.blockchain.com/btc/tx/' + tx[0]
    r = requests.get(url)
    soup = BeautifulSoup(r.content, features='lxml')
    button = soup.find(id=re.compile("tx-*"))
    try:
        if button.find_all('button')[0].string != 'Unconfirmed Transaction!':
            raw_table.delete_tx(tx[0])
            row = soup.find_all("tr")
            index = 6
            if row.__len__() == 18:
                index = 7
            block_time = row[index].find_all("td")[1].text.split('(')[1].lstrip().split(' +')[0]
            block_time = utils.date_to_timestamp(block_time)

            tx_ = dict()
            tx_['hash'] = tx[0]
            tx_['size'] = tx[1]
            tx_['receive_time'] = tx[6]
            tx_['block_time'] = block_time
            tx_['total_input'] = tx[2]
            tx_['total_output'] = tx[3]
            tx_['fees'] = tx[4]
            tx_['fee_rate'] = tx[5]
            delta_table.add_block_tx(tx_)

    except Exception as e:
        print(str(e))
        print(tx[0])
        raw_table.delete_tx(tx[0])
        delta_table.delete_tx(tx[0])


def update_tx_info(raw_tx_list):
    print("starting to update transaction information...")
    raw_table = db.Tx(config.db_name, config.raw_tx_table_name)
    delta_table = db.Tx(config.db_name, config.delta_tx_table_name)
    i = 0
    for tx in raw_tx_list:
        update_tx(tx, raw_table, delta_table)
        # print(str(i) + " in " + str(len(raw_tx_list)))
        # i = i + 1


def assign_update_work():
    raw_table = db.Tx(config.db_name, config.raw_tx_table_name)
    delta_table = db.Tx(config.db_name, config.delta_tx_table_name)
    block_table = db.Tx(config.db_name, config.block_tx_table_name)
    thread_size = config.update_tx_thread_size
    while True:
        all_tx = raw_table.get_all_tx()
        print("current raw_tx size = " + str(raw_table.get_total_size()))
        print("current block_tx_size = " + str(block_table.get_total_size()))
        print("current delta_tx size = " + str(delta_table.get_total_size()))

        if len(all_tx) < 500:
            print("too few raw tx, stop updating raw tx")
            time.sleep(30)
            continue
        if len(all_tx) < 7000:
            thread_size = min(2, config.total_thead_size)
        thread_pool = []
        tx_size = len(all_tx) // thread_size
        for i in range(thread_size):
            if i == thread_size - 1:
                tx_list = all_tx[i * tx_size:len(all_tx) - 1]
            else:
                tx_list = all_tx[i * tx_size:(i + 1) * tx_size]
            thread_pool.append(threading.Thread(target=update_tx_info, args=[tx_list]))
            thread_pool[i].start()

        for j in range(thread_size):
            thread_pool[j].join()


def dump_delta_db():
    delta_table = db.Tx(config.db_name, config.delta_tx_table_name)
    block_table = db.Tx(config.db_name, config.block_tx_table_name)
    while True:
        time.sleep(config.delta_tx_dump_interval)
        if not os.path.exists(config.dump_tx_dir):
            os.mkdir(config.dump_tx_dir)
        file_name = utils.timestamp_to_date(time.time()) + "_delta_.csv"
        block_table.get_all_tx_from(delta_table.tablename)

        delta_tx_list = delta_table.get_all_tx()
        utils.dump_csv_file(delta_tx_list, config.dump_tx_dir + file_name)
        print("dump file " + file_name)
        for tx in delta_tx_list:
            delta_table.delete_tx(tx[0])


def dump_raw_db():
    raw_table = db.Tx(config.db_name, config.raw_tx_table_name)
    while True:
        time.sleep(config.raw_tx_dump_interval)
        if not os.path.exists(config.dump_tx_dir):
            os.mkdir(config.dump_tx_dir)
        file_name = utils.timestamp_to_date(time.time()) + "_raw_.csv"
        utils.dump_csv_file(raw_table.get_all_tx(), config.dump_tx_dir + file_name)
        print("dump file " + file_name)


def main():
    if not os.path.exists(config.db_name):
        db.create_db(config.db_name)
        db.create_table(config.db_name, config.raw_tx_table_name)
        db.create_table(config.db_name, config.delta_tx_table_name)
        db.create_table(config.db_name, config.block_tx_table_name)

    time.sleep(3)

    dump_db_thread = threading.Thread(target=dump_db, args=[])
    dump_db_thread.start()

    get_raw_thread = []
    for i in range(config.get_raw_tx_thread_size):
        get_raw_thread.append(threading.Thread(target=get_new_tx, args=[]))
        get_raw_thread[i].start()

    if config.generate_snapshot:
        dump_raw_thread = threading.Thread(target=dump_raw_db, args=[])
        dump_raw_thread.start()

        dump_delta_thread = threading.Thread(target=dump_delta_db, args=[])
        dump_delta_thread.start()

    assign_update_work()

    if config.generate_snapshot:
        dump_raw_thread.join()
        dump_delta_thread.join()

    for i in range(config.get_raw_tx_thread_size):
        get_raw_thread[i].join()

    dump_db_thread.join()


def dump_db():
    while True:
        os.system("rm " + config.db_copy_name)
        cmd = "cp " + config.db_name + " " + config.db_copy_name
        os.system(cmd)
        time.sleep(config.db_dump_time)


if __name__ == '__main__':
    # print(cpu_count())
    main()
