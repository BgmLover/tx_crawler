import requests
from bs4 import BeautifulSoup
import re
import utils
import db
import config
import os
import threading


def get_block_tx(block_hash, table_name, db_lock):
    url = "https://www.blockchain.com/btc/block/" + block_hash
    r = requests.get(url)
    soup = BeautifulSoup(r.content, features='lxml')
    chain_info = soup.find_all(href=re.compile("/btc/block-height/*"))[0] \
        .parent.find(color=re.compile('"*')).text.rstrip()
    tx_list = []
    table = db.Tx(config.db_name, table_name)

    if chain_info == '(Main Chain)':
        i = 0
        for tx in soup.find_all(href=re.compile("/btc/tx*")):
            i = i + 1
            if i == 1:
                continue
            tx_info = get_block_tx_info(tx.string)
            tx_list.append(tx_info)
            if i % 200 == 0:
                print("get " + str(i) + " tx in " + block_hash)
        db_lock.acquire()
        for tx_info in tx_list:
            try:
                table.add_block_tx(tx_info)
            except Exception as e:
                print(e, tx_info)
        db_lock.release()
    print("finish get block " + block_hash)


def get_block_tx_info(hash_str):
    url = 'https://www.blockchain.com/btc/tx/' + hash_str
    r = requests.get(url)
    soup = BeautifulSoup(r.content, features='lxml')
    try:

        row = soup.find_all("tr")
        size = row[3].find_all("td")[1].string.split("(")[0]
        # print('size = ' + size)

        weight = row[4].find_all("td")[1].string
        # print(weight)

        receive_time = row[5].find_all("td")[1].string.lstrip()
        # print(receive_time)

        receive_time = utils.date_to_timestamp(receive_time)

        lock_time = -1

        block_time_index = 6
        total_input_index = 10

        if row.__len__() == 18:
            lock_time = row[6].find_all('td')[1].string.split('\n')[2].lstrip()
            block_time_index = block_time_index + 1
            total_input_index = total_input_index + 1

        block_time = row[block_time_index].find_all("td")[1].text.split('(')[1].lstrip().split(' +')[0]
        block_time = utils.date_to_timestamp(block_time)

        confirmations = row[block_time_index + 1].find_all("td")[1].string

        total_input = row[total_input_index].find_all("td")[1].find("span").string.split(' ')[0]
        total_input = utils.format_value(total_input)
        # print(total_input)

        total_output = row[total_input_index + 1].find_all("td")[1].find("span").string.split(' ')[0]
        total_output = utils.format_value(total_output)
        # print(total_output)

        fees = row[total_input_index + 2].find_all("td")[1].find("span").string.split(' ')[0]
        fees = utils.format_value(fees)
        # print(fees)

        fee_rate = row[total_input_index + 3].find_all("td")[1].string.lstrip().rstrip().split(' ')[0]
        fee_rate = utils.format_value(fee_rate)
        # print(fee_rate)

        fee_wrate = row[total_input_index + 4].find_all("td")[1].string.lstrip().rstrip().split(' ')[0]
        fee_wrate = utils.format_value(fee_wrate)

        transacted = row[total_input_index + 5].find_all("td")[1].text.lstrip().rstrip().split(' ')[0]
        transacted = utils.format_value(transacted)

        tx = dict()
        tx['hash'] = hash_str
        tx['size'] = int(size)
        tx['weight'] = int(weight)
        tx['lock_time'] = int(lock_time)
        tx['confirmations'] = int(confirmations)
        tx['receive_time'] = float(receive_time)
        tx['block_time'] = float(block_time)
        tx['total_input'] = float(total_input)
        tx['total_output'] = float(total_output)
        tx['fees'] = float(fees)
        tx['fee_rate'] = float(fee_rate)
        tx['fee_wrate'] = float(fee_wrate)
        tx['transacted'] = float(transacted)

        return tx
    except Exception as e:
        print(hash_str)
        print(str(e))


def assign_work(table_name):
    height = config.block_init_height
    thread_pool = []
    db_lock = threading.Lock()
    while True:
        url = "https://www.blockchain.com/btc/block-height/" + str(height)
        r = requests.get(url)
        soup = BeautifulSoup(r.content, features='lxml')
        tr = soup.find_all("tr")
        block_hash = tr[2].find_all("td")[1].text
        block_time = tr[5].find_all("td")[1].string
        print(str(height) + "  " + block_time)
        height = height - 1
        if utils.date_to_timestamp(block_time) > utils.date_to_timestamp(config.block_stop_time):
            if thread_pool.__len__() < config.thread_pool_max_size:
                thread = threading.Thread(target=get_block_tx, args=[block_hash, table_name, db_lock])
                thread.start()
                thread_pool.append(thread)
            else:
                for thread in thread_pool:
                    thread.join()
                    thread_pool.remove(thread)
        else:
            print("current block time = " + block_time + ", stop block time = " + config.block_stop_time)
            return


def main():
    if not os.path.exists(config.db_name):
        db.create_db(config.db_name)
        db.create_table(config.db_name, config.block_tx_table_name)

    assign_work(config.block_tx_table_name)


if __name__ == '__main__':
    main()
