import sqlite3
from threading import Lock


def create_db(db_name):
    conn = sqlite3.connect(db_name)
    conn.commit()
    conn.close()


def create_table(db_name, table_name):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    command = "CREATE TABLE " + table_name + '''
    (HASH TEXT PRIMARY KEY NOT NULL,
    SIZE INT,
    TOTAL_INPUT REAL,
    TOTAL_OUTPUT REAL,
    FEE REAL,
    FEE_RATE REAL,
    RECEIVED_TIME REAL,
    BLOCK_TIME REAL
    );
    '''
    c.execute(command)
    conn.commit()
    conn.close()


class Tx:
    def __init__(self, db_name, table_name):
        self.tablename = table_name
        self.conn = sqlite3.connect(db_name)

    def add_raw_tx(self, tx_info):
        if not self.is_exist(tx_info['hash']):
            c = self.conn.cursor()
            command = "INSERT INTO " + self.tablename + "(HASH,SIZE,TOTAL_INPUT,TOTAL_OUTPUT,FEE,FEE_RATE,RECEIVED_TIME) VALUES ('" + \
                      tx_info['hash'] + "'," + str(tx_info['size']) + "," + str(tx_info['total_input']) + "," + \
                      str(tx_info['total_output']) + "," + str(tx_info['fees']) + "," + str(tx_info['fee_rate']) + \
                      "," + str(tx_info['receive_time']) + ")"
            c.execute(command)
            self.conn.commit()
            # print("add new into " + self.tablename + " " + tx_info['hash'])

    def add_block_tx(self, tx_info):
        if not self.is_exist(tx_info['hash']):
            c = self.conn.cursor()
            command = "INSERT INTO " + self.tablename + "(HASH,SIZE,TOTAL_INPUT,TOTAL_OUTPUT,FEE,FEE_RATE,RECEIVED_TIME,BLOCK_TIME) VALUES ('" + \
                      tx_info['hash'] + "'," + str(tx_info['size']) + "," + str(tx_info['total_input']) + "," + \
                      str(tx_info['total_output']) + "," + str(tx_info['fees']) + "," + str(tx_info['fee_rate']) + \
                      "," + str(tx_info['receive_time']) + \
                      "," + str(tx_info['block_time']) + \
                      ")"
            c.execute(command)
            self.conn.commit()
            # print("add new into " + self.tablename + " " + tx_info['hash'])

    def get_all_tx_from(self, source_table):
        c = self.conn.cursor()
        command = "INSERT INTO " + self.tablename + " SELECT * FROM " + source_table
        c.execute(command)
        self.conn.commit()

    # def updateTx(self, tx_info):
    #     if self.is_exist(tx_info["hash"]):
    #         c = self.conn.cursor()
    #         command = " UPDATE TX" + \
    #                   " set SIZE = " + str(tx_info['size']) + "," + \
    #                   " TOTAL_INPUT = " + str(tx_info['total_input']) + "," + \
    #                   " TOTAL_OUTPUT = " + str(tx_info['total_output']) + "," + \
    #                   " FEE = " + str(tx_info["fees"]) + "," + \
    #                   " FEE_RATE = " + str(tx_info["fee_rate"]) + "," + \
    #                   " RECEIVED_TIME = " + '"' + str(tx_info["receive_time"]) + '"' + "," + \
    #                   " BLOCK_TIME = " + '"' + str(tx_info["block_time"]) + '"' + "  " + \
    #                   " where HASH = " + '"' + str(tx_info["hash"]) + '"'
    #         c.execute(command)
    #         self.conn.commit()
    #         print("update in " + self.tablename + "  " + tx_info["hash"])

    def is_exist(self, hash_str):
        c = self.conn.cursor()
        command = "SELECT HASH from " + self.tablename + "  where HASH=" + '"' + hash_str + '"'
        cursor = c.execute(command)
        data = cursor.fetchone()
        if data is None:
            return False
        else:
            return True

    def delete_tx(self, hash_str):
        if self.is_exist(hash_str):
            c = self.conn.cursor()
            command = "DELETE from " + self.tablename + " where HASH=" + '"' + hash_str + '"'
            c.execute(command)
            self.conn.commit()
            # print("delete from " + self.tablename + hash_str)

    def get_all_tx(self):
        c = self.conn.cursor()
        command = "SELECT * from " + self.tablename
        cursor = c.execute(command)
        return cursor.fetchall()

    def close(self):
        self.conn.close()

    def execute_sql(self, command):
        c = self.conn.cursor()
        cursor = c.execute(command)
        return cursor.fetchall()

    def get_total_size(self):
        c = self.conn.cursor()
        command = "SELECT COUNT(*) FROM " + self.tablename
        size = c.execute(command).fetchone()
        return size[0]


class DBCache:
    def __init__(self):
        self.lock = Lock()
        self.add_queue = dict()
        self.delete_queue = dict()

    def add_item(self, key, value, is_add_queue):
        self.lock.acquire()
        if is_add_queue:
            self.add_queue[key] = value
        else:
            self.delete_queue[key] = value
        self.lock.release()

    def remove_item(self, key, is_add_queue):
        self.lock.acquire()
        if is_add_queue:
            self.add_queue.pop(key)
        else:
            self.delete_queue.pop(key)
        self.lock.release()

    def get_one(self, is_add_queue):
        self.lock.acquire()
        if is_add_queue:
            value = self.add_queue.popitem()
        else:
            value = self.delete_queue.popitem()
        self.lock.release()
        return value

    def is_empty(self, is_add_queue):
        self.lock.acquire()
        if is_add_queue:
            res = self.add_queue.__len__() == 0
        else:
            res = self.delete_queue.__len__() == 0
        self.lock.release()
        return res

    def get_size(self,is_add_queue):
        self.lock.acquire()
        if is_add_queue:
            res = self.add_queue.__len__()
        else:
            res = self.delete_queue.__len__()
        self.lock.release()
        return res
