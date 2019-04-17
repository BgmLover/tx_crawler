import db
import config
import utils

if __name__ == '__main__':
    # a = dict()
    # a[1] =2
    # print(a.popitem()[1])
    tx_table = db.Tx("/Users/qingzhang/tx_.db", config.block_tx_table_name)
    tx_list = tx_table.get_all_tx()
    utils.dump_csv_file(tx_list, "confirmed_tx.csv")
    print("dumped confirmed tx")

    # tx_table = db.Tx("tx_.db", config.raw_tx_table_name)
    # tx_list1 = tx_table.get_all_tx()
    # utils.dump_csv_file(tx_list1, "unconfirmed.csv")
    # print("dumped unconfirmed tx")

