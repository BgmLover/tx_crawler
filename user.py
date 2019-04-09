import db
import config
import utils

if __name__ == '__main__':
    tx_table = db.Tx(config.db_name, config.delta_tx_table_name)
    tx_list = tx_table.get_all_tx()
    utils.dump_csv_file(tx_list, "block_tx.csv")
