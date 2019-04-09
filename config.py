from multiprocessing import cpu_count

raw_tx_db_name = "raw_tx.db"
block_tx_db_name = "block_tx.db"

generate_csv = True

total_thead_size = cpu_count()
get_raw_tx_thread_size = min(3, total_thead_size - 2)
update_tx_thread_size = cpu_count() - get_raw_tx_thread_size
