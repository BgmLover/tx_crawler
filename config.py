from multiprocessing import cpu_count

db_name = "/data/tx.db"

raw_tx_table_name = "raw_tx"
block_tx_table_name = "block_tx"
delta_tx_table_name = "delta_tx"

raw_tx_dump_interval = 60 * 10
delta_tx_dump_interval = 60 * 10

dump_tx_dir = 'snapshots/'

generate_snapshot = False

total_thead_size = cpu_count()
get_raw_tx_thread_size = min(3, total_thead_size - 2)
update_tx_thread_size = cpu_count() - get_raw_tx_thread_size
