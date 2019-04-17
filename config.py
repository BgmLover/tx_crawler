from multiprocessing import cpu_count

db_name = "tx.db"
db_copy_name = "tx_.db"
db_dump_time = 5 * 60

raw_tx_table_name = "raw_tx"
block_tx_table_name = "block_tx"
delta_tx_table_name = "delta_tx"

raw_tx_dump_interval = 60 * 10
delta_tx_dump_interval = 60 * 10

dump_tx_dir = 'snapshots/'

generate_snapshot = False

total_thead_size = cpu_count()
get_raw_tx_thread_size = min(6, total_thead_size - 2)
update_tx_thread_size = min(cpu_count() - get_raw_tx_thread_size, 8)


block_stop_time = "2018-01-01 00:00:00"
block_init_height = 570444
thread_pool_max_size = 1