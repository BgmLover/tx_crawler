# a simple crawler to get bitcoin's transaction information

## description
* goal: get and track unconfirmed transactions

* data source: https://www.blockchain.com/btc/unconfirmed-transactions

* result: you will get two sqlite db file and a tx.csv which will be like:

  | hash                                                         | size(bytes) | Receive_time | Block_time   | Total_input(BTC) | Total_output(BTC) | fees(BTC) | Fee_rate(sat/btye) |
  | ------------------------------------------------------------ | ----------- | ------------ | ------------ | ---------------- | ----------------- | --------- | ------------------ |
  | 81991f04b2df862482eabcd95520d38e0faffd5e0cfb3ea53cdd55ee9f4c2054 | 217         | 1554757170.0 | 1554757467.0 | 0.00038281       | 0.00029813        | 8.468e-05 | 39.023             |
  |                                                              |             |              |              |                  |                   |           |                    |

  

## dependence
* beautifulsoup4
* sqlite3
* requests

you can just run the build.sh to install these dependence
```bash
./build.sh
```
## run
```bash
python3 crawler.py
```