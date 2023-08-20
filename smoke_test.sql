DROP EXTENSION IF EXISTS eth_fdw CASCADE; --drops eth_server, eth_view, eth_table since they all depend on eth_fdw


CREATE EXTENSION eth_fdw;
CREATE SERVER eth_server FOREIGN DATA WRAPPER eth_fdw;
CREATE FOREIGN TABLE eth_table (
parentHash TEXT,
sha3Uncles TEXT,
miner TEXT,
stateRoot TEXT,
transactionsRoot TEXT,
receiptsRoot TEXT,
logsBloom TEXT,
difficulty TEXT,
number TEXT,
gasLimit TEXT,
gasUsed TEXT,
timestamp TEXT,
extraData TEXT,
mixHash TEXT,
nonce TEXT,
baseFeePerGas TEXT,
withdrawalsRoot TEXT,
transactions TEXT[][],
withdrawals TEXT[][]
                            ) SERVER eth_server OPTIONS ( start '1', count '100', url '/home/thomas/eth_fdw/blocks.json' );


CREATE MATERIALIZED VIEW eth_view AS SELECT * FROM eth_table;
SELECT number, nonce FROM eth_view;

--REFRESH MATERIALIZED VIEW eth_view;
