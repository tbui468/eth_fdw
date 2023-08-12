DROP FOREIGN TABLE IF EXISTS eth_table;
DROP SERVER IF EXISTS eth_server;
DROP EXTENSION IF EXISTS eth_fdw;


CREATE EXTENSION eth_fdw;
CREATE SERVER eth_server FOREIGN DATA WRAPPER eth_fdw;
CREATE FOREIGN TABLE eth_table (baseFeePerGas INT8, 
                                difficulty INT8, 
                                extraData TEXT,
                                gasLimit INT8,
                                gasUsed INT8,
                                hash TEXT,
                                logsBloom TEXT,
                                miner TEXT,
                                mixHash TEXT,
                                nonce TEXT,
                                number INT8,
                                parentHash TEXT,
                                receiptsRoot TEXT,
                                sha3Uncles TEXT,
                                size INT8,
                                stateRoot TEXT,
                                timestamp INT8,
                                totalDifficulty INT8,
                                transactionsRoot TEXT,
                                withdrawalsRoot TEXT
                            ) SERVER eth_server OPTIONS ( start '0', end '4' );


SELECT timestamp, totalDifficulty, transactionsRoot, withdrawalsRoot FROM eth_table;

