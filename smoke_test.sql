DROP EXTENSION IF EXISTS eth_fdw CASCADE; --drops eth_server, eth_view, eth_table since they all depend on eth_fdw


CREATE EXTENSION eth_fdw;
CREATE SERVER eth_server FOREIGN DATA WRAPPER eth_fdw;
CREATE FOREIGN TABLE eth_table (
            number TEXT
    /*
            difficulty TEXT,
            baseFeePerGas TEXT,
            extraData TEXT,
            gasLimit TEXT,
            gasUsed TEXT,

            hash TEXT,
            logsBloom TEXT,
            miner TEXT,
            mixHash TEXT,
            nonce TEXT

            number TEXT,
            parentHash TEXT,
            receiptsRoot TEXT,
            sha3Uncles TEXT,
            size TEXT

            stateRoot TEXT,
            timestamp TEXT,
            transactionsRoot TEXT,
            withdrawalsRoot TEXT,
            totalDifficulty TEXT*/
                            ) SERVER eth_server OPTIONS ( start '1', count '100', url '/home/thomas/eth_fdw/blocks.json' );


CREATE MATERIALIZED VIEW eth_view AS SELECT * FROM eth_table;
SELECT * FROM eth_view;

--REFRESH MATERIALIZED VIEW eth_view;
