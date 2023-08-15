DROP FOREIGN TABLE IF EXISTS eth_table;
DROP SERVER IF EXISTS eth_server;
DROP EXTENSION IF EXISTS eth_fdw;


CREATE EXTENSION eth_fdw;
CREATE SERVER eth_server FOREIGN DATA WRAPPER eth_fdw;
CREATE FOREIGN TABLE eth_table (jsonrpc TEXT, 
                                id INT8
                            ) SERVER eth_server OPTIONS ( start '1', count '100', url '/home/thomas/eth_fdw/blocks.json' );


SELECT * FROM eth_table;

