MODULE_big = eth_fdw
OBJS = eth_fdw.o

EXTENSION = eth_fdw
DATA = eth_fdw--1.0.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
