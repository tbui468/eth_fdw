#include "postgres.h"
#include "utils/builtins.h" //for CStringGetTextDatum
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "optimizer/pathnode.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/planmain.h"
#include "utils/rel.h"
#include "access/table.h"
#include "access/reloptions.h"
#include "catalog/pg_type.h"
#include "foreign/foreign.h"
#include "commands/defrem.h"
#include "nodes/pg_list.h"

#include "curl/curl.h"
#include <threads.h>

#define THRD_COUNT 64
#define MAX 64

PG_MODULE_MAGIC; //lets postgres know that this is dynamically loadable (put in ONE source file after fmgr.h)
PG_FUNCTION_INFO_V1(edw_handler);
PG_FUNCTION_INFO_V1(edw_validator);

/*
 * Shared primitive data structures and function declarations
 */

enum data_type {
    DATA_STR,
    DATA_INT,
    DATA_LIST,
    DATA_OBJ
};

struct data_buf {
    char* buf;
    size_t size;
    int cur;
};

struct data_string {
    char* start;
    int len;
};

struct data_list {
    struct data_value* values;
    int count;
    int capacity;
};

struct data_value {
    enum data_type type;
    union {
        struct data_string string;
        uint64_t integer;
        struct data_list *list;
        struct data_object *object;
    } as;
};

struct data_entry {
    struct data_string key;
    struct data_value value;
};

struct data_object {
    struct data_entry* entries;
    int capacity;
    int count;
};

struct data_value data_string_init(char* start, int len);

struct data_object *data_object_init(void);
void data_object_free(struct data_object* o);
void data_object_append_entry(struct data_object *o, struct data_entry e);
struct data_value data_object_get(struct data_object *o, const char* key);

struct data_list *data_list_init(void);
void data_list_free(struct data_list* l);
void data_list_append_value(struct data_list *l, struct data_value v);
void data_list_insert_value(struct data_list *l, struct data_value v, int idx);

/*
 * Ethereum data wrapper core structs and function declarations
 */

struct edw_state {
    char* key_buf;
    size_t key_len;

    char* buf;
    size_t len;

    int cur; //???? Think of more descriptive field name - I don't remember what this is for
    int count; //???? Think of more descriptive field name - don't remember

    struct data_buf bufs[THRD_COUNT];
};

Datum edw_handler(PG_FUNCTION_ARGS);
Datum edw_validator(PG_FUNCTION_ARGS);
void edw_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
void edw_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
ForeignScan *edw_GetForeignPlan(PlannerInfo *root, 
                                         RelOptInfo *baserel,
                                         Oid foreigntablid,
                                         ForeignPath *best_path, 
                                         List *tlist, 
                                         List *scan_clauses, 
                                         Plan *outer_plan);
void edw_BeginForeignScan(ForeignScanState *node, int eflags);
TupleTableSlot *edw_IterateForeignScan(ForeignScanState *node);
void edw_ReScanForeignScan(ForeignScanState *node);
void edw_EndForeignScan(ForeignScanState *node);

void edw_insert_value(TupleTableSlot *slot, int idx, struct data_value v);
int edw_append_headers(TupleTableSlot *slot, int start_idx, struct data_list *headers);
int edw_append_transactions(TupleTableSlot *slot, int start_idx, struct data_list *encoded_txns);
int edw_append_uncles(TupleTableSlot *slot, int start_idx, struct data_list *uncles);
int edw_append_withdrawals(TupleTableSlot *slot, int start_idx, struct data_list *withdrawals);

/*
 * Json parser structs and function declarations
 */

void json_skip_whitespace(struct data_buf *res);
bool json_is_digit(char c);
void json_next_char(struct data_buf *res);
char json_peek_char(struct data_buf *res);
uint64_t json_parse_integer(struct data_buf *res);
struct data_string json_parse_string(struct data_buf *res);
struct data_list *json_parse_list(struct data_buf *res);
struct data_value json_parse_value(struct data_buf *res);
struct data_object *json_parse_object(struct data_buf *res);

/*
 * Recursive-length prefix (RLP) decoder structs and function declarations
 */

struct data_value rlp_decode_raw_buf(struct data_buf *buf);
struct data_value rlp_parse_value(struct data_buf *buf);
int rlp_peek_hex(struct data_buf *buf, int bytes);
int rlp_next_hex(struct data_buf *buf, int bytes);
struct data_value rlp_parse_string(struct data_buf *buf, bool is_long);
struct data_value rlp_parse_list(struct data_buf *buf, bool is_long);

/*
 * Http structs and function declarations
 */

size_t http_write_callback(void *ptr, size_t size, size_t nmemb, void *userdata);
int http_send_request(void* args);


/*
 * Shared primitive data function implementations
 */

struct data_value data_string_init(char* start, int len) {
    struct data_value v;
    v.type = DATA_STR;
    v.as.string.start = start;
    v.as.string.len = len;
    return v;
}

struct data_object *data_object_init(void) {
    struct data_object* o = palloc(sizeof(struct data_object));

    o->capacity = 8;
    o->count = 0;
    o->entries = palloc(sizeof(struct data_entry) * o->capacity);

    return o;
}

void data_object_free(struct data_object* o) {
    for (int i = 0; i < o->count; i++) {
        struct data_entry* e = &o->entries[i];
        if (e->value.type == DATA_LIST) {
            data_list_free(e->value.as.list);
        } else if (e->value.type == DATA_OBJ) {
            data_object_free(e->value.as.object);
        }
    }
    pfree(o->entries);
    pfree(o);
}

void data_object_append_entry(struct data_object* o, struct data_entry e) {
    if (o->count + 1 > o->capacity) {
        o->capacity *= 2;
        o->entries = repalloc(o->entries, sizeof(struct data_entry) * o->capacity);
    }
    o->entries[o->count] = e;
    o->count++;
}

struct data_value data_object_get(struct data_object *o, const char* key) {
    char buf[128];
    int cur = 0;
    for (int i = 0; i < o->count; i++) {
        struct data_entry e = o->entries[i];
        struct data_string cur_key = e.key;
        if (cur_key.len == strlen(key) && strncmp(cur_key.start, key, cur_key.len) == 0) {
            return e.value;
        }
        memcpy(buf + cur, cur_key.start, cur_key.len);
        cur += cur_key.len; 
    }

    buf[cur] = '\0';
    ereport(ERROR,
            errcode(ERRCODE_FDW_ERROR),
            errmsg("result key now found: keys %s", buf));
}

struct data_list *data_list_init(void) {
    struct data_list* l = palloc(sizeof(struct data_list));

    l->capacity = 8;
    l->count = 0;
    l->values = palloc(sizeof(struct data_value) * l->capacity);

    return l;
}

void data_list_free(struct data_list* l) {
    for (int i = 0; i < l->count; i++) {
        struct data_value* v = &l->values[i];
        if (v->type == DATA_LIST) {
            data_list_free(v->as.list);
        } else if (v->type == DATA_OBJ) {
            data_object_free(v->as.object);
        }
    }
    pfree(l->values);
    pfree(l);
}

void data_list_append_value(struct data_list *l, struct data_value v) {
    if (l->count + 1 > l->capacity) {
        l->capacity *= 2;
        l->values = repalloc(l->values, sizeof(struct data_value) * l->capacity);
    }
    l->values[l->count] = v;
    l->count++;
}

void data_list_insert_value(struct data_list *l, struct data_value v, int idx) {
    if (l->count + 1 > l->capacity) {
        l->capacity *= 2;
        l->values = repalloc(l->values, sizeof(struct data_value) * l->capacity);
    }

    struct data_value *src = &l->values[idx];
    struct data_value *dst = src + 1;
    size_t len = (l->count - idx) * sizeof(struct data_value);
    memmove(dst, src, len);

    l->values[idx] = v;
    l->count++;
}

/*
 * Ethererum data wrapper core function implementations
 */

Datum edw_handler(PG_FUNCTION_ARGS) {
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);

    fdwroutine->GetForeignRelSize   =   edw_GetForeignRelSize;
    fdwroutine->GetForeignPaths     =   edw_GetForeignPaths;
    fdwroutine->GetForeignPlan      =   edw_GetForeignPlan;
    fdwroutine->BeginForeignScan    =   edw_BeginForeignScan;
    fdwroutine->IterateForeignScan  =   edw_IterateForeignScan;
    fdwroutine->ReScanForeignScan   =   edw_ReScanForeignScan;
    fdwroutine->EndForeignScan      =   edw_EndForeignScan;

    PG_RETURN_POINTER(fdwroutine);    
}

//validate table options
Datum edw_validator(PG_FUNCTION_ARGS) {
    List *options_list;
    ListCell *cell;
    
    
    options_list = untransformRelOptions(PG_GETARG_DATUM(0));

    foreach(cell, options_list) {
        DefElem *def = lfirst_node(DefElem, cell);

        if (!(strcmp("count", def->defname) == 0 ||
              strcmp("provider", def->defname) == 0)) {
            ereport(ERROR,
                    errcode(ERRCODE_FDW_ERROR),
                    errmsg("\"%s\" is not a valid option", def->defname),
                    errhint("Valid table options for edw are \"count\" and \"provider\""));
        }
    }


    PG_RETURN_VOID();
}

void edw_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {
    /*
    Oid typid;
    Relation rel;

    //validate table schema
    //TODO: should define the function for importing a foreign schema (look at postgres_fdw.c for hints on how to do this)
    rel = table_open(foreigntableid, NoLock);

    if (rel->rd_att->natts != 20) {
        ereport(ERROR,
                errcode(ERRCODE_FDW_INVALID_COLUMN_NUMBER),
                errmsg("incorrect schema for tutorial_fdw table %s: table must have exactly twenty columns", NameStr(rel->rd_rel->relname)));
    }

    typid = rel->rd_att->attrs[0].atttypid;
    if (typid != TEXTOID) {
        ereport(ERROR,
                errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
                errmsg("incorrect schema for tutorial_fdw table %s: table column must have type int", NameStr(rel->rd_rel->relname)));
    }

    table_close(rel, NoLock);*/

    //ForeignTable *ft = GetForeignTable(foreigntableid);
    //eth_fdw_apply_data_options(state, ft);


    //make private struct for fdw, and pass to baserel
    //read options values (should have been validated in validator by now)
    //set baserel->rows (this is used to estimate best path????)
}

void edw_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {
    Path *path = (Path*)create_foreignscan_path(root, baserel,
            NULL,
            baserel->rows,
            1,
            1 + baserel->rows,
            NIL,
            NULL,
            NULL,
            NIL);
    add_path(baserel, path);
}

ForeignScan *edw_GetForeignPlan(PlannerInfo *root, 
                                         RelOptInfo *baserel,
                                         Oid foreigntablid,
                                         ForeignPath *best_path, 
                                         List *tlist, 
                                         List *scan_clauses, 
                                         Plan *outer_plan) {

    //get private struct from baserel
    //pass those options to make_foreignscan function (may need to extend node or make data into nodes)
    scan_clauses = extract_actual_clauses(scan_clauses, false);
    return make_foreignscan(tlist,
            scan_clauses,
            baserel->relid,
            NIL,
            NIL,
            NIL,
            NIL,
            outer_plan);
}

void edw_BeginForeignScan(ForeignScanState *node, int eflags) {
    struct edw_state *state;
    long bufsize;
    FILE* f;
    state = palloc(sizeof(struct edw_state));
    state->cur = 0;
    state->len = 10;

    const char* path = "/home/thomas/eth_fdw/allthatnode.key";
    //const char* path = "/home/thomas/eth_fdw/infura.key";

    if (!(f = fopen(path, "r"))) {
        ereport(ERROR,
                errcode(ERRCODE_FDW_ERROR),
                errmsg("failed to open \"%s\": fopen failed", path));
    }

    if (fseek(f, 0L, SEEK_END) != 0) {
        ereport(ERROR,
                errcode(ERRCODE_FDW_ERROR),
                errmsg("failed to seek to end of file \"%s\": fseek failed", path));
    }

    if ((bufsize = ftell(f)) == -1) {
        ereport(ERROR,
                errcode(ERRCODE_FDW_ERROR),
                errmsg("failed to find offset at end of file \"%s\": ftell failed", path));
    }

    if (fseek(f, 0L, SEEK_SET) != 0) {
        ereport(ERROR,
                errcode(ERRCODE_FDW_ERROR),
                errmsg("failed to seek to beginning of file \"%s\": fseek failed", path));
    }

    state->key_buf = palloc(sizeof(char) * (bufsize + 1));
    state->key_len = fread(state->key_buf, sizeof(char), bufsize, f);
    state->cur = 0;

    if (ferror(f) != 0) {
        ereport(ERROR,
                errcode(ERRCODE_FDW_ERROR),
                errmsg("failed to read file \"%s\": fread failed", path));
    } else {
        state->key_buf[state->key_len - 1] = '\0'; //overwriting eof/newline TODO: a bit hacky
    }

    fclose(f);

    curl_global_init(CURL_GLOBAL_ALL);

    state->count = 0;

    node->fdw_state = state;
}

void edw_insert_value(TupleTableSlot *slot, int idx, struct data_value v) {
    slot->tts_isnull[idx] = false;
    switch (v.type) {
        case DATA_INT: {
            char buf[128];
            sprintf(buf, "%ld", v.as.integer);
            slot->tts_values[idx] = CStringGetTextDatum(buf);
            //slot->tts_values[idx] = Int64GetDatum(v.as.integer); using all TEXT for now just to simplify my life
            break;
        }
        case DATA_STR: {
            char* buf = palloc(sizeof(char) * (v.as.string.len + 1));
            memcpy(buf, v.as.string.start, v.as.string.len);
            buf[v.as.string.len] = '\0';
            slot->tts_values[idx] = CStringGetTextDatum(buf);
            pfree(buf);
            break;
        }
        default:
            break;
    }
}

int edw_append_headers(TupleTableSlot *slot, int start_idx, struct data_list *headers) {
    //block header.  See EIP4895 for details
   
    int idx = start_idx;
    edw_insert_value(slot, idx++, headers->values[0]); //parent_hash
    edw_insert_value(slot, idx++, headers->values[1]); //ommers_hash
    edw_insert_value(slot, idx++, headers->values[2]); //coinbase
    edw_insert_value(slot, idx++, headers->values[3]); //state_root
    edw_insert_value(slot, idx++, headers->values[4]); //txs_root

    edw_insert_value(slot, idx++, headers->values[5]); //receipts_root
    edw_insert_value(slot, idx++, headers->values[6]); //logs_bloom
    edw_insert_value(slot, idx++, headers->values[7]); //difficulty
    edw_insert_value(slot, idx++, headers->values[8]); //number
    edw_insert_value(slot, idx++, headers->values[9]); //gas_limit

    edw_insert_value(slot, idx++, headers->values[10]); //gas_used
    edw_insert_value(slot, idx++, headers->values[11]); //timestamp
    edw_insert_value(slot, idx++, headers->values[12]); //extradata
    edw_insert_value(slot, idx++, headers->values[13]); //prev_randao
    edw_insert_value(slot, idx++, headers->values[14]); //nonce

    edw_insert_value(slot, idx++, headers->values[15]); //base_fee_per_gas
    edw_insert_value(slot, idx++, headers->values[16]); //withdrawals_root

    return idx;
}

int edw_append_transactions(TupleTableSlot *slot, int start_idx, struct data_list *encoded_txns) {
    //transactions (first value is type, and then the following list is transaction information)
    //see EIP1559 for details

    int idx = start_idx;
    int ele = 13; //using EIP1559 fields. Types without those fields (0 and 1) will be filled with null
    Datum *values = palloc(sizeof(Datum) * encoded_txns->count * ele);
    bool *nulls = palloc(sizeof(bool) * encoded_txns->count * ele);
    int dims[2];
    dims[0] = encoded_txns->count;
    dims[1] = ele;
    int lbs[2];
    lbs[0] = 1;
    lbs[1] = 1;

    for (int i = 0; i < encoded_txns->count; i++) {
        struct data_value v = encoded_txns->values[i];
        struct data_list *fields;
        if (v.type == DATA_LIST) {
            //type 0 transaction
            struct data_value type = data_string_init("0", 1);
            fields = v.as.list;
            data_list_insert_value(fields, type, 0);
        } else {
            //type 1 or 2 transaction - need to decode payload
            struct data_buf buf;
            buf.size = v.as.string.len;
            buf.buf = v.as.string.start;
            buf.cur = 0;
            struct data_value type = rlp_parse_value(&buf);
            fields = rlp_parse_value(&buf).as.list;
            data_list_insert_value(fields, type, 0);
        }


        /*
        struct data_value d = fields->values[0];
        switch (d.type) {
            case DATA_INT: {
                char n[128];
                sprintf(n, "%ld", d.as.integer);
                values[i * ele] = CStringGetTextDatum(n);
                break;
            }
            case DATA_STR: {
                char n[128];
                memcpy(n, d.as.string.start, d.as.string.len);
                n[d.as.string.len] = '\0';
                values[i * ele] = CStringGetTextDatum(n);
                break;
            }
             default:
                break;
        }
        nulls[i * ele] = false;*/

        for (int j = 0; j < ele; j++) {
            if (j >= fields->count) {
                nulls[i * ele + j] = true;
                continue;
            }

            //TODO: This is breaking - a non-string/int is getting decoded
            struct data_value d = fields->values[j];
            switch (d.type) {
                case DATA_INT: {
                    char n[128];
                    sprintf(n, "%ld", d.as.integer);
                    values[i * ele + j] = CStringGetTextDatum(n);
                    break;
                }
                case DATA_STR: {
                    char *n = palloc(sizeof(char) * (d.as.string.len + 1));
                    memcpy(n, d.as.string.start, d.as.string.len);
                    n[d.as.string.len] = '\0';
                    values[i * ele + j] = CStringGetTextDatum(n);
                    pfree(n);
                    break;
                }
                case DATA_LIST: {
                    values[i * ele + j] = CStringGetTextDatum("access list placeholder");
                    break;
                }
                 default:
        ereport(ERROR,
                errcode(ERRCODE_FDW_ERROR),
                errmsg("non int or string found in transaction!"));
                    break;
            }
            //values[i * ele + j] = CStringGetTextDatum("est");
            nulls[i * ele + j] = false;
        }
    }

    slot->tts_isnull[idx] = false;
    slot->tts_values[idx++] = PointerGetDatum(construct_md_array(values, nulls, 2, dims, lbs, TEXTOID, -1, false, 'i'));

    pfree(values);

    return idx;
}

int edw_append_uncles(TupleTableSlot *slot, int start_idx, struct data_list *uncles) {
    return start_idx;
}

int edw_append_withdrawals(TupleTableSlot *slot, int start_idx, struct data_list *withdrawals) {
    //withdrawals. See EIP4895 for details
    
    int idx = start_idx;
    int ele = 4; //index, validator_index, address, amount
    Datum *values = palloc(sizeof(Datum) * withdrawals->count * ele);
    int dims[2];
    dims[0] = withdrawals->count;
    dims[1] = ele;
    int lbs[2];
    lbs[0] = 1;
    lbs[1] = 1;

    for (int i = 0; i < withdrawals->count; i++) {
        struct data_list *w = withdrawals->values[i].as.list;
        for (int j = 0; j < ele; j++) {
            struct data_string s = w->values[j].as.string;
            char buf[128];
            memcpy(buf, s.start, s.len);
            buf[s.len] = '\0';
            values[i * ele + j] = CStringGetTextDatum(buf);
        }
    }

    Datum array = PointerGetDatum(construct_md_array(values, NULL, 2, dims, lbs, TEXTOID, -1, false, 'i'));
    slot->tts_isnull[idx] = false;
    slot->tts_values[idx++] = array;

    pfree(values);

    return idx;
}

TupleTableSlot *edw_IterateForeignScan(ForeignScanState *node) {
    struct edw_state *state;
    TupleTableSlot *slot;

    slot = node->ss.ss_ScanTupleSlot;
    ExecClearTuple(slot);
    state = node->fdw_state;

    if (state->count < MAX) {
        //batch http requests by THRD_COUNT
        if (state->count % THRD_COUNT == 0) {
            thrd_t threads[THRD_COUNT];
            uint8_t thrd_data[THRD_COUNT][sizeof(int) + sizeof(struct edw_state**)];

            for (int i = 0; i < THRD_COUNT; i++) {
                *((int*)(thrd_data[i])) = (state->count / THRD_COUNT) * THRD_COUNT + i;
                *((struct edw_state**)(thrd_data[i] + sizeof(int))) = state;
                thrd_create(&threads[i], &http_send_request, thrd_data[i]);
            }
            
            for (int i = 0; i < THRD_COUNT; i++) {
                thrd_join(threads[i], NULL);
            }
        }

        if (state->count < MAX) {
            struct data_object *obj = json_parse_object(&state->bufs[state->count % THRD_COUNT]);
            struct data_value s = data_object_get(obj, "result");

            struct data_buf buf;
            buf.buf = s.as.string.start;
            buf.size = s.as.string.len;
            buf.cur = 0;
            struct data_list *decoded_result = rlp_decode_raw_buf(&buf).as.list;

            int idx = 0;
            idx = edw_append_headers(slot, idx, decoded_result->values[0].as.list);
            idx = edw_append_transactions(slot, idx, decoded_result->values[1].as.list);
            idx = edw_append_uncles(slot, idx, decoded_result->values[2].as.list);
            idx = edw_append_withdrawals(slot, idx, decoded_result->values[3].as.list);

            ExecStoreVirtualTuple(slot);
            data_object_free(obj);
            free(state->bufs[state->count % THRD_COUNT].buf);
        }
    }

    state->count++;

    return slot;
}

void edw_ReScanForeignScan(ForeignScanState *node) {
    struct edw_state *state = node->fdw_state;
    state->cur = 0;
}

void edw_EndForeignScan(ForeignScanState *node) {
    struct edw_state *state = node->fdw_state;
    curl_global_cleanup();
    pfree(state);
}

/*
 * Json parser function implementations
 */

void json_skip_whitespace(struct data_buf *res) {
    char c = res->buf[res->cur];
    while (res->cur < res->size && (c == ' ' || c == '\n' || c == '\t')) {
        res->cur++;
        c = res->buf[res->cur];
    }
}

bool json_is_digit(char c) {
    return '0' <= c && c <= '9'; 
}

void json_next_char(struct data_buf *res) {
    json_skip_whitespace(res);
    res->cur++;
}

char json_peek_char(struct data_buf *res) {
    json_skip_whitespace(res);
    return res->buf[res->cur];
}

uint64_t json_parse_integer(struct data_buf *res) {
    char* start = &res->buf[res->cur];

    while (json_is_digit(json_peek_char(res))) {
        json_next_char(res);
    }

    uint64_t final;
    sscanf(start, "%ld", &final);

    return final;
}

struct data_string json_parse_string(struct data_buf *res) {
    struct data_string s;
    json_next_char(res); //"

    s.start = &res->buf[res->cur];
    s.len = 0;

    while (json_peek_char(res) != '"') {
        s.len++;
        json_next_char(res);
    }

    json_next_char(res); //"

    return s;
}

struct data_list *json_parse_list(struct data_buf *res) {
    struct data_list *l;
    json_next_char(res); //[

    l = data_list_init();

    while (json_peek_char(res) != ']') {
        data_list_append_value(l, json_parse_value(res));

        
        if (json_peek_char(res) == ',') {
            json_next_char(res);
        }
    }

    json_next_char(res); //]

    return l;
}

struct data_value json_parse_value(struct data_buf *res) {
    struct data_value v;

    switch (json_peek_char(res)) {
        case '[':
            v.type = DATA_LIST;
            v.as.list = json_parse_list(res);
            break;
        case '"':
            v.type = DATA_STR;
            v.as.string = json_parse_string(res);
            break;
        case '{':
            v.type = DATA_OBJ;
            v.as.object = json_parse_object(res);
            break;
        default:
            v.type = DATA_INT;
            v.as.integer = json_parse_integer(res);
            break;
    }

    return v;
}

struct data_object *json_parse_object(struct data_buf *res) {
    struct data_object *obj;
    struct data_entry e;
    json_next_char(res); //{

    obj = data_object_init();

    while (json_peek_char(res) != '}') {

        e.key = json_parse_string(res); //key
        json_next_char(res); //:
        e.value = json_parse_value(res);

        data_object_append_entry(obj, e);

        if (json_peek_char(res) == ',') {
            json_next_char(res);
        }
    }

    json_next_char(res); //}

    return obj;
}

/*
 * Recursive-length prefix decoder function implementations
 */

struct data_value rlp_decode_raw_buf(struct data_buf* buf) {
    //seek until we find 'x'
    char c;
    while ((c = buf->buf[buf->cur]) != 'x') {
        buf->cur++;
    }

    buf->cur++; //skip 'x'

    return rlp_parse_value(buf);
}

struct data_value rlp_parse_value(struct data_buf* buf) {
    int hex = rlp_peek_hex(buf, 1);

    if (0x0 <= hex && hex <= 0x7f) {
        struct data_value v;
        v.type = DATA_INT;
        v.as.integer = rlp_next_hex(buf, 1);
        return v;
    } else if (0x80 <= hex && hex <= 0xb7) {
        return rlp_parse_string(buf, false);
    } else if (0xb8 <= hex && hex <= 0xbf) {
        return rlp_parse_string(buf, true);
    } else if (0xc0 <= hex && hex <= 0xf7) {
        return rlp_parse_list(buf, false);
    } else if (0xf8 <= hex && hex <= 0xff) {
        return rlp_parse_list(buf, true);
    }

    ereport(ERROR,
            errcode(ERRCODE_FDW_ERROR),
            errmsg("invalid rlp format"));
}

int rlp_peek_hex(struct data_buf *buf, int bytes) {
    int len;
    char hex[128];

    len = bytes * 2;
    memcpy(hex, &buf->buf[buf->cur], sizeof(char) * len);
    hex[len] = '\0';
    return (int)strtol(hex, NULL, 16);
}

int rlp_next_hex(struct data_buf *buf, int bytes) {
    int hex = rlp_peek_hex(buf, bytes);
    buf->cur += bytes * 2;
    return hex;
}

struct data_value rlp_parse_string(struct data_buf *buf, bool is_long) {
    int payload_len, payload_len_bytes;
    struct data_value v;

    if (!is_long) {
        payload_len = rlp_next_hex(buf, 1) - 0x80;
    } else {
        payload_len_bytes = rlp_next_hex(buf, 1) - 0xb7;
        payload_len = rlp_next_hex(buf, payload_len_bytes);
    }

    v.type = DATA_STR;
    v.as.string.start = &buf->buf[buf->cur];
    v.as.string.len = payload_len * 2;

    buf->cur += payload_len * 2;

    return v;
}

struct data_value rlp_parse_list(struct data_buf *buf, bool is_long) {
    int payload_len, payload_len_bytes, end;
    struct data_list *l;
    struct data_value v;

    if (!is_long) {
        payload_len = rlp_next_hex(buf, 1) - 0xc0;
    } else {
        payload_len_bytes = rlp_next_hex(buf, 1) - 0xf7;
        payload_len = rlp_next_hex(buf, payload_len_bytes);
    }

    end = buf->cur + payload_len * 2;

    l = data_list_init();

    while (buf->cur < end) {
        data_list_append_value(l, rlp_parse_value(buf));
    }

    v.type = DATA_LIST;
    v.as.list = l;
    return v;
}

/*
 * Http request function implementations
 */

size_t http_write_callback(void *ptr, size_t size, size_t nmemb, void *userdata) {
    size_t realsize;
    struct data_buf *d;
    char *buf;

    realsize = size * nmemb;
    d = (struct data_buf*)userdata;

    buf = realloc(d->buf, d->size + realsize + 1); //why does using repalloc cause a crash here
    if (!buf) {
        return 0;
    }
    d->buf = buf;
    memcpy(&(d->buf[d->size]), ptr, realsize);
    d->size += realsize;
    d->buf[d->size] = '\0';

    return realsize;
}


int http_send_request(void* args) {
    CURL *curl;
    CURLcode res;
    struct curl_slist *list;
    struct edw_state *state;
    int off, id;
    char uri[128];
    char data[128];
    size_t len;
    long http_code;
    const char *header = "Content-Type: application/json";

    off = *((int*)args);
    state = *((struct edw_state**)((uint8_t*)(args) + sizeof(int)));
    id = off % THRD_COUNT;

    curl = curl_easy_init();
    list = NULL;

    sprintf(uri, "https://ethereum-mainnet-archive.allthatnode.com/%s", state->key_buf);
    //sprintf(uri, "https://mainnet.infura.io/v3/%s", state->key_buf);
    
    curl_easy_setopt(curl, CURLOPT_URL, uri);
    list = curl_slist_append(list, header);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, list);

    const int start = 17891014;
    char hex[16];
    hex[0] = '0';
    hex[1] = 'x';
    sprintf(hex + 2, "%X", start + off);
    sprintf(data, "{ \"jsonrpc\":\"2.0\", \"method\":\"debug_getRawBlock\",\"params\":[\"%s\"],\"id\":%d}", hex, off);
    len = strlen(data);

    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, len);
    curl_easy_setopt(curl, CURLOPT_COPYPOSTFIELDS, data);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &state->bufs[id]);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, http_write_callback);

    curl_easy_setopt(curl, CURLOPT_URL, uri);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

    http_code = 0;
    state->bufs[id].buf = NULL;
    do {
        //initialize buffer to null/free in case curl_easy_perform is called multiple times
        if (state->bufs[id].buf) {
            free(state->bufs[id].buf);
        }
        state->bufs[id].buf = NULL;
        state->bufs[id].size = 0;
        state->bufs[id].cur = 0;

        res = curl_easy_perform(curl);
        if (res != CURLE_OK) {
            ereport(ERROR,
                    errcode(ERRCODE_FDW_ERROR),
                    errmsg("curl error: id: %d, %s", id, curl_easy_strerror(res)));
        }
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    } while (http_code != 200);


    curl_slist_free_all(list);
    curl_easy_cleanup(curl);

    return 0;
}

