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


Datum edw_handler(PG_FUNCTION_ARGS);
Datum edw_validator(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(edw_handler);
PG_FUNCTION_INFO_V1(edw_validator);

PG_MODULE_MAGIC; //lets postgres know that this is dynamically loadable (put in ONE source file after fmgr.h)

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

enum edw_type {
    EDWT_STR,
    EDWT_INT,
    EDWT_LIST,
    EDWT_OBJ
};


struct edw_response {
    char* buf;
    size_t size;
    int cur;
};

#define THRD_COUNT 64

struct edw_state {
    char* key_buf;
    size_t key_len;
    char* buf;
    size_t len;
    int cur;
    int count;

    struct edw_response bufs[THRD_COUNT];
};

struct edw_string {
    char* start;
    int len;
};

struct edw_list {
    struct edw_value* values;
    int count;
    int capacity;
};

struct edw_value {
    enum edw_type type;
    union {
        struct edw_string string;
        uint64_t integer;
        struct edw_list *list;
        struct edw_object *object;
    } as;
};

struct edw_entry {
    struct edw_string key;
    struct edw_value value;
};

struct edw_object {
    struct edw_entry* entries;
    int capacity;
    int count;
};

void edw_skip_whitespace(struct edw_response *res);
bool edw_is_digit(char c);
void edw_next_char(struct edw_response *res);
char edw_peek_char(struct edw_response *res);
uint64_t edw_parse_integer(struct edw_response *res);
struct edw_string edw_parse_string(struct edw_response *res);
struct edw_list *edw_parse_list(struct edw_response *res);
struct edw_value edw_parse_value(struct edw_response *res);
struct edw_object *edw_parse_object(struct edw_response *res);

struct edw_object *edw_object_init(void);
void edw_object_free(struct edw_object* o);
void edw_object_append_entry(struct edw_object *o, struct edw_entry e);
struct edw_value edw_object_get(struct edw_object *o, const char* key);

struct edw_list *edw_list_init(void);
void edw_list_free(struct edw_list* l);
void edw_list_append_value(struct edw_list *l, struct edw_value v);

void edw_insert_value(TupleTableSlot *slot, int idx, struct edw_object *obj, const char* attr, enum edw_type type);
void edw_insert_attr_value(TupleTableSlot *slot, int idx, const char* attr, struct edw_value v);

size_t write_callback(void *ptr, size_t size, size_t nmemb, void *userdata);
int thrd_request(void* args);

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
              strcmp("url", def->defname) == 0 ||
              strcmp("start", def->defname) == 0)) {
            ereport(ERROR,
                    errcode(ERRCODE_FDW_ERROR),
                    errmsg("\"%s\" is not a valid option", def->defname),
                    errhint("Valid table options for edw are \"start\", \"count\" and \"url\""));
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

void edw_skip_whitespace(struct edw_response *res) {
    char c = res->buf[res->cur];
    while (res->cur < res->size && (c == ' ' || c == '\n' || c == '\t')) {
        res->cur++;
        c = res->buf[res->cur];
    }
}

bool edw_is_digit(char c) {
    return '0' <= c && c <= '9'; 
}

void edw_next_char(struct edw_response *res) {
    edw_skip_whitespace(res);
    res->cur++;
}

char edw_peek_char(struct edw_response *res) {
    edw_skip_whitespace(res);
    return res->buf[res->cur];
}

uint64_t edw_parse_integer(struct edw_response *res) {
    char* start = &res->buf[res->cur];

    while (edw_is_digit(edw_peek_char(res))) {
        edw_next_char(res);
    }

    uint64_t final;
    sscanf(start, "%ld", &final);

    return final;
}

struct edw_string edw_parse_string(struct edw_response *res) {
    struct edw_string s;
    edw_next_char(res); //"

    s.start = &res->buf[res->cur];
    s.len = 0;

    while (edw_peek_char(res) != '"') {
        s.len++;
        edw_next_char(res);
    }

    edw_next_char(res); //"

    return s;
}

struct edw_list *edw_parse_list(struct edw_response *res) {
    struct edw_list *l;
    edw_next_char(res); //[

    l = edw_list_init();

    while (edw_peek_char(res) != ']') {
        edw_list_append_value(l, edw_parse_value(res));

        
        if (edw_peek_char(res) == ',') {
            edw_next_char(res);
        }
    }

    edw_next_char(res); //]

    return l;
}

struct edw_value edw_parse_value(struct edw_response *res) {
    struct edw_value v;

    switch (edw_peek_char(res)) {
        case '[':
            v.type = EDWT_LIST;
            v.as.list = edw_parse_list(res);
            break;
        case '"':
            v.type = EDWT_STR;
            v.as.string = edw_parse_string(res);
            break;
        case '{':
            v.type = EDWT_OBJ;
            v.as.object = edw_parse_object(res);
            break;
        default:
            v.type = EDWT_INT;
            v.as.integer = edw_parse_integer(res);
            break;
    }

    return v;
}

struct edw_object *edw_parse_object(struct edw_response *res) {
    struct edw_object *obj;
    struct edw_entry e;
    edw_next_char(res); //{

    obj = edw_object_init();

    while (edw_peek_char(res) != '}') {

        e.key = edw_parse_string(res); //key
        edw_next_char(res); //:
        e.value = edw_parse_value(res);

        edw_object_append_entry(obj, e);

        if (edw_peek_char(res) == ',') {
            edw_next_char(res);
        }
    }

    edw_next_char(res); //}

    return obj;
}

struct edw_object *edw_object_init(void) {
    struct edw_object* o = palloc(sizeof(struct edw_object));

    o->capacity = 8;
    o->count = 0;
    o->entries = palloc(sizeof(struct edw_entry) * o->capacity);

    return o;
}

void edw_object_free(struct edw_object* o) {
    for (int i = 0; i < o->count; i++) {
        struct edw_entry* e = &o->entries[i];
        if (e->value.type == EDWT_LIST) {
            edw_list_free(e->value.as.list);
        } else if (e->value.type == EDWT_OBJ) {
            edw_object_free(e->value.as.object);
        }
    }
    pfree(o->entries);
    pfree(o);
}

void edw_object_append_entry(struct edw_object* o, struct edw_entry e) {
    if (o->count + 1 > o->capacity) {
        o->capacity *= 2;
        o->entries = repalloc(o->entries, sizeof(struct edw_entry) * o->capacity);
    }
    o->entries[o->count] = e;
    o->count++;
}

struct edw_value edw_object_get(struct edw_object *o, const char* key) {
    char buf[128];
    int cur = 0;
    for (int i = 0; i < o->count; i++) {
        struct edw_entry e = o->entries[i];
        struct edw_string cur_key = e.key;
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

struct edw_list *edw_list_init(void) {
    struct edw_list* l = palloc(sizeof(struct edw_list));

    l->capacity = 8;
    l->count = 0;
    l->values = palloc(sizeof(struct edw_value) * l->capacity);

    return l;
}

void edw_list_free(struct edw_list* l) {
    for (int i = 0; i < l->count; i++) {
        struct edw_value* v = &l->values[i];
        if (v->type == EDWT_LIST) {
            edw_list_free(v->as.list);
        } else if (v->type == EDWT_OBJ) {
            edw_object_free(v->as.object);
        }
    }
    pfree(l->values);
    pfree(l);
}

void edw_list_append_value(struct edw_list *l, struct edw_value v) {
    if (l->count + 1 > l->capacity) {
        l->capacity *= 2;
        l->values = repalloc(l->values, sizeof(struct edw_value) * l->capacity);
    }
    l->values[l->count] = v;
    l->count++;
}

void edw_insert_value(TupleTableSlot *slot, int idx, struct edw_object *obj, const char* attr, enum edw_type type) {
    char buf[1028];
    for (int i = 0; i < obj->count; i++) {
        struct edw_entry e = obj->entries[i];
        if (strncmp(e.key.start, attr, strlen(attr)) == 0) {
            slot->tts_isnull[idx] = false;

            switch (type) {
                case EDWT_INT:
                    slot->tts_values[idx] = Int64GetDatum(e.value.as.integer);
                    break;
                case EDWT_STR:
                    memcpy(buf, e.value.as.string.start, e.value.as.string.len);
                    buf[e.value.as.string.len] = '\0';
                    slot->tts_values[idx] = CStringGetTextDatum(buf);
                    break;
                default:
                    break;
            }

            break;
        }
    }
}


void edw_insert_attr_value(TupleTableSlot *slot, int idx, const char* attr, struct edw_value v) {
    slot->tts_isnull[idx] = false;
    switch (v.type) {
        case EDWT_INT:
            slot->tts_values[idx] = Int64GetDatum(v.as.integer);
            break;
        case EDWT_STR: {
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

size_t write_callback(void *ptr, size_t size, size_t nmemb, void *userdata) {
    size_t realsize;
    struct edw_response *d;
    char *buf;

    realsize = size * nmemb;
    d = (struct edw_response*)userdata;

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


int thrd_request(void* args) {
    int i = *((int*)args);
    struct edw_state* state = *((struct edw_state**)((uint8_t*)(args) + sizeof(int)));
    int id = i % THRD_COUNT;

    CURL *curl;
    curl = curl_easy_init();
    CURLcode res;
    struct curl_slist *list = NULL;

    char uri[128];
    sprintf(uri, "https://ethereum-mainnet-archive.allthatnode.com/%s", state->key_buf);
    //sprintf(uri, "https://mainnet.infura.io/v3/%s", state->key_buf);
    const char *header = "Content-Type: application/json";
    
    curl_easy_setopt(curl, CURLOPT_URL, uri);
    list = curl_slist_append(list, header);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, list);

    char data[128];
    sprintf(data, "{ \"jsonrpc\":\"2.0\", \"method\":\"debug_getRawHeader\",\"params\":[\"0x110fec6\"],\"id\":%d}", i);
    size_t len = strlen(data);

    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, len);
    curl_easy_setopt(curl, CURLOPT_COPYPOSTFIELDS, data);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &state->bufs[id]);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);

    curl_easy_setopt(curl, CURLOPT_URL, uri);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

    long http_code = 0;
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


TupleTableSlot *edw_IterateForeignScan(ForeignScanState *node) {
    struct edw_state *state;
    TupleTableSlot *slot;

    slot = node->ss.ss_ScanTupleSlot;
    ExecClearTuple(slot);
    state = node->fdw_state;

    int MAX = 64;

    if (state->count < MAX) {
        if (state->count % THRD_COUNT == 0) { //batch by thread count
            thrd_t threads[THRD_COUNT];
            uint8_t thrd_data[THRD_COUNT][sizeof(int) + sizeof(struct edw_state**)];

            for (int i = 0; i < THRD_COUNT; i++) {
                *((int*)(thrd_data[i])) = (state->count / THRD_COUNT) * THRD_COUNT + i;
                *((struct edw_state**)(thrd_data[i] + sizeof(int))) = state;
                thrd_create(&threads[i], &thrd_request, thrd_data[i]);
            }
            
            for (int i = 0; i < THRD_COUNT; i++) {
                thrd_join(threads[i], NULL);
            }
        }

        if (state->count < MAX) {
            struct edw_object *obj = edw_parse_object(&state->bufs[state->count % THRD_COUNT]);
            //struct edw_value v = edw_object_get(obj, "result");

            int i = 0;
            edw_insert_attr_value(slot, i++, "result", edw_object_get(obj, "result"));
            /*
            edw_insert_attr_value(slot, i++, "difficulty", edw_object_get(v.as.object, "difficulty"));
            edw_insert_attr_value(slot, i++, "baseFeePerGas", edw_object_get(v.as.object, "baseFeePerGas"));
            edw_insert_attr_value(slot, i++, "extraData", edw_object_get(v.as.object, "extraData"));
            edw_insert_attr_value(slot, i++, "gasLimit", edw_object_get(v.as.object, "gasLimit"));
            edw_insert_attr_value(slot, i++, "gasUsed", edw_object_get(v.as.object, "gasUsed"));

            edw_insert_attr_value(slot, i++, "hash", edw_object_get(v.as.object, "hash"));
            edw_insert_attr_value(slot, i++, "logsBloom", edw_object_get(v.as.object, "logsBloom"));
            edw_insert_attr_value(slot, i++, "miner", edw_object_get(v.as.object, "miner"));
            edw_insert_attr_value(slot, i++, "mixHash", edw_object_get(v.as.object, "mixHash"));
            edw_insert_attr_value(slot, i++, "nonce", edw_object_get(v.as.object, "nonce"));

            edw_insert_attr_value(slot, i++, "number", edw_object_get(v.as.object, "number"));
            edw_insert_attr_value(slot, i++, "parentHash", edw_object_get(v.as.object, "parentHash"));
            edw_insert_attr_value(slot, i++, "receiptsRoot", edw_object_get(v.as.object, "receiptsRoot"));
            edw_insert_attr_value(slot, i++, "sha3Uncles", edw_object_get(v.as.object, "sha3Uncles"));
            edw_insert_attr_value(slot, i++, "size", edw_object_get(v.as.object, "size"));

            edw_insert_attr_value(slot, i++, "stateRoot", edw_object_get(v.as.object, "stateRoot"));
            edw_insert_attr_value(slot, i++, "timestamp", edw_object_get(v.as.object, "timestamp"));
            edw_insert_attr_value(slot, i++, "transactionsRoot", edw_object_get(v.as.object, "transactionsRoot"));
            edw_insert_attr_value(slot, i++, "withdrawalsRoot", edw_object_get(v.as.object, "withdrawalsRoot"));
            edw_insert_attr_value(slot, i++, "totalDifficulty", edw_object_get(v.as.object, "totalDifficulty"));*/

            ExecStoreVirtualTuple(slot);
            edw_object_free(obj);
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

