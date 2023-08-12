#include "postgres.h"
#include "utils/builtins.h" //for CStringGetTextDatum
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "optimizer/pathnode.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/planmain.h"
#include "utils/rel.h"
#include "access/table.h"
#include "catalog/pg_type.h"
#include "foreign/foreign.h"
#include "commands/defrem.h"
#include "nodes/pg_list.h"

Datum edw_handler(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(edw_handler);
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

struct edw_state {
    char* buf;
    size_t len;
    int cur;
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

void edw_skip_whitespace(struct edw_state *state);
bool edw_is_digit(char c);
void edw_next_char(struct edw_state *state);
char edw_peek_char(struct edw_state *state);
uint64_t edw_parse_integer(struct edw_state *state);
struct edw_string edw_parse_string(struct edw_state *state);
struct edw_list *edw_parse_list(struct edw_state *state);
struct edw_value edw_parse_value(struct edw_state *state);
struct edw_object *edw_parse_object(struct edw_state *state);

struct edw_object *edw_object_init(void);
void edw_object_free(struct edw_object* o);
void edw_object_append_entry(struct edw_object *o, struct edw_entry e);

struct edw_list *edw_list_init(void);
void edw_list_free(struct edw_list* l);
void edw_list_append_value(struct edw_list *l, struct edw_value v);

void edw_insert_value(TupleTableSlot *slot, int idx, struct edw_object *obj, const char* attr, enum edw_type type);

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

void edw_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {
    //check options
    //make private struct for fdw, and pass to baserel
    //validate attribute count
    //validate data types
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

    if (!(f = fopen("/home/thomas/eth_fdw/blocks.json", "r"))) {
        //error
    }

    if (fseek(f, 0L, SEEK_END) != 0) {
        //error
    }

    if ((bufsize = ftell(f)) == -1) {
        //error
    }

    if (fseek(f, 0L, SEEK_SET) != 0) {
        //error
    }

    state->buf = palloc(sizeof(char) * (bufsize + 1));
    state->len = fread(state->buf, sizeof(char), bufsize, f);
    state->cur = 0;

    if (ferror(f) != 0) {
        //error reading
    } else {
        state->buf[state->len] = '\0';
    }

    fclose(f);
    node->fdw_state = state;
}

void edw_skip_whitespace(struct edw_state *state) {
    char c = state->buf[state->cur];
    while (state->cur < state->len && (c == ' ' || c == '\n' || c == '\t')) {
        state->cur++;
        c = state->buf[state->cur];
    }
}

bool edw_is_digit(char c) {
    return '0' <= c && c <= '9'; 
}

void edw_next_char(struct edw_state *state) {
    edw_skip_whitespace(state);
    state->cur++;
}

char edw_peek_char(struct edw_state *state) {
    edw_skip_whitespace(state);
    return state->buf[state->cur];
}

uint64_t edw_parse_integer(struct edw_state *state) {
    char* start = &state->buf[state->cur];

    while (edw_is_digit(edw_peek_char(state))) {
        edw_next_char(state);
    }

    uint64_t final;
    sscanf(start, "%ld", &final);

    return final;
}

struct edw_string edw_parse_string(struct edw_state *state) {
    edw_next_char(state); //"

    struct edw_string s;
    s.start = &state->buf[state->cur];
    s.len = 0;

    while (edw_peek_char(state) != '"') {
        s.len++;
        edw_next_char(state);
    }

    edw_next_char(state); //"

    return s;
}

struct edw_list *edw_parse_list(struct edw_state *state) {
    edw_next_char(state); //[

    struct edw_list *l = edw_list_init();

    while (edw_peek_char(state) != ']') {
        edw_list_append_value(l, edw_parse_value(state));

        
        if (edw_peek_char(state) == ',') {
            edw_next_char(state);
        }
    }

    edw_next_char(state); //]

    return l;
}

struct edw_value edw_parse_value(struct edw_state *state) {
    struct edw_value v;

    switch (edw_peek_char(state)) {
        case '[':
            v.type = EDWT_LIST;
            v.as.list = edw_parse_list(state);
            break;
        case '"':
            v.type = EDWT_STR;
            v.as.string = edw_parse_string(state);
            break;
        case '{':
            v.type = EDWT_OBJ;
            v.as.object = edw_parse_object(state);
            break;
        default:
            v.type = EDWT_INT;
            v.as.integer = edw_parse_integer(state);
            break;
    }

    return v;
}

struct edw_object *edw_parse_object(struct edw_state *state) {
    edw_next_char(state); //{

    struct edw_object* obj = edw_object_init();

    while (edw_peek_char(state) != '}') {
        struct edw_entry e;

        e.key = edw_parse_string(state); //key
        edw_next_char(state); //:
        e.value = edw_parse_value(state);

        edw_object_append_entry(obj, e);

        if (edw_peek_char(state) == ',') {
            edw_next_char(state);
        }
    }

    edw_next_char(state); //}

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

struct edw_list *edw_list_init(void) {
    struct edw_list* l = palloc(sizeof(struct edw_list));

    l->capacity = 8;
    l->count = 0;
    l->values = palloc(sizeof(struct edw_value) * l->capacity);

    return l;
}

void edw_list_free(struct edw_list* l) {
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

TupleTableSlot *edw_IterateForeignScan(ForeignScanState *node) {
    struct edw_state *state;
    TupleTableSlot *slot;

    slot = node->ss.ss_ScanTupleSlot;
    ExecClearTuple(slot);
    state = node->fdw_state;

    while (state->cur < state->len && edw_peek_char(state) != '{') {
        edw_next_char(state);
    }

    if (state->cur < state->len) {
        struct edw_object* obj = edw_parse_object(state);

        int i = 0;
        edw_insert_value(slot, i++, obj, "baseFeePerGas", EDWT_INT);
        edw_insert_value(slot, i++, obj, "difficulty", EDWT_INT);
        edw_insert_value(slot, i++, obj, "extraData", EDWT_STR);
        edw_insert_value(slot, i++, obj, "gasLimit", EDWT_INT);
        edw_insert_value(slot, i++, obj, "gasUsed", EDWT_INT);
        edw_insert_value(slot, i++, obj, "hash", EDWT_STR);
        edw_insert_value(slot, i++, obj, "logsBloom", EDWT_STR);
        edw_insert_value(slot, i++, obj, "miner", EDWT_STR);
        edw_insert_value(slot, i++, obj, "mixHash", EDWT_STR);
        edw_insert_value(slot, i++, obj, "nonce", EDWT_STR);
        edw_insert_value(slot, i++, obj, "number", EDWT_INT);
        edw_insert_value(slot, i++, obj, "parentHash", EDWT_STR);
        edw_insert_value(slot, i++, obj, "receiptsRoot", EDWT_STR);
        edw_insert_value(slot, i++, obj, "sha3Uncles", EDWT_STR);
        edw_insert_value(slot, i++, obj, "size", EDWT_INT);
        edw_insert_value(slot, i++, obj, "stateRoot", EDWT_STR);
        edw_insert_value(slot, i++, obj, "timestamp", EDWT_INT);
        edw_insert_value(slot, i++, obj, "totalDifficulty", EDWT_INT);
        edw_insert_value(slot, i++, obj, "transactionsRoot", EDWT_STR);
        edw_insert_value(slot, i++, obj, "withdrawalsRoot", EDWT_STR);


        ExecStoreVirtualTuple(slot);
        edw_object_free(obj);
    }

    return slot;
}

void edw_ReScanForeignScan(ForeignScanState *node) {
    struct edw_state *state = node->fdw_state;
    state->cur = 0;
}

void edw_EndForeignScan(ForeignScanState *node) {
    struct edw_state *state = node->fdw_state;
    pfree(state->buf);
    pfree(state);
}

