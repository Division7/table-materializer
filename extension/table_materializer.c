#include "postgres.h"
#include "fmgr.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "parser/analyze.h"
#include "parser/parse_relation.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "catalog/pg_type.h"

PG_MODULE_MAGIC;

/* ----------------------------------------------------------------
 * Top-queries cache
 * ---------------------------------------------------------------- */

#define TOP_N     5
#define QUERY_LEN 1024

typedef struct
{
    char   query[QUERY_LEN];
    double mean_exec_time_ms;
    int64  calls;
} TopQuery;

typedef struct
{
    TopQuery entries[TOP_N];
} TopQueriesState;

/* ----------------------------------------------------------------
 * MV registry
 * ---------------------------------------------------------------- */

#define MV_MAX_TABLES   4
#define MV_NAME_LEN     64
#define MV_REGISTRY_MAX 32

typedef struct
{
    char mv_schema[MV_NAME_LEN];
    char mv_name[MV_NAME_LEN];

    int  num_source_tables;   /* 1 = single-table mirror, 2+ = pre-join */

    struct
    {
        char schema[MV_NAME_LEN];
        char name[MV_NAME_LEN];
    } source_tables[MV_MAX_TABLES];

    /*
     * Phase-2 column mapping for join rewrites.
     * col_map[table_idx][src_attno] = mv_attno.
     * has_col_map == false means the MV has an identical column layout to
     * the source table — no remapping needed (single-table path only).
     */
    int  col_map[MV_MAX_TABLES][64];
    bool has_col_map;
} MVRegistryEntry;

typedef struct
{
    MVRegistryEntry entries[MV_REGISTRY_MAX];
    int             num_entries;
} MVRegistryState;

/*
 * Seed entries written into shared memory on first startup.
 * The background worker will eventually overwrite these with real data.
 * TODO: replace with catalog-based loading once the BGW creates MVs.
 */
static const MVRegistryEntry mv_seed_entries[] = {
    {
        .mv_schema         = "public",
        .mv_name           = "orders_mv",
        .num_source_tables = 1,
        .source_tables     = { { .schema = "public", .name = "orders" } },
        .has_col_map       = false,
    },
    {
        .mv_schema         = "public",
        .mv_name           = "customer_orders_mv",
        .num_source_tables = 2,
        .source_tables     = {
            { .schema = "public", .name = "customers" },
            { .schema = "public", .name = "orders"    },
        },
        .has_col_map       = false,
    },
};

static const int mv_seed_count =
    sizeof(mv_seed_entries) / sizeof(mv_seed_entries[0]);

/* ----------------------------------------------------------------
 * Shared state and hooks
 * ---------------------------------------------------------------- */

static TopQueriesState              *top_queries_state   = NULL;
static MVRegistryState              *mv_registry_state   = NULL;
/* slot 0 = top queries, slot 1 = mv registry */
static LWLockPadded                 *top_queries_locks   = NULL;
static shmem_request_hook_type       prev_shmem_request  = NULL;
static shmem_startup_hook_type       prev_shmem_startup  = NULL;
static post_parse_analyze_hook_type  prev_post_parse_analyze = NULL;

static char *database_name      = NULL;
static int   update_interval_ms = 5000;

static volatile sig_atomic_t got_sigterm = false;

void _PG_init(void);
PGDLLEXPORT void bgworker_main(Datum main_arg);
PG_FUNCTION_INFO_V1(top_expensive_queries);

static void tq_shmem_request(void);
static void tq_shmem_startup(void);
static void handle_sigterm(SIGNAL_ARGS);
static void update_top_queries(void);

static void tm_post_parse_analyze(ParseState *pstate, Query *query,
                                   JumbleState *jstate);
static bool tm_match_query(Query *query, MVRegistryEntry *out_entry,
                            int matched_rte_indexes[MV_MAX_TABLES]);
static void tm_rewrite_query(Query *query, const MVRegistryEntry *entry,
                              int matched_rte_indexes[MV_MAX_TABLES]);

/* ----------------------------------------------------------------
 * Extension init
 * ---------------------------------------------------------------- */

void
_PG_init(void)
{
    BackgroundWorker worker;

    if (!process_shared_preload_libraries_in_progress)
        ereport(ERROR,
                (errmsg("table_materializer must be loaded via shared_preload_libraries")));

    DefineCustomStringVariable("table_materializer.database",
                               "Database the background worker connects to.",
                               NULL,
                               &database_name,
                               "postgres",
                               PGC_POSTMASTER, 0,
                               NULL, NULL, NULL);

    DefineCustomIntVariable("table_materializer.interval_ms",
                            "How often (ms) the worker refreshes the top-queries cache.",
                            NULL,
                            &update_interval_ms,
                            5000, 100, 300000,
                            PGC_SIGHUP, 0,
                            NULL, NULL, NULL);

    MarkGUCPrefixReserved("table_materializer");

    prev_shmem_request = shmem_request_hook;
    shmem_request_hook = tq_shmem_request;

    prev_shmem_startup = shmem_startup_hook;
    shmem_startup_hook = tq_shmem_startup;

    prev_post_parse_analyze = post_parse_analyze_hook;
    post_parse_analyze_hook = tm_post_parse_analyze;

    memset(&worker, 0, sizeof(worker));
    worker.bgw_flags       = BGWORKER_SHMEM_ACCESS |
                             BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time  = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 10;
    snprintf(worker.bgw_name,          BGW_MAXLEN, "table_materializer worker");
    snprintf(worker.bgw_type,          BGW_MAXLEN, "table_materializer");
    snprintf(worker.bgw_library_name,  BGW_MAXLEN, "table_materializer");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "bgworker_main");
    worker.bgw_main_arg = Int32GetDatum(0);

    RegisterBackgroundWorker(&worker);
}

/* ----------------------------------------------------------------
 * Shared memory
 * ---------------------------------------------------------------- */

static void
tq_shmem_request(void)
{
    if (prev_shmem_request)
        prev_shmem_request();

    RequestAddinShmemSpace(sizeof(TopQueriesState));
    RequestAddinShmemSpace(sizeof(MVRegistryState));
    RequestNamedLWLockTranche("table_materializer", 2);
}

static void
tq_shmem_startup(void)
{
    bool found;

    if (prev_shmem_startup)
        prev_shmem_startup();

    top_queries_state = ShmemInitStruct("table_materializer",
                                        sizeof(TopQueriesState),
                                        &found);
    if (!found)
        memset(top_queries_state->entries, 0, sizeof(top_queries_state->entries));

    top_queries_locks = GetNamedLWLockTranche("table_materializer");

    mv_registry_state = ShmemInitStruct("table_materializer_mv_registry",
                                        sizeof(MVRegistryState),
                                        &found);
    if (!found)
    {
        int n = mv_seed_count < MV_REGISTRY_MAX ? mv_seed_count : MV_REGISTRY_MAX;

        memset(mv_registry_state, 0, sizeof(MVRegistryState));
        memcpy(mv_registry_state->entries, mv_seed_entries,
               n * sizeof(MVRegistryEntry));
        mv_registry_state->num_entries = n;
    }
}

/* ----------------------------------------------------------------
 * Query interceptor
 * ---------------------------------------------------------------- */

static void
tm_post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate)
{
    if (prev_post_parse_analyze)
        prev_post_parse_analyze(pstate, query, jstate);

    if (query->commandType != CMD_SELECT)
        return;

    if (query->rtable == NIL)
        return;

    {
        MVRegistryEntry matched_entry;
        int             matched_rte_indexes[MV_MAX_TABLES];

        if (!tm_match_query(query, &matched_entry, matched_rte_indexes))
            return;

        tm_rewrite_query(query, &matched_entry, matched_rte_indexes);
    }
}

/*
 * Walk query->rtable and try to match a registry entry whose source tables
 * are all present.  On match, copies the entry into *out_entry, fills
 * matched_rte_indexes, and returns true.
 */
static bool
tm_match_query(Query *query, MVRegistryEntry *out_entry,
               int matched_rte_indexes[MV_MAX_TABLES])
{
    MVRegistryEntry  registry_snapshot[MV_REGISTRY_MAX];
    int              num_entries;
    int              e;

    if (mv_registry_state == NULL)
        return false;

    /* Take a local copy under the shared lock so we don't hold it long. */
    LWLockAcquire(&top_queries_locks[1].lock, LW_SHARED);
    num_entries = mv_registry_state->num_entries;
    if (num_entries > 0)
        memcpy(registry_snapshot, mv_registry_state->entries,
               num_entries * sizeof(MVRegistryEntry));
    LWLockRelease(&top_queries_locks[1].lock);

    for (e = 0; e < num_entries; e++)
    {
        const MVRegistryEntry *entry = &registry_snapshot[e];
        int   found_indexes[MV_MAX_TABLES];
        int   found_count = 0;
        int   s;

        for (s = 0; s < entry->num_source_tables; s++)
        {
            ListCell *lc;
            int       rte_idx = 0;
            bool      table_found = false;

            foreach(lc, query->rtable)
            {
                RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

                if (rte->rtekind == RTE_RELATION)
                {
                    char *relname = get_rel_name(rte->relid);
                    char *nspname = get_namespace_name(
                                        get_rel_namespace(rte->relid));

                    if (relname != NULL && nspname != NULL &&
                        strcmp(relname, entry->source_tables[s].name) == 0 &&
                        strcmp(nspname, entry->source_tables[s].schema) == 0)
                    {
                        found_indexes[s] = rte_idx;
                        table_found = true;
                        pfree(relname);
                        pfree(nspname);
                        break;
                    }

                    if (relname) pfree(relname);
                    if (nspname) pfree(nspname);
                }
                rte_idx++;
            }

            if (table_found)
                found_count++;
            else
                break;
        }

        if (found_count == entry->num_source_tables)
        {
            memcpy(matched_rte_indexes, found_indexes,
                   entry->num_source_tables * sizeof(int));
            memcpy(out_entry, entry, sizeof(MVRegistryEntry));
            return true;
        }
    }

    return false;
}

/*
 * Rewrite the query in-place to use the matched MV.
 *
 * Single-table path: replace the source table's relid/relkind in its RTE.
 * Join path: phase-2 stub — logs and returns without rewriting.
 */
static void
tm_rewrite_query(Query *query, const MVRegistryEntry *entry,
                 int matched_rte_indexes[MV_MAX_TABLES])
{
    RangeTblEntry *rte;
    RangeVar       rv;
    Oid            mv_oid;

    if (entry->num_source_tables >= 2)
    {
        ereport(DEBUG1,
                (errmsg("table_materializer: join rewrite for MV \"%s\".\"%s\" "
                        "not yet implemented (phase 2)",
                        entry->mv_schema, entry->mv_name)));
        return;
    }

    /* Single-table path */
    rte = (RangeTblEntry *) list_nth(query->rtable, matched_rte_indexes[0]);

    memset(&rv, 0, sizeof(rv));
    rv.type            = T_RangeVar;
    rv.schemaname      = (char *) entry->mv_schema;
    rv.relname         = (char *) entry->mv_name;
    rv.inh             = false;
    rv.relpersistence  = RELPERSISTENCE_PERMANENT;
    rv.location        = -1;

    mv_oid = RangeVarGetRelid(&rv, NoLock, true /* missing_ok */);

    if (!OidIsValid(mv_oid))
    {
        ereport(DEBUG1,
                (errmsg("table_materializer: MV \"%s\".\"%s\" not found, "
                        "skipping rewrite",
                        entry->mv_schema, entry->mv_name)));
        return;
    }

    /*
     * PG 16+ maintains a parallel RTEPermissionInfo list.  getRTEPermissionInfo
     * checks perminfo->relid == rte->relid, so we must fetch the pointer
     * BEFORE changing rte->relid, then update both atomically.
     */
    {
        RTEPermissionInfo *perminfo = NULL;

        if (rte->perminfoindex > 0)
            perminfo = getRTEPermissionInfo(query->rteperminfos, rte);

        rte->relid       = mv_oid;
        rte->relkind     = RELKIND_MATVIEW;
        rte->inh         = false;
        rte->tablesample = NULL;

        if (perminfo != NULL)
        {
            perminfo->relid = mv_oid;
            perminfo->inh   = false;
        }
    }

    if (rte->eref != NULL)
        rte->eref->aliasname = pstrdup(entry->mv_name);

    ereport(DEBUG1,
            (errmsg("table_materializer: rewrote query to use MV \"%s\".\"%s\"",
                    entry->mv_schema, entry->mv_name)));
}

/* ----------------------------------------------------------------
 * Background worker
 * ---------------------------------------------------------------- */

static void
handle_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

PGDLLEXPORT void
bgworker_main(Datum main_arg)
{
    pqsignal(SIGTERM, handle_sigterm);
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    BackgroundWorkerUnblockSignals();

    BackgroundWorkerInitializeConnection(database_name, NULL, 0);

    elog(LOG, "table_materializer worker started (interval %d ms, db \"%s\")",
         update_interval_ms, database_name);

    while (!got_sigterm)
    {
        int rc;

        rc = WaitLatch(MyLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                       (long) update_interval_ms,
                       0);
        ResetLatch(MyLatch);

        if (rc & WL_LATCH_SET && got_sigterm)
            break;

        if (ConfigReloadPending)
        {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        update_top_queries();
    }

    proc_exit(0);
}

static void
update_top_queries(void)
{
    int ret;
    int i;

    PG_TRY();
    {
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot(GetTransactionSnapshot());

        ret = SPI_execute(
            "SELECT query, mean_exec_time, calls "
            "FROM pg_stat_statements "
            "ORDER BY mean_exec_time DESC "
            "LIMIT 5",
            true, TOP_N);

        if (ret == SPI_OK_SELECT && SPI_processed > 0)
        {
            LWLockAcquire(&top_queries_locks[0].lock, LW_EXCLUSIVE);
            memset(top_queries_state->entries, 0,
                   sizeof(top_queries_state->entries));

            for (i = 0; i < (int) SPI_processed && i < TOP_N; i++)
            {
                bool  isnull;
                Datum d;

                d = SPI_getbinval(SPI_tuptable->vals[i],
                                  SPI_tuptable->tupdesc, 1, &isnull);
                if (!isnull)
                    strlcpy(top_queries_state->entries[i].query,
                            TextDatumGetCString(d), QUERY_LEN);

                d = SPI_getbinval(SPI_tuptable->vals[i],
                                  SPI_tuptable->tupdesc, 2, &isnull);
                if (!isnull)
                    top_queries_state->entries[i].mean_exec_time_ms =
                        DatumGetFloat8(d);

                d = SPI_getbinval(SPI_tuptable->vals[i],
                                  SPI_tuptable->tupdesc, 3, &isnull);
                if (!isnull)
                    top_queries_state->entries[i].calls = DatumGetInt64(d);
            }

            LWLockRelease(&top_queries_locks[0].lock);
        }

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
    }
    PG_CATCH();
    {
        EmitErrorReport();
        if (IsTransactionState())
            AbortCurrentTransaction();
        FlushErrorState();
    }
    PG_END_TRY();
}

/* ----------------------------------------------------------------
 * SQL-callable function: top_expensive_queries()
 * ---------------------------------------------------------------- */

Datum
top_expensive_queries(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    TopQuery        *snapshot;

    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldctx;
        TupleDesc     tupdesc;
        int           j;

        funcctx = SRF_FIRSTCALL_INIT();
        oldctx  = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        snapshot = palloc(sizeof(TopQuery) * TOP_N);

        if (top_queries_state != NULL)
        {
            LWLockAcquire(&top_queries_locks[0].lock, LW_SHARED);
            memcpy(snapshot, top_queries_state->entries,
                   sizeof(TopQuery) * TOP_N);
            LWLockRelease(&top_queries_locks[0].lock);
        }
        else
            memset(snapshot, 0, sizeof(TopQuery) * TOP_N);

        j = 0;
        for (int i = 0; i < TOP_N; i++)
            if (snapshot[i].query[0] != '\0')
                snapshot[j++] = snapshot[i];

        funcctx->user_fctx = snapshot;
        funcctx->max_calls  = j;

        tupdesc = CreateTemplateTupleDesc(4);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "rank",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "query",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "mean_exec_time_ms",
                           FLOAT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "calls",
                           INT8OID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        MemoryContextSwitchTo(oldctx);
    }

    funcctx = SRF_PERCALL_SETUP();
    snapshot = (TopQuery *) funcctx->user_fctx;

    if (funcctx->call_cntr < funcctx->max_calls)
    {
        int       i = funcctx->call_cntr;
        Datum     values[4];
        bool      nulls[4] = {false, false, false, false};
        HeapTuple tuple;

        values[0] = Int32GetDatum(i + 1);
        values[1] = CStringGetTextDatum(snapshot[i].query);
        values[2] = Float8GetDatum(snapshot[i].mean_exec_time_ms);
        values[3] = Int64GetDatum(snapshot[i].calls);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
}
