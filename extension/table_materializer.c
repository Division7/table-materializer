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

static char  *database_name      = NULL;
static int    update_interval_ms = 5000;

/* ----------------------------------------------------------------
 * Heuristic GUC state  (see HEURISTIC section below)
 * ---------------------------------------------------------------- */
static int    max_mv_count          = 5;
static int    heuristic_min_calls   = 10;
static double heuristic_min_exec_ms = 100.0;

static volatile sig_atomic_t got_sigterm = false;

void _PG_init(void);
PGDLLEXPORT void bgworker_main(Datum main_arg);
PG_FUNCTION_INFO_V1(top_expensive_queries);
PG_FUNCTION_INFO_V1(force_spawn_mvs);

static void tq_shmem_request(void);
static void tq_shmem_startup(void);
static void handle_sigterm(SIGNAL_ARGS);
static void update_top_queries(void);
static int  do_select_and_create_mvs(void);
static void select_and_create_mvs(void);

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

    DefineCustomIntVariable("table_materializer.max_materialized_views",
                            "Maximum number of auto-created IMMVs the worker will maintain.",
                            "The worker picks this many tables from pg_stat_statements "
                            "using the heuristic score and creates incrementally maintained "
                            "materialized views (IMMVs) via pg_ivm.",
                            &max_mv_count,
                            5, 1, MV_REGISTRY_MAX,
                            PGC_SIGHUP, 0,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("table_materializer.heuristic_min_calls",
                            "Minimum execution count before a query is a materialization candidate.",
                            "Raise to ignore rarely-run queries; lower to react faster to new workloads.",
                            &heuristic_min_calls,
                            10, 1, INT_MAX,
                            PGC_SIGHUP, 0,
                            NULL, NULL, NULL);

    DefineCustomRealVariable("table_materializer.heuristic_min_exec_time_ms",
                             "Minimum mean execution time (ms) for a query to qualify for an IMMV.",
                             "Filters out already-fast queries that do not benefit from materialization.",
                             &heuristic_min_exec_ms,
                             100.0, 0.0, 3600000.0,
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
        select_and_create_mvs();
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

/* ================================================================
 * MV SELECTION HEURISTIC
 * ================================================================
 *
 * This is the single place to tune which queries get a materialized
 * view.  Three levers are available:
 *
 * 1. GUC THRESHOLDS — set in postgresql.conf or ALTER SYSTEM SET:
 *
 *      table_materializer.max_materialized_views   (default: 5)
 *          Hard cap on IMMVs the worker will create and maintain.
 *          Range: 1 – MV_REGISTRY_MAX (32).
 *
 *      table_materializer.heuristic_min_calls      (default: 10)
 *          A query must have been executed at least this many times
 *          to be a candidate.  Raise to ignore rare/bursty queries;
 *          lower to react more aggressively to new workloads.
 *
 *      table_materializer.heuristic_min_exec_time_ms  (default: 100.0)
 *          Minimum mean execution time (ms) a query must have before
 *          its table is considered.  Prevents MVs for fast look-ups.
 *
 * 2. SCORING FORMULA — edit HEURISTIC_SCORE_EXPR and recompile:
 *
 *      The expression is evaluated per pg_stat_statements row; tables
 *      are ranked by their highest-scoring query.
 *
 *      Columns available (pg_stat_statements, PostgreSQL 14+):
 *        calls              total invocation count
 *        mean_exec_time     average wall time per call (ms)
 *        total_exec_time    sum of all wall times (ms)
 *        stddev_exec_time   standard deviation of exec time (ms)
 *        rows               average rows returned per call
 *        shared_blks_hit    avg shared-buffer hits per call
 *        shared_blks_read   avg physical reads per call
 *
 *      Ready-made alternatives (replace the string and recompile):
 *        "total_exec_time"                  same as default, precomputed
 *        "mean_exec_time"                   slowest individual queries first
 *        "mean_exec_time * ln(calls + 1)"   diminishing return on volume
 *        "stddev_exec_time * calls"         high-variance / unpredictable
 *        "shared_blks_read * calls"         I/O-heavy queries
 *
 * 3. SQL TEMPLATE — edit heuristic_sql_tmpl inside do_select_and_create_mvs()
 *      for structural changes: extra WHERE filters, different GROUP BY,
 *      schema awareness, join detection, etc.
 * ================================================================
 */
#define HEURISTIC_SCORE_EXPR  "mean_exec_time * calls"

/*
 * do_select_and_create_mvs — inner MV selection and creation logic.
 *
 * Must be called with SPI already connected and an active snapshot.
 * Returns the number of IMMVs newly created or re-registered.
 *
 * Called by select_and_create_mvs() (BGW path, manages its own txn) and
 * by force_spawn_mvs() (SQL-callable path, runs in the caller's txn).
 */
static int
do_select_and_create_mvs(void)
{
    /*
     * Heuristic SQL template — structural changes belong here.
     * Format args: heuristic_min_calls (%d), heuristic_min_exec_ms (%.4f),
     * max_mv_count (%d).  Use %% for literal percent (LIKE wildcards).
     */
    static const char heuristic_sql_tmpl[] =
        "WITH ranked AS ("
        "  SELECT"
        "    lower((regexp_match(query,"
        "      'FROM[[:space:]]+\"?([[:alpha:]_][[:alnum:]_]*)\"?',"
        "      'i'))[1]) AS tbl,"
        "    " HEURISTIC_SCORE_EXPR " AS score"
        "  FROM pg_stat_statements"
        "  WHERE upper(ltrim(query)) LIKE 'SELECT%%'"
        "    AND calls          >= %d"
        "    AND mean_exec_time >= %.4f"
        ")"
        " SELECT tbl, max(score) AS best_score"
        " FROM ranked"
        " WHERE tbl IS NOT NULL"
        "   AND tbl NOT LIKE 'pg_%%'"
        " GROUP BY tbl"
        " ORDER BY best_score DESC"
        " LIMIT %d";

    char  heuristic_sql[1024];
    /* Copied out before Phase 2 SPI calls overwrite SPI_tuptable. */
    char  candidates[MV_REGISTRY_MAX][NAMEDATALEN];
    int   num_candidates = 0;
    int   created = 0;
    int   ret;
    int   i;

    snprintf(heuristic_sql, sizeof(heuristic_sql),
             heuristic_sql_tmpl,
             heuristic_min_calls,
             heuristic_min_exec_ms,
             max_mv_count);

    /* ---- Phase 1: collect candidate table names ---- */
    ret = SPI_execute(heuristic_sql, true, max_mv_count);
    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        int nrows = (int) SPI_processed;

        for (i = 0; i < nrows && num_candidates < MV_REGISTRY_MAX; i++)
        {
            bool  isnull;
            Datum d = SPI_getbinval(SPI_tuptable->vals[i],
                                    SPI_tuptable->tupdesc, 1, &isnull);
            if (!isnull)
                strlcpy(candidates[num_candidates++],
                        TextDatumGetCString(d), NAMEDATALEN);
        }
    }

    /* ---- Phase 2: create IMMVs for new candidates ---- */
    for (i = 0; i < num_candidates; i++)
    {
        const char *tbl = candidates[i];
        char        mv_name[NAMEDATALEN + 16]; /* tbl + "_auto_mv" */
        bool        already_registered;
        bool        immv_exists;
        int         j;
        Oid         arg_types[2];
        Datum       arg_values[2];

        /* Reject anything that looks like a system catalog */
        if (strncmp(tbl, "pg_", 3) == 0)
            continue;

        snprintf(mv_name, sizeof(mv_name), "%s_auto_mv", tbl);

        /*
         * pg_ivm creates IMMVs as regular tables, not matviews.
         * Check pg_tables to see if this IMMV already exists on disk.
         */
        arg_types[0]  = TEXTOID;
        arg_values[0] = CStringGetTextDatum(mv_name);
        ret = SPI_execute_with_args(
                "SELECT 1 FROM pg_tables "
                "WHERE schemaname = 'public' AND tablename = $1",
                1, arg_types, arg_values, " ", true, 1);
        immv_exists = (ret == SPI_OK_SELECT && SPI_processed > 0);

        /* Check if already tracked in the shared registry. */
        already_registered = false;
        LWLockAcquire(&top_queries_locks[1].lock, LW_SHARED);
        for (j = 0; j < mv_registry_state->num_entries; j++)
        {
            if (strcmp(mv_registry_state->entries[j].mv_name, mv_name) == 0)
            {
                already_registered = true;
                break;
            }
        }
        LWLockRelease(&top_queries_locks[1].lock);

        /*
         * Skip only when the IMMV actually exists on disk AND is in the
         * registry.  If the table was dropped but the registry still has a
         * stale entry, fall through to re-create it.
         */
        if (immv_exists && already_registered)
            continue;

        if (!immv_exists)
        {
            /* Verify the source table actually exists in public schema */
            arg_values[0] = CStringGetTextDatum(tbl);
            ret = SPI_execute_with_args(
                    "SELECT 1 FROM pg_tables "
                    "WHERE schemaname = 'public' AND tablename = $1",
                    1, arg_types, arg_values, " ", true, 1);

            if (ret != SPI_OK_SELECT || SPI_processed == 0)
            {
                ereport(DEBUG1,
                        (errmsg("table_materializer: skipping \"%s\" — "
                                "table not found in public schema", tbl)));
                continue;
            }

            /*
             * Create the IMMV via pg_ivm.  format('%I', ...) safely quotes
             * identifiers to prevent SQL injection.
             *
             * create_immv() populates the view immediately and installs
             * triggers on the source table that keep it incrementally
             * up-to-date on every INSERT/UPDATE/DELETE — no REFRESH needed.
             */
            arg_types[0]  = TEXTOID;
            arg_types[1]  = TEXTOID;
            arg_values[0] = CStringGetTextDatum(mv_name);
            arg_values[1] = CStringGetTextDatum(tbl);
            ret = SPI_execute_with_args(
                    "SELECT pgivm.create_immv("
                    "  format('public.%I', $1),"
                    "  format('SELECT * FROM public.%I', $2)"
                    ")",
                    2, arg_types, arg_values, "  ", false, 0);

            if (ret < 0)
            {
                ereport(LOG,
                        (errmsg("table_materializer: create_immv failed "
                                "for table \"%s\" (SPI code %d), skipping",
                                tbl, ret)));
                continue;
            }

            ereport(LOG,
                    (errmsg("table_materializer: created IMMV "
                            "public.%s for table public.%s",
                            mv_name, tbl)));
        }

        /* Add the (new or pre-existing) IMMV to the shared registry */
        LWLockAcquire(&top_queries_locks[1].lock, LW_EXCLUSIVE);
        if (mv_registry_state->num_entries < MV_REGISTRY_MAX)
        {
            MVRegistryEntry *entry =
                &mv_registry_state->entries[mv_registry_state->num_entries];

            memset(entry, 0, sizeof(*entry));
            strlcpy(entry->mv_schema, "public", sizeof(entry->mv_schema));
            strlcpy(entry->mv_name,   mv_name,  sizeof(entry->mv_name));
            entry->num_source_tables = 1;
            strlcpy(entry->source_tables[0].schema, "public",
                    sizeof(entry->source_tables[0].schema));
            strlcpy(entry->source_tables[0].name, tbl,
                    sizeof(entry->source_tables[0].name));
            entry->has_col_map = false;
            mv_registry_state->num_entries++;
            created++;

            ereport(DEBUG1,
                    (errmsg("table_materializer: registered IMMV "
                            "public.%s in MV registry (slot %d)",
                            mv_name, mv_registry_state->num_entries - 1)));
        }
        else
        {
            ereport(LOG,
                    (errmsg("table_materializer: MV registry full "
                            "(%d entries), cannot register public.%s",
                            MV_REGISTRY_MAX, mv_name)));
        }
        LWLockRelease(&top_queries_locks[1].lock);
    }

    return created;
}

/*
 * select_and_create_mvs — BGW wrapper around do_select_and_create_mvs.
 * Sets up a full transaction + SPI context, runs the inner logic, then
 * tears down.  Errors are caught and logged so the BGW loop survives.
 */
static void
select_and_create_mvs(void)
{
    PG_TRY();
    {
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot(GetTransactionSnapshot());
        do_select_and_create_mvs();
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
 * SQL-callable: table_materializer_force_spawn()
 *
 * Runs the MV selection heuristic immediately in the calling session,
 * bypassing the BGW interval.  Useful for testing and on-demand setup.
 * Returns the number of IMMVs created or re-registered this call.
 *
 * Example:
 *   SELECT table_materializer_force_spawn();
 * ---------------------------------------------------------------- */
Datum
force_spawn_mvs(PG_FUNCTION_ARGS)
{
    int n;

    SPI_connect();
    n = do_select_and_create_mvs();
    SPI_finish();

    PG_RETURN_INT32(n);
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
