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
#include "catalog/pg_operator.h"
#include "nodes/nodeFuncs.h"
#include "utils/syscache.h"

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
     * Join columns linking consecutive source_tables pairs.
     * join_cols[i].t0_col = column on source_tables[i],
     * join_cols[i].t1_col = column on source_tables[i+1].
     * Used by tm_verify_join_condition to confirm the incoming query's
     * equijoin matches what the IMMV was built on (fail-closed safety check).
     */
    struct {
        char t0_col[MV_NAME_LEN];
        char t1_col[MV_NAME_LEN];
    } join_cols[MV_MAX_TABLES - 1];

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
 * The MV registry starts empty.  It is populated at runtime by:
 *   • the background worker (via select_and_create_mvs on each interval), and
 *   • table_materializer_force_spawn() (on-demand SQL call).
 *
 * Both paths call do_select_and_create_mvs(), which queries pg_stat_statements,
 * picks the top-N expensive tables, creates IMMVs via pg_ivm, and registers
 * each one here so the post_parse_analyze hook can rewrite matching queries.
 *
 * NOTE: do not add hand-written seed entries here.  A seed entry whose
 * backing table does not exist on disk causes the rewrite hook to silently
 * skip ALL queries that would have matched a dynamically-created IMMV with
 * lower source-table count, because tm_match_query picks the entry with the
 * highest num_source_tables without checking whether the physical table
 * exists.
 */
static const int mv_seed_count = 0;

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

/* Context for the Var-remapping expression mutator used in join rewrites. */
typedef struct
{
    int  varno_keep;        /* varno of source_tables[0]; becomes the IMMV varno */
    int  varno_drop;        /* varno of source_tables[1]; remapped to varno_keep */
    int  col_map_keep[64];  /* source_tables[0] attno → mv attno */
    int  col_map_drop[64];  /* source_tables[1] attno → mv attno */
    bool has_map;
} VarRewriteCtx;

/* Read-only context for pre-mutation Var mappability check. */
typedef struct
{
    int varno_keep;
    int varno_drop;
    int col_map_keep[64];
    int col_map_drop[64];
} VarCheckCtx;

static void tm_post_parse_analyze(ParseState *pstate, Query *query,
                                   JumbleState *jstate);
static bool tm_match_query(Query *query, MVRegistryEntry *out_entry,
                            int matched_rte_indexes[MV_MAX_TABLES]);
static void tm_rewrite_query(Query *query, const MVRegistryEntry *entry,
                              int matched_rte_indexes[MV_MAX_TABLES]);

static void tm_build_join_col_map(Oid mv_oid, Oid src_oid0, Oid src_oid1,
                                   int map0[64], int map1[64]);
static bool tm_find_equijoin(Node *quals, int varno0, AttrNumber attno0,
                              int varno1, AttrNumber attno1);
static bool tm_verify_join_condition(Query *query, const MVRegistryEntry *entry,
                                      int matched_rte_indexes[MV_MAX_TABLES]);
static bool tm_has_unmapped_var(Node *node, void *context);
static Node *tm_rewrite_var_mutator(Node *node, void *ctx);
static void tm_rewrite_join_query(Query *query, const MVRegistryEntry *entry,
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
        /* Registry starts empty; populated by the BGW / force_spawn_mvs(). */
        memset(mv_registry_state, 0, sizeof(MVRegistryState));
        mv_registry_state->num_entries = mv_seed_count; /* always 0 */
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
    int              best_idx        = -1;
    int              best_src_count  = 0;
    int              best_indexes[MV_MAX_TABLES];

    if (mv_registry_state == NULL)
        return false;

    /* Take a local copy under the shared lock so we don't hold it long. */
    LWLockAcquire(&top_queries_locks[1].lock, LW_SHARED);
    num_entries = mv_registry_state->num_entries;
    if (num_entries > 0)
        memcpy(registry_snapshot, mv_registry_state->entries,
               num_entries * sizeof(MVRegistryEntry));
    LWLockRelease(&top_queries_locks[1].lock);

    /*
     * Scan all entries and pick the one whose source-table set is the largest
     * superset of the query's rtable.  This ensures a 2-table join entry beats
     * a 1-table entry that happens to match one of the joined tables.
     */
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

        if (found_count == entry->num_source_tables &&
            found_count > best_src_count)
        {
            best_idx       = e;
            best_src_count = found_count;
            memcpy(best_indexes, found_indexes, found_count * sizeof(int));
        }
    }

    if (best_idx >= 0)
    {
        memcpy(matched_rte_indexes, best_indexes,
               best_src_count * sizeof(int));
        memcpy(out_entry, &registry_snapshot[best_idx], sizeof(MVRegistryEntry));
        return true;
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
        tm_rewrite_join_query(query, entry, matched_rte_indexes);
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
 * Join rewrite helpers
 * ---------------------------------------------------------------- */

/*
 * Build per-table column maps: col_map[t][src_attno] = mv_attno.
 * Uses catalog functions — no SPI, safe to call in the parse hook.
 * Entries that cannot be matched (e.g. aliased columns) remain 0.
 */
static void
tm_build_join_col_map(Oid mv_oid, Oid src_oid0, Oid src_oid1,
                      int map0[64], int map1[64])
{
    HeapTuple   reltup;
    int         mv_natts;
    int         i;

    memset(map0, 0, 64 * sizeof(int));
    memset(map1, 0, 64 * sizeof(int));

    reltup = SearchSysCache1(RELOID, ObjectIdGetDatum(mv_oid));
    if (!HeapTupleIsValid(reltup))
        return;
    mv_natts = ((Form_pg_class) GETSTRUCT(reltup))->relnatts;
    ReleaseSysCache(reltup);

    for (i = 1; i <= mv_natts; i++)
    {
        char       *mv_attname;
        AttrNumber  src_attno;

        mv_attname = get_attname(mv_oid, i, true /* missing_ok */);
        if (mv_attname == NULL)
            continue;   /* dropped column */

        src_attno = get_attnum(src_oid0, mv_attname);
        if (AttributeNumberIsValid(src_attno) && src_attno > 0 && src_attno < 64)
            map0[src_attno] = i;

        src_attno = get_attnum(src_oid1, mv_attname);
        if (AttributeNumberIsValid(src_attno) && src_attno > 0 && src_attno < 64)
            map1[src_attno] = i;

        pfree(mv_attname);
    }
}

/*
 * Recursively search a qual tree for an equijoin condition of the form
 *   Var(varno0, attno0) = Var(varno1, attno1)   (or commuted).
 * Only walks AND conjuncts and flat Lists; stops at first match.
 */
static bool
tm_find_equijoin(Node *quals, int varno0, AttrNumber attno0,
                 int varno1, AttrNumber attno1)
{
    if (quals == NULL)
        return false;

    if (IsA(quals, List))
    {
        ListCell *lc;

        foreach(lc, (List *) quals)
        {
            if (tm_find_equijoin((Node *) lfirst(lc), varno0, attno0,
                                 varno1, attno1))
                return true;
        }
        return false;
    }

    if (IsA(quals, BoolExpr))
    {
        BoolExpr *bexpr = (BoolExpr *) quals;

        if (bexpr->boolop == AND_EXPR)
        {
            ListCell *lc;

            foreach(lc, bexpr->args)
            {
                if (tm_find_equijoin((Node *) lfirst(lc), varno0, attno0,
                                     varno1, attno1))
                    return true;
            }
        }
        return false;
    }

    if (IsA(quals, OpExpr))
    {
        OpExpr    *opexpr = (OpExpr *) quals;
        HeapTuple  optup;
        bool       is_eq = false;
        Var       *lvar,
                  *rvar;

        if (list_length(opexpr->args) != 2)
            return false;

        /* Verify the operator is "=" */
        optup = SearchSysCache1(OPEROID, ObjectIdGetDatum(opexpr->opno));
        if (HeapTupleIsValid(optup))
        {
            Form_pg_operator opform = (Form_pg_operator) GETSTRUCT(optup);

            is_eq = (strncmp(NameStr(opform->oprname), "=", 2) == 0);
            ReleaseSysCache(optup);
        }
        if (!is_eq)
            return false;

        if (!IsA(linitial(opexpr->args), Var) ||
            !IsA(lsecond(opexpr->args), Var))
            return false;

        lvar = (Var *) linitial(opexpr->args);
        rvar = (Var *) lsecond(opexpr->args);

        /* Check normal and commuted forms */
        if (lvar->varno == varno0 && lvar->varattno == attno0 &&
            rvar->varno == varno1 && rvar->varattno == attno1)
            return true;
        if (lvar->varno == varno1 && lvar->varattno == attno1 &&
            rvar->varno == varno0 && rvar->varattno == attno0)
            return true;
    }

    return false;
}

/*
 * Verify that the query contains an equijoin condition matching each
 * join_cols[i] for the registry entry.  Searches both JoinExpr.quals
 * (explicit JOIN ... ON) and FromExpr.quals (implicit comma join + WHERE).
 * Returns false if any required condition is absent — rewrite is skipped.
 */
static bool
tm_verify_join_condition(Query *query, const MVRegistryEntry *entry,
                          int matched_rte_indexes[MV_MAX_TABLES])
{
    int joins_to_check = entry->num_source_tables - 1;
    int i;

    for (i = 0; i < joins_to_check; i++)
    {
        RangeTblEntry *rte0,
                      *rte1;
        AttrNumber     attno0,
                       attno1;
        int            varno0,
                       varno1;
        bool           found = false;
        ListCell      *lc;

        rte0   = (RangeTblEntry *) list_nth(query->rtable,
                                            matched_rte_indexes[i]);
        rte1   = (RangeTblEntry *) list_nth(query->rtable,
                                            matched_rte_indexes[i + 1]);
        varno0 = matched_rte_indexes[i] + 1;     /* 1-based varno */
        varno1 = matched_rte_indexes[i + 1] + 1;

        attno0 = get_attnum(rte0->relid, entry->join_cols[i].t0_col);
        attno1 = get_attnum(rte1->relid, entry->join_cols[i].t1_col);

        if (!AttributeNumberIsValid(attno0) || !AttributeNumberIsValid(attno1))
            return false;

        /* Check FromExpr.quals (implicit join / top-level WHERE) */
        if (tm_find_equijoin(query->jointree->quals,
                             varno0, attno0, varno1, attno1))
            found = true;

        /* Check JoinExpr.quals for each explicit JOIN in fromlist */
        if (!found)
        {
            foreach(lc, query->jointree->fromlist)
            {
                Node *node = (Node *) lfirst(lc);

                if (IsA(node, JoinExpr))
                {
                    JoinExpr *je = (JoinExpr *) node;

                    if (tm_find_equijoin(je->quals,
                                        varno0, attno0, varno1, attno1))
                    {
                        found = true;
                        break;
                    }
                }
            }
        }

        if (!found)
            return false;
    }

    return true;
}

/*
 * Expression walker: returns true if any Var referencing varno_keep or
 * varno_drop has no valid column-map entry (col_map == 0 or attno >= 64).
 * Used as a pre-mutation safety check to prevent dangling Var references.
 */
static bool
tm_has_unmapped_var(Node *node, void *context)
{
    VarCheckCtx *ctx = (VarCheckCtx *) context;

    if (node == NULL)
        return false;

    if (IsA(node, Var))
    {
        Var *var = (Var *) node;

        /*
         * Whole-row and system-column Vars on the dropped RTE cannot be
         * remapped: the mutator leaves them unchanged, but varno_drop's RTE
         * will no longer appear in the jointree after rewrite.
         */
        if (var->varno == ctx->varno_drop && var->varattno <= 0)
            return true;

        if (var->varattno <= 0)
            return false;   /* system cols on varno_keep: RTE stays, safe */

        if (var->varno == ctx->varno_keep)
        {
            if (var->varattno >= 64 || ctx->col_map_keep[var->varattno] == 0)
                return true;
        }
        else if (var->varno == ctx->varno_drop)
        {
            if (var->varattno >= 64 || ctx->col_map_drop[var->varattno] == 0)
                return true;
        }

        return false;
    }

    return expression_tree_walker(node, tm_has_unmapped_var, context);
}

/*
 * Expression mutator: rewrite Var nodes from the dropped source-table RTE
 * to the kept (IMMV) RTE, applying the column map.  Also remaps kept-RTE
 * Vars when the IMMV column layout differs from source_tables[0].
 */
static Node *
tm_rewrite_var_mutator(Node *node, void *context)
{
    VarRewriteCtx *ctx = (VarRewriteCtx *) context;

    if (node == NULL)
        return NULL;

    if (IsA(node, Var))
    {
        Var *var = (Var *) node;

        if (var->varno == ctx->varno_drop)
        {
            Var *newvar;

            if (var->varattno <= 0)
            {
                /* System column or whole-row ref on the dropped table. */
                if (var->varattno == 0)
                    ereport(WARNING,
                            (errmsg("table_materializer: whole-row Var on "
                                    "dropped source table varno %d during "
                                    "join rewrite — skipping remapping",
                                    ctx->varno_drop)));
                return (Node *) var;   /* leave as-is; safe: not in jointree */
            }

            if (var->varattno >= 64 || ctx->col_map_drop[var->varattno] == 0)
            {
                ereport(DEBUG1,
                        (errmsg("table_materializer: no col_map entry for "
                                "varno %d attno %d during join rewrite",
                                ctx->varno_drop, var->varattno)));
                return (Node *) var;
            }

            newvar           = copyObject(var);
            newvar->varno    = ctx->varno_keep;
            newvar->varattno = ctx->col_map_drop[var->varattno];
            return (Node *) newvar;
        }

        if (var->varno == ctx->varno_keep && ctx->has_map &&
            var->varattno > 0 && var->varattno < 64 &&
            ctx->col_map_keep[var->varattno] != 0)
        {
            Var *newvar    = copyObject(var);

            newvar->varattno = ctx->col_map_keep[var->varattno];
            return (Node *) newvar;
        }

        return (Node *) var;
    }

    return expression_tree_mutator(node, tm_rewrite_var_mutator, ctx);
}

/*
 * Rewrite a join query in-place to use the matched IMMV.
 *
 * Strategy:
 *   1. Verify the query's join predicate matches the IMMV's baked-in
 *      condition (fail-closed: skip rewrite if uncertain).
 *   2. Modify RTE[matched[0]] in-place to point at the IMMV (mirrors the
 *      single-table path — keeps the varno stable).
 *   3. Remove RTE[matched[1]] from the jointree fromlist (it stays in
 *      query->rtable as a dead permission-check entry).
 *   4. Remap all Var nodes referencing either source table to the IMMV RTE,
 *      using a column map built from the catalog.
 */
static void
tm_rewrite_join_query(Query *query, const MVRegistryEntry *entry,
                      int matched_rte_indexes[MV_MAX_TABLES])
{
    RangeVar       rv;
    Oid            mv_oid;
    RangeTblEntry *rte0,
                  *rte1;
    Oid            src_oid0,
                   src_oid1;
    VarRewriteCtx  ctx;
    JoinExpr      *found_je;      /* non-NULL if explicit JoinExpr shape */
    ListCell      *lc;
    bool           fixed_fromlist;

    /* --- Step 1: resolve IMMV OID --- */
    memset(&rv, 0, sizeof(rv));
    rv.type           = T_RangeVar;
    rv.schemaname     = (char *) entry->mv_schema;
    rv.relname        = (char *) entry->mv_name;
    rv.inh            = false;
    rv.relpersistence = RELPERSISTENCE_PERMANENT;
    rv.location       = -1;

    mv_oid = RangeVarGetRelid(&rv, NoLock, true /* missing_ok */);
    if (!OidIsValid(mv_oid))
    {
        ereport(DEBUG1,
                (errmsg("table_materializer: join MV \"%s\".\"%s\" not found, "
                        "skipping rewrite",
                        entry->mv_schema, entry->mv_name)));
        return;
    }

    /* --- Step 2: capture source OIDs before modifying any RTE --- */
    rte0     = (RangeTblEntry *) list_nth(query->rtable,
                                          matched_rte_indexes[0]);
    rte1     = (RangeTblEntry *) list_nth(query->rtable,
                                          matched_rte_indexes[1]);
    src_oid0 = rte0->relid;
    src_oid1 = rte1->relid;

    /* --- Step 3: verify join predicate matches IMMV definition --- */
    if (!tm_verify_join_condition(query, entry, matched_rte_indexes))
    {
        ereport(DEBUG1,
                (errmsg("table_materializer: join predicate not verified for "
                        "MV \"%s\".\"%s\", skipping rewrite",
                        entry->mv_schema, entry->mv_name)));
        return;
    }

    /* --- Step 4: build column maps from catalog --- */
    tm_build_join_col_map(mv_oid, src_oid0, src_oid1,
                          ctx.col_map_keep, ctx.col_map_drop);
    ctx.varno_keep = matched_rte_indexes[0] + 1;   /* 1-based */
    ctx.varno_drop = matched_rte_indexes[1] + 1;
    ctx.has_map    = true;

    /* --- Pre-mutation check A: validate jointree shape ---
     * We only support two shapes:
     *   (a) A single JoinExpr whose direct larg/rarg are both RangeTblRefs
     *       matching varno_keep and varno_drop.
     *   (b) Two top-level RangeTblRefs (implicit comma join) where one has
     *       rtindex == varno_keep and the other == varno_drop.
     * Bail before any mutation if neither shape is present — prevents
     * silent wrong-answer bugs on 3-way joins or nested JoinExprs.
     * found_je is set non-NULL for shape (a); it is used by check A2 below.
     */
    found_je = NULL;
    {
        bool shape_ok = false;

        foreach(lc, query->jointree->fromlist)
        {
            Node *fnode = (Node *) lfirst(lc);

            if (IsA(fnode, JoinExpr))
            {
                JoinExpr    *je = (JoinExpr *) fnode;
                RangeTblRef *lr = NULL,
                            *rr = NULL;

                if (IsA(je->larg, RangeTblRef))
                    lr = (RangeTblRef *) je->larg;
                if (IsA(je->rarg, RangeTblRef))
                    rr = (RangeTblRef *) je->rarg;

                if (lr && rr &&
                    ((lr->rtindex == ctx.varno_keep &&
                      rr->rtindex == ctx.varno_drop) ||
                     (lr->rtindex == ctx.varno_drop &&
                      rr->rtindex == ctx.varno_keep)))
                {
                    found_je  = je;
                    shape_ok  = true;
                    break;
                }
            }
        }

        if (!shape_ok)
        {
            /* Try implicit-join shape: two top-level RangeTblRefs */
            bool has_keep = false,
                 has_drop = false;

            foreach(lc, query->jointree->fromlist)
            {
                Node *fnode = (Node *) lfirst(lc);

                if (IsA(fnode, RangeTblRef))
                {
                    int idx = ((RangeTblRef *) fnode)->rtindex;

                    if (idx == ctx.varno_keep)
                        has_keep = true;
                    else if (idx == ctx.varno_drop)
                        has_drop = true;
                }
            }

            if (has_keep && has_drop)
                shape_ok = true;
        }

        if (!shape_ok)
        {
            ereport(DEBUG1,
                    (errmsg("table_materializer: unsupported jointree shape "
                            "for MV \"%s\".\"%s\", skipping join rewrite",
                            entry->mv_schema, entry->mv_name)));
            return;
        }
    }

    /* --- Pre-mutation check A2: no extra quals in the JoinExpr ---
     * When shape (a) is used, replacing the JoinExpr with a bare RangeTblRef
     * discards je->quals.  That is safe only if je->quals is NULL (the
     * equijoin was in the WHERE clause) or is exactly the one OpExpr we
     * already verified via tm_verify_join_condition.  Any additional
     * conjuncts would be silently dropped → wrong results.
     * We bail conservatively if the quals are not a single OpExpr matching
     * our registered equijoin.
     */
    if (found_je != NULL && found_je->quals != NULL)
    {
        AttrNumber je_attno0 = get_attnum(src_oid0, entry->join_cols[0].t0_col);
        AttrNumber je_attno1 = get_attnum(src_oid1, entry->join_cols[0].t1_col);

        if (!IsA(found_je->quals, OpExpr) ||
            !AttributeNumberIsValid(je_attno0) ||
            !AttributeNumberIsValid(je_attno1) ||
            !tm_find_equijoin(found_je->quals,
                              ctx.varno_keep, je_attno0,
                              ctx.varno_drop, je_attno1))
        {
            ereport(DEBUG1,
                    (errmsg("table_materializer: JoinExpr has extra or "
                            "unrecognized quals for MV \"%s\".\"%s\", "
                            "skipping join rewrite",
                            entry->mv_schema, entry->mv_name)));
            return;
        }
    }

    /* --- Pre-mutation check B: verify all Vars have valid col_map entries ---
     * If any Var referencing either source table has no map entry (e.g. an
     * aliased column in the IMMV), the post-mutation Var would be dangling.
     * Bail before touching the RTE.
     */
    {
        VarCheckCtx vcheck;

        vcheck.varno_keep = ctx.varno_keep;
        vcheck.varno_drop = ctx.varno_drop;
        memcpy(vcheck.col_map_keep, ctx.col_map_keep,
               sizeof(vcheck.col_map_keep));
        memcpy(vcheck.col_map_drop, ctx.col_map_drop,
               sizeof(vcheck.col_map_drop));

        if (expression_tree_walker((Node *) query->targetList,
                                   tm_has_unmapped_var, &vcheck) ||
            expression_tree_walker(query->jointree->quals,
                                   tm_has_unmapped_var, &vcheck) ||
            (query->havingQual &&
             expression_tree_walker(query->havingQual,
                                    tm_has_unmapped_var, &vcheck)))
        {
            ereport(DEBUG1,
                    (errmsg("table_materializer: column map incomplete for "
                            "MV \"%s\".\"%s\" (aliased columns?), "
                            "skipping join rewrite",
                            entry->mv_schema, entry->mv_name)));
            return;
        }
    }

    /* --- Step 5: modify RTE[matched[0]] in-place to point at the IMMV --- */
    {
        RTEPermissionInfo *perminfo = NULL;

        if (rte0->perminfoindex > 0)
            perminfo = getRTEPermissionInfo(query->rteperminfos, rte0);

        rte0->relid       = mv_oid;
        rte0->relkind     = RELKIND_MATVIEW;
        rte0->inh         = false;
        rte0->tablesample = NULL;

        if (perminfo != NULL)
        {
            perminfo->relid = mv_oid;
            perminfo->inh   = false;
        }
    }

    if (rte0->eref != NULL)
        rte0->eref->aliasname = pstrdup(entry->mv_name);

    /* --- Step 6: fix jointree — remove dropped-table refs / collapse JoinExpr --- */
    fixed_fromlist = false;

    /* Handle explicit JoinExpr: replace entire JoinExpr with RangeTblRef */
    foreach(lc, query->jointree->fromlist)
    {
        Node *node = (Node *) lfirst(lc);

        if (IsA(node, JoinExpr))
        {
            JoinExpr    *je     = (JoinExpr *) node;
            RangeTblRef *lref   = NULL,
                        *rref   = NULL;

            if (IsA(je->larg, RangeTblRef))
                lref = (RangeTblRef *) je->larg;
            if (IsA(je->rarg, RangeTblRef))
                rref = (RangeTblRef *) je->rarg;

            if (lref && rref &&
                ((lref->rtindex == ctx.varno_keep &&
                  rref->rtindex == ctx.varno_drop) ||
                 (lref->rtindex == ctx.varno_drop &&
                  rref->rtindex == ctx.varno_keep)))
            {
                RangeTblRef *mvref = makeNode(RangeTblRef);

                mvref->rtindex = ctx.varno_keep;
                lfirst(lc)     = mvref;
                fixed_fromlist = true;
                break;
            }
        }
    }

    /* Handle implicit join: find and remove the dropped-table RangeTblRef */
    if (!fixed_fromlist)
    {
        foreach(lc, query->jointree->fromlist)
        {
            Node *node = (Node *) lfirst(lc);

            if (IsA(node, RangeTblRef) &&
                ((RangeTblRef *) node)->rtindex == ctx.varno_drop)
            {
                query->jointree->fromlist =
                    list_delete_ptr(query->jointree->fromlist, node);
                fixed_fromlist = true;
                break;
            }
        }
    }

    /* --- Step 7: remap Var nodes throughout the query expressions --- */
    query->targetList =
        (List *) tm_rewrite_var_mutator((Node *) query->targetList, &ctx);
    query->jointree->quals =
        tm_rewrite_var_mutator(query->jointree->quals, &ctx);
    if (query->havingQual)
        query->havingQual =
            tm_rewrite_var_mutator(query->havingQual, &ctx);
    if (query->returningList)
        query->returningList =
            (List *) tm_rewrite_var_mutator((Node *) query->returningList,
                                            &ctx);

    ereport(DEBUG1,
            (errmsg("table_materializer: rewrote join query to use MV "
                    "\"%s\".\"%s\"",
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
// #define HEURISTIC_SCORE_EXPR  "mean_exec_time * calls"
#define HEURISTIC_SCORE_EXPR  "rows"

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
