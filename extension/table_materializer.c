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
#include "catalog/pg_attribute.h"
#include "nodes/nodeFuncs.h"
#include "nodes/value.h"
#include "utils/syscache.h"
#include "utils/memutils.h"
#include "lib/stringinfo.h"
#include "parser/parser.h"

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

#define MV_MAX_TABLES   8
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
     * Per-added-table join links.  join_links[i] (for i in 1 ..
     * num_source_tables-1) describes how source_tables[i] attaches to an
     * already-present table.  The benchmark joins are not strictly i<->i+1
     * (e.g. "p.id = oi.product_id" links a later table back to table 0), so
     * each link records which earlier table it joins against.
     *   left_idx  = index (into source_tables) of the already-present side
     *   left_col  = equijoin column on source_tables[left_idx]
     *   right_col = equijoin column on source_tables[i] (the newly added table)
     *   join_type = JOIN_INNER or JOIN_LEFT (PostgreSQL JoinType enum value)
     * join_links[0] is unused (table 0 is the chain root).
     * Used by tm_verify_join_to confirm the incoming query's join matches what
     * the IMMV was built on (fail-closed safety check).
     */
    struct {
        int  left_idx;
        char left_col[MV_NAME_LEN];
        char right_col[MV_NAME_LEN];
        int  join_type;
    } join_links[MV_MAX_TABLES];

    /*
     * Column mapping for join rewrites, populated at IMMV creation time.
     * col_map[table_idx][src_attno] = mv_attno (the attribute number of the
     * column in the IMMV that came from source_tables[table_idx].src_attno).
     * A zero entry means "this source column is not present in the IMMV".
     * has_col_map == false means the MV has an identical column layout to the
     * source table — no remapping needed (single-table path only).
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
 * detects expensive join chains and single hot tables, creates IMMVs via
 * pg_ivm, and registers each one here so the post_parse_analyze hook can
 * rewrite matching queries.
 *
 * NOTE: do not add hand-written seed entries here.  The rewrite hook tries
 * candidate entries largest-first and verifies (fail-closed) before mutating,
 * but a seed entry whose backing IMMV does not exist on disk still wastes
 * verification work on every parse.  Entries belong here only once their
 * IMMV has actually been created.
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
static int  tm_create_join_mvs(int budget);
static void select_and_create_mvs(void);

/*
 * Context for the Var-remapping expression mutator used in join rewrites.
 * All matched source-table RTEs collapse onto a single IMMV RTE (varno_keep,
 * = the RTE of source_tables[0]).  varno_of_table[t] is the query varno that
 * matched source_tables[t]; a Var on any of those varnos is rewritten to
 * varno_keep with its attno remapped via col_map[t].
 */
typedef struct
{
    int  varno_keep;                    /* IMMV varno (matched source_tables[0]) */
    int  n_tables;                      /* entry->num_source_tables              */
    int  varno_of_table[MV_MAX_TABLES]; /* query varno matched to source_tables[t] */
    int  (*col_map)[64];                /* aliased to entry->col_map             */
} VarRewriteCtx;

/* Read-only context for the pre-mutation Var mappability check. */
typedef struct
{
    int varno_keep;
    int n_tables;
    int varno_of_table[MV_MAX_TABLES];
    int (*col_map)[64];
} VarCheckCtx;

static void tm_post_parse_analyze(ParseState *pstate, Query *query,
                                   JumbleState *jstate);
static bool tm_entry_tables_present(Query *query, const MVRegistryEntry *entry,
                                    int matched_rte_indexes[MV_MAX_TABLES]);
static void tm_rewrite_single(Query *query, const MVRegistryEntry *entry,
                              int matched_rte_indexes[MV_MAX_TABLES]);

static bool tm_find_equijoin(Node *quals, int varno0, AttrNumber attno0,
                              int varno1, AttrNumber attno1);
static bool tm_has_unmapped_var(Node *node, void *context);
static Node *tm_rewrite_var_mutator(Node *node, void *ctx);
static bool tm_try_rewrite_join(Query *query, const MVRegistryEntry *entry,
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
    MVRegistryEntry registry_snapshot[MV_REGISTRY_MAX];
    int             num_entries;
    int             max_src;
    int             want;

    if (prev_post_parse_analyze)
        prev_post_parse_analyze(pstate, query, jstate);

    if (query->commandType != CMD_SELECT)
        return;

    if (query->rtable == NIL)
        return;

    if (mv_registry_state == NULL)
        return;

    /* Take a local copy under the shared lock so we don't hold it long. */
    LWLockAcquire(&top_queries_locks[1].lock, LW_SHARED);
    num_entries = mv_registry_state->num_entries;
    if (num_entries > 0)
        memcpy(registry_snapshot, mv_registry_state->entries,
               num_entries * sizeof(MVRegistryEntry));
    LWLockRelease(&top_queries_locks[1].lock);

    if (num_entries <= 0)
        return;

    /*
     * Prefer the entry whose source-table set is the largest superset of the
     * query's tables (a join MV beats a single-table MV covering one of its
     * tables).  We iterate by decreasing num_source_tables and try each
     * candidate verify-then-apply: join rewrites can fail their fail-closed
     * checks, in which case we fall through to a smaller entry (down to a
     * single-table mirror) rather than giving up.
     */
    max_src = 0;
    for (int e = 0; e < num_entries; e++)
        if (registry_snapshot[e].num_source_tables > max_src)
            max_src = registry_snapshot[e].num_source_tables;

    for (want = max_src; want >= 1; want--)
    {
        for (int e = 0; e < num_entries; e++)
        {
            const MVRegistryEntry *entry = &registry_snapshot[e];
            int matched_rte_indexes[MV_MAX_TABLES];

            if (entry->num_source_tables != want)
                continue;

            if (!tm_entry_tables_present(query, entry, matched_rte_indexes))
                continue;

            if (entry->num_source_tables >= 2)
            {
                if (tm_try_rewrite_join(query, entry, matched_rte_indexes))
                    return;
                /* verification failed — try the next candidate */
            }
            else
            {
                tm_rewrite_single(query, entry, matched_rte_indexes);
                return;
            }
        }
    }
}

/*
 * Return true if every source table of *entry is present in query->rtable as
 * an RTE_RELATION.  Fills matched_rte_indexes[s] with the 0-based rtable index
 * matched to source_tables[s].
 */
static bool
tm_entry_tables_present(Query *query, const MVRegistryEntry *entry,
                        int matched_rte_indexes[MV_MAX_TABLES])
{
    int s;

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
                    matched_rte_indexes[s] = rte_idx;
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

        if (!table_found)
            return false;
    }

    return true;
}

/*
 * Single-table rewrite: replace the matched source table's relid/relkind in
 * its RTE so the query reads from the IMMV mirror instead of the base table.
 */
static void
tm_rewrite_single(Query *query, const MVRegistryEntry *entry,
                  int matched_rte_indexes[MV_MAX_TABLES])
{
    RangeTblEntry *rte;
    RangeVar       rv;
    Oid            mv_oid;

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
 * Given a matched query varno, return the source-table index t such that
 * ctx->varno_of_table[t] == varno, or -1 if varno is not one of the matched
 * source tables (e.g. an unrelated/extra table left in the query).
 */
static inline int
tm_table_idx_for_varno(const VarRewriteCtx *ctx, int varno)
{
    int t;

    for (t = 0; t < ctx->n_tables; t++)
        if (ctx->varno_of_table[t] == varno)
            return t;
    return -1;
}

/*
 * Expression walker: returns true if any Var referencing one of the matched
 * source tables has no valid column-map entry (col_map == 0, attno out of
 * range, or a system/whole-row Var on a table that will be dropped from the
 * jointree).  Used as a pre-mutation safety check to prevent dangling Vars.
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
        int  t;

        for (t = 0; t < ctx->n_tables; t++)
            if (ctx->varno_of_table[t] == var->varno)
                break;
        if (t == ctx->n_tables)
            return false;   /* not a matched table — left untouched, safe */

        /*
         * Whole-row / system-column Vars on a dropped table cannot be
         * remapped (the mutator leaves them unchanged, but the RTE will no
         * longer appear in the jointree).  System cols on the kept RTE (t==0)
         * are safe because that RTE survives, repointed at the IMMV.
         */
        if (var->varattno <= 0)
            return (ctx->varno_of_table[t] != ctx->varno_keep);

        if (var->varattno >= 64 || ctx->col_map[t][var->varattno] == 0)
            return true;

        return false;
    }

    return expression_tree_walker(node, tm_has_unmapped_var, context);
}

/*
 * Expression mutator: rewrite every Var referencing a matched source-table
 * RTE to the kept (IMMV) RTE, applying that table's column map.  This also
 * remaps Vars on the kept table itself, because the IMMV column layout differs
 * from source_tables[0].
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
        int  t   = tm_table_idx_for_varno(ctx, var->varno);
        Var *newvar;

        if (t < 0)
            return (Node *) var;   /* unrelated table: leave as-is */

        if (var->varattno <= 0)
        {
            /* System column or whole-row ref; pre-check guarantees these only
             * survive on the kept table, whose RTE remains valid. */
            if (var->varattno == 0 && ctx->varno_of_table[t] != ctx->varno_keep)
                ereport(WARNING,
                        (errmsg("table_materializer: whole-row Var on dropped "
                                "source table varno %d during join rewrite",
                                var->varno)));
            return (Node *) var;
        }

        if (var->varattno >= 64 || ctx->col_map[t][var->varattno] == 0)
        {
            ereport(DEBUG1,
                    (errmsg("table_materializer: no col_map entry for varno %d "
                            "attno %d during join rewrite",
                            var->varno, var->varattno)));
            return (Node *) var;
        }

        newvar           = copyObject(var);
        newvar->varno    = ctx->varno_keep;
        newvar->varattno = ctx->col_map[t][var->varattno];
        return (Node *) newvar;
    }

    return expression_tree_mutator(node, tm_rewrite_var_mutator, ctx);
}

/*
 * Flatten a strictly left-deep JoinExpr tree.  On success fills:
 *   order[k]  = rtindex of the base relation at chain position k
 *   jtype[k]  = JoinType of the JoinExpr that added order[k]  (k >= 1)
 *   jquals[k] = quals (ON clause) of that JoinExpr             (k >= 1)
 * and sets *nout to the number of base relations.  order[0] is the deepest-
 * left leaf (jtype[0]/jquals[0] are unused).
 *
 * Returns false (no partial state guaranteed) for any non-left-deep shape:
 * a JoinExpr whose rarg is not a base RangeTblRef, a bushy tree, or overflow.
 */
static bool
tm_flatten_leftdeep(Node *node, int order[], int jtype[], Node *jquals[],
                    int *nout, int cap)
{
    JoinExpr *je;
    int       k;

    if (node == NULL)
        return false;

    if (IsA(node, RangeTblRef))
    {
        if (cap < 1)
            return false;
        order[0] = ((RangeTblRef *) node)->rtindex;
        *nout    = 1;
        return true;
    }

    if (!IsA(node, JoinExpr))
        return false;

    je = (JoinExpr *) node;

    /* Left-deep: rarg must be a base relation. */
    if (!IsA(je->rarg, RangeTblRef))
        return false;

    if (!tm_flatten_leftdeep(je->larg, order, jtype, jquals, &k, cap))
        return false;

    if (k >= cap)
        return false;

    order[k]  = ((RangeTblRef *) je->rarg)->rtindex;
    jtype[k]  = (int) je->jointype;
    jquals[k] = je->quals;
    *nout     = k + 1;
    return true;
}

/*
 * Reset the jointype of every RTE_JOIN in a collapsed JoinExpr subtree to
 * JOIN_INNER.  The planner detects outer joins by scanning the range table
 * for RTE_JOIN entries with an outer jointype; once we have collapsed the
 * JoinExpr out of the jointree those entries are unreferenced, but a leftover
 * outer jointype makes reduce_outer_joins trip on "so where are the outer
 * joins?".  Neutralizing the (now dead) join RTEs avoids that.
 */
static void
tm_neutralize_join_rtes(Node *node, List *rtable)
{
    JoinExpr *je;

    if (node == NULL || !IsA(node, JoinExpr))
        return;

    je = (JoinExpr *) node;
    if (je->rtindex > 0)
    {
        RangeTblEntry *jrte = (RangeTblEntry *) list_nth(rtable,
                                                         je->rtindex - 1);
        jrte->jointype = JOIN_INNER;
    }
    tm_neutralize_join_rtes(je->larg, rtable);
    tm_neutralize_join_rtes(je->rarg, rtable);
}

/*
 * Try to rewrite an N-table join query in-place to read from the matched
 * IMMV.  ALL validation happens before any mutation, and the function returns
 * false (leaving the query untouched) on any uncertainty, so the caller can
 * fall through to another registry entry.  Returns true once the rewrite is
 * applied.
 *
 * Two jointree shapes are supported:
 *   (A) The whole FROM is one strictly left-deep JoinExpr tree of exactly the
 *       matched tables, in the same order the IMMV was built on, with each
 *       JoinExpr's type matching the registered link type (inner→inner,
 *       left→left with the newly-added table on the nullable side) and its ON
 *       clause being exactly the registered equijoin.  Supports inner & left.
 *   (B) The matched tables appear as top-level RangeTblRefs (implicit comma
 *       join) with the equijoins in WHERE — inner joins only (a comma join is
 *       always inner).  Extra unrelated tables may coexist.
 */
static bool
tm_try_rewrite_join(Query *query, const MVRegistryEntry *entry,
                    int matched_rte_indexes[MV_MAX_TABLES])
{
    RangeVar       rv;
    Oid            mv_oid;
    VarRewriteCtx  ctx;
    int            n = entry->num_source_tables;
    int            i;
    ListCell      *lc;
    Node          *join_fromlist_node = NULL;   /* shape (A): the top JoinExpr */
    bool           shape_a = false;

    /* --- resolve IMMV OID --- */
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
                        "skipping rewrite", entry->mv_schema, entry->mv_name)));
        return false;
    }

    /* --- build the remapping context --- */
    ctx.varno_keep = matched_rte_indexes[0] + 1;   /* 1-based */
    ctx.n_tables   = n;
    ctx.col_map    = (int (*)[64]) entry->col_map;
    for (i = 0; i < n; i++)
        ctx.varno_of_table[i] = matched_rte_indexes[i] + 1;

    /*
     * --- Shape detection + per-link verification ---
     * Shape (A): the FROM clause is exactly one left-deep JoinExpr.
     */
    if (list_length(query->jointree->fromlist) == 1 &&
        IsA((Node *) linitial(query->jointree->fromlist), JoinExpr))
    {
        int   order[MV_MAX_TABLES];
        int   jtype[MV_MAX_TABLES];
        Node *jquals[MV_MAX_TABLES];
        int   nout = 0;

        join_fromlist_node = (Node *) linitial(query->jointree->fromlist);

        if (tm_flatten_leftdeep(join_fromlist_node, order, jtype, jquals,
                                &nout, MV_MAX_TABLES) &&
            nout == n)
        {
            bool ok = true;

            /* Join order must match the registered source-table order. */
            for (i = 0; i < n && ok; i++)
                if (order[i] != ctx.varno_of_table[i])
                    ok = false;

            /* Each link: matching join type + exactly the registered equijoin
             * as the ON clause (a bare OpExpr — no extra conjuncts to drop). */
            for (i = 1; i < n && ok; i++)
            {
                int            li      = entry->join_links[i].left_idx;
                RangeTblEntry *rte_r   = (RangeTblEntry *)
                    list_nth(query->rtable, matched_rte_indexes[i]);
                RangeTblEntry *rte_l   = (RangeTblEntry *)
                    list_nth(query->rtable, matched_rte_indexes[li]);
                AttrNumber     attno_r = get_attnum(rte_r->relid,
                                            entry->join_links[i].right_col);
                AttrNumber     attno_l = get_attnum(rte_l->relid,
                                            entry->join_links[i].left_col);

                if (li < 0 || li >= i ||
                    jtype[i] != entry->join_links[i].join_type ||
                    !AttributeNumberIsValid(attno_r) ||
                    !AttributeNumberIsValid(attno_l) ||
                    jquals[i] == NULL || !IsA(jquals[i], OpExpr) ||
                    !tm_find_equijoin(jquals[i],
                                      ctx.varno_of_table[li], attno_l,
                                      ctx.varno_of_table[i], attno_r))
                    ok = false;
            }

            shape_a = ok;
        }
    }

    /*
     * Shape (B): implicit comma join.  Every matched table appears as a
     * top-level RangeTblRef and every registered link is an inner join whose
     * equijoin is present in the WHERE clause.
     */
    if (!shape_a)
    {
        bool all_inner = true;
        bool all_present = true;

        for (i = 1; i < n; i++)
            if (entry->join_links[i].join_type != JOIN_INNER)
                all_inner = false;

        for (i = 0; i < n && all_present; i++)
        {
            bool found = false;

            foreach(lc, query->jointree->fromlist)
            {
                Node *fnode = (Node *) lfirst(lc);

                if (IsA(fnode, RangeTblRef) &&
                    ((RangeTblRef *) fnode)->rtindex == ctx.varno_of_table[i])
                {
                    found = true;
                    break;
                }
            }
            if (!found)
                all_present = false;
        }

        if (!all_inner || !all_present)
        {
            ereport(DEBUG1,
                    (errmsg("table_materializer: unsupported jointree shape "
                            "for MV \"%s\".\"%s\", skipping join rewrite",
                            entry->mv_schema, entry->mv_name)));
            return false;
        }

        /* Verify each link's equijoin is present in WHERE. */
        for (i = 1; i < n; i++)
        {
            int            li      = entry->join_links[i].left_idx;
            RangeTblEntry *rte_r   = (RangeTblEntry *)
                list_nth(query->rtable, matched_rte_indexes[i]);
            RangeTblEntry *rte_l   = (RangeTblEntry *)
                list_nth(query->rtable, matched_rte_indexes[li]);
            AttrNumber     attno_r = get_attnum(rte_r->relid,
                                        entry->join_links[i].right_col);
            AttrNumber     attno_l = get_attnum(rte_l->relid,
                                        entry->join_links[i].left_col);

            if (li < 0 || li >= i ||
                !AttributeNumberIsValid(attno_r) ||
                !AttributeNumberIsValid(attno_l) ||
                !tm_find_equijoin(query->jointree->quals,
                                  ctx.varno_of_table[li], attno_l,
                                  ctx.varno_of_table[i], attno_r))
            {
                ereport(DEBUG1,
                        (errmsg("table_materializer: join predicate not "
                                "verified for MV \"%s\".\"%s\", skipping "
                                "rewrite", entry->mv_schema, entry->mv_name)));
                return false;
            }
        }
    }

    /*
     * --- Pre-mutation check: every Var on a matched table maps into the IMMV.
     * Bail before touching anything if a referenced column is absent (e.g. a
     * dropped/aliased column, or a whole-row Var on a dropped table).
     */
    {
        VarCheckCtx vcheck;
        bool        unmapped;

        vcheck.varno_keep = ctx.varno_keep;
        vcheck.n_tables   = ctx.n_tables;
        vcheck.col_map    = ctx.col_map;
        memcpy(vcheck.varno_of_table, ctx.varno_of_table,
               sizeof(vcheck.varno_of_table));

        unmapped =
            expression_tree_walker((Node *) query->targetList,
                                   tm_has_unmapped_var, &vcheck) ||
            expression_tree_walker(query->jointree->quals,
                                   tm_has_unmapped_var, &vcheck) ||
            (query->havingQual &&
             expression_tree_walker(query->havingQual,
                                    tm_has_unmapped_var, &vcheck)) ||
            (query->returningList &&
             expression_tree_walker((Node *) query->returningList,
                                    tm_has_unmapped_var, &vcheck));

        /* GROUP BY expressions live in an RTE_GROUP range-table entry (PG17+),
         * not the target list, so they must be checked separately. */
        if (!unmapped)
        {
            foreach(lc, query->rtable)
            {
                RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

                if (rte->rtekind == RTE_GROUP &&
                    expression_tree_walker((Node *) rte->groupexprs,
                                           tm_has_unmapped_var, &vcheck))
                {
                    unmapped = true;
                    break;
                }
            }
        }

        if (unmapped)
        {
            ereport(DEBUG1,
                    (errmsg("table_materializer: column map incomplete for "
                            "MV \"%s\".\"%s\", skipping join rewrite",
                            entry->mv_schema, entry->mv_name)));
            return false;
        }
    }

    /* ============================================================
     * All checks passed — apply the rewrite (no early returns below).
     * ============================================================ */

    /* --- repoint RTE[matched[0]] (the kept varno) at the IMMV --- */
    {
        RangeTblEntry     *rte0 = (RangeTblEntry *)
            list_nth(query->rtable, matched_rte_indexes[0]);
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

        if (rte0->eref != NULL)
            rte0->eref->aliasname = pstrdup(entry->mv_name);
    }

    /* --- collapse the jointree --- */
    if (shape_a)
    {
        /* Replace the whole JoinExpr subtree with a single RangeTblRef to the
         * kept (IMMV) varno. */
        RangeTblRef *mvref = makeNode(RangeTblRef);

        mvref->rtindex = ctx.varno_keep;
        foreach(lc, query->jointree->fromlist)
        {
            if ((Node *) lfirst(lc) == join_fromlist_node)
            {
                lfirst(lc) = mvref;
                break;
            }
        }
    }
    else
    {
        /* Implicit join: remove every matched RangeTblRef except the kept one.
         * Dropped RTEs stay in query->rtable as dead permission entries. */
        for (i = 1; i < n; i++)
        {
            foreach(lc, query->jointree->fromlist)
            {
                Node *fnode = (Node *) lfirst(lc);

                if (IsA(fnode, RangeTblRef) &&
                    ((RangeTblRef *) fnode)->rtindex == ctx.varno_of_table[i])
                {
                    query->jointree->fromlist =
                        list_delete_ptr(query->jointree->fromlist, fnode);
                    break;
                }
            }
        }
    }

    /* --- remap Var nodes throughout the query expressions ---
     * Each container is mutated exactly once (the mutator is not idempotent —
     * it would remap already-remapped kept-table Vars a second time). */
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

    /* GROUP BY expressions live in an RTE_GROUP range-table entry (PG17+),
     * separate from the target list — remap those too. */
    foreach(lc, query->rtable)
    {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

        if (rte->rtekind == RTE_GROUP)
            rte->groupexprs =
                (List *) tm_rewrite_var_mutator((Node *) rte->groupexprs, &ctx);
    }

    /*
     * For shape (A) we collapsed an entire JoinExpr subtree; neutralize the
     * now-dead RTE_JOIN entries so the planner's outer-join handling does not
     * trip on a join that no longer exists in the jointree.
     */
    if (shape_a)
        tm_neutralize_join_rtes(join_fromlist_node, query->rtable);

    ereport(DEBUG1,
            (errmsg("table_materializer: rewrote %d-table join query to use "
                    "MV \"%s\".\"%s\"",
                    n, entry->mv_schema, entry->mv_name)));
    return true;
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

/* ================================================================
 * JOIN-MV CREATION
 *
 * Detects simple equijoin chains in the expensive-query workload (via the
 * real parser, not regex), creates a pre-joined IMMV for each, and registers
 * it so the interceptor can rewrite matching join queries.  Only the explicit
 * left-deep "FROM a JOIN b ON ... JOIN c ON ..." shape with single-column
 * equijoins and INNER/LEFT join types is supported.
 * ================================================================
 */

/* Parsed description of a simple equijoin chain extracted from a query. */
typedef struct
{
    int n;
    struct {
        char schema[MV_NAME_LEN];
        char name[MV_NAME_LEN];
        char alias[MV_NAME_LEN];
        Oid  oid;
    } tabs[MV_MAX_TABLES];
    struct {
        int  left_idx;              /* earlier table this one attaches to */
        char left_col[MV_NAME_LEN];
        char right_col[MV_NAME_LEN];
        int  join_type;             /* JOIN_INNER / JOIN_LEFT */
    } links[MV_MAX_TABLES];         /* links[0] unused */
} JoinSpec;

/* One IMMV output column and the source column it came from. */
typedef struct
{
    int  table_idx;
    int  src_attno;
    char outname[NAMEDATALEN];
} ColMapRec;

/* Index (into spec->tabs[0..count-1]) of the table with the given alias. */
static int
tm_alias_idx(const JoinSpec *spec, int count, const char *alias)
{
    int t;

    for (t = 0; t < count; t++)
        if (strcmp(spec->tabs[t].alias, alias) == 0)
            return t;
    return -1;
}

/* Extract (alias, column) from a qualified raw ColumnRef ("alias"."col"). */
static bool
tm_colref_parts(Node *node, char **alias, char **col)
{
    ColumnRef *cr;

    *alias = NULL;
    *col   = NULL;

    if (node == NULL || !IsA(node, ColumnRef))
        return false;

    cr = (ColumnRef *) node;
    if (list_length(cr->fields) == 2 &&
        IsA(linitial(cr->fields), String) &&
        IsA(lsecond(cr->fields), String))
    {
        *alias = strVal(linitial(cr->fields));
        *col   = strVal(lsecond(cr->fields));
        return true;
    }
    return false;   /* unqualified or schema-qualified refs are not supported */
}

/*
 * Parse the ON clause of the join that introduces table k.  Requires a single
 * "<a>.<x> = <b>.<y>" equijoin where exactly one side is table k (the newly
 * added relation) and the other is some earlier table.  Fills spec->links[k].
 */
static bool
tm_parse_join_on(Node *quals, JoinSpec *spec, int k)
{
    A_Expr *ae;
    char   *la, *lc, *ra, *rc;
    int     lidx, ridx;

    if (quals == NULL || !IsA(quals, A_Expr))
        return false;

    ae = (A_Expr *) quals;
    if (ae->kind != AEXPR_OP || list_length(ae->name) != 1 ||
        strcmp(strVal(linitial(ae->name)), "=") != 0)
        return false;

    if (!tm_colref_parts(ae->lexpr, &la, &lc) ||
        !tm_colref_parts(ae->rexpr, &ra, &rc))
        return false;

    lidx = tm_alias_idx(spec, k + 1, la);
    ridx = tm_alias_idx(spec, k + 1, ra);
    if (lidx < 0 || ridx < 0)
        return false;

    if (lidx == k && ridx < k)
    {
        spec->links[k].left_idx = ridx;
        strlcpy(spec->links[k].left_col,  rc, MV_NAME_LEN);
        strlcpy(spec->links[k].right_col, lc, MV_NAME_LEN);
        return true;
    }
    if (ridx == k && lidx < k)
    {
        spec->links[k].left_idx = lidx;
        strlcpy(spec->links[k].left_col,  lc, MV_NAME_LEN);
        strlcpy(spec->links[k].right_col, rc, MV_NAME_LEN);
        return true;
    }
    return false;   /* equijoin does not attach the new table to an earlier one */
}

/* Append a base relation (RangeVar) to the spec; aliases must be unique. */
static bool
tm_add_relation(JoinSpec *spec, RangeVar *rv)
{
    int         idx = spec->n;
    const char *alias;

    if (idx >= MV_MAX_TABLES || rv->relname == NULL)
        return false;

    strlcpy(spec->tabs[idx].schema,
            rv->schemaname ? rv->schemaname : "public", MV_NAME_LEN);
    strlcpy(spec->tabs[idx].name, rv->relname, MV_NAME_LEN);
    alias = (rv->alias && rv->alias->aliasname) ? rv->alias->aliasname
                                                : rv->relname;
    strlcpy(spec->tabs[idx].alias, alias, MV_NAME_LEN);

    if (tm_alias_idx(spec, idx, spec->tabs[idx].alias) >= 0)
        return false;   /* duplicate alias (self-join) — not supported */

    spec->n = idx + 1;
    return true;
}

/* Recursively collect a strictly left-deep raw JoinExpr tree into the spec. */
static bool
tm_collect_join(Node *node, JoinSpec *spec)
{
    JoinExpr *je;
    int       k;

    if (node == NULL)
        return false;

    if (IsA(node, RangeVar))
        return tm_add_relation(spec, (RangeVar *) node);

    if (!IsA(node, JoinExpr))
        return false;   /* subselect / function / etc. */

    je = (JoinExpr *) node;
    if ((je->jointype != JOIN_INNER && je->jointype != JOIN_LEFT) ||
        je->isNatural || je->usingClause != NIL)
        return false;

    /* Left-deep only: the right arm must be a base relation. */
    if (!IsA(je->rarg, RangeVar))
        return false;

    if (!tm_collect_join(je->larg, spec))
        return false;
    if (!tm_add_relation(spec, (RangeVar *) je->rarg))
        return false;

    k = spec->n - 1;
    spec->links[k].join_type = (int) je->jointype;
    return tm_parse_join_on(je->quals, spec, k);
}

/*
 * Parse a (possibly normalized) query text and, if it is a simple left-deep
 * equijoin chain of 2..MV_MAX_TABLES base relations, fill *spec.  Ignores the
 * WHERE/GROUP BY/aggregates — the IMMV is built from the join shape only.
 * May raise on syntax errors in truncated text; the caller runs it inside a
 * subtransaction.
 */
static bool
tm_extract_join_spec(const char *query_text, JoinSpec *spec)
{
    List       *parsetree_list;
    RawStmt    *rs;
    SelectStmt *sel;
    Node       *fromnode;

    memset(spec, 0, sizeof(*spec));

    parsetree_list = raw_parser(query_text, RAW_PARSE_DEFAULT);
    if (list_length(parsetree_list) != 1)
        return false;

    rs = (RawStmt *) linitial(parsetree_list);
    if (!IsA(rs->stmt, SelectStmt))
        return false;

    sel = (SelectStmt *) rs->stmt;
    if (sel->op != SETOP_NONE || sel->withClause != NULL)
        return false;
    if (list_length(sel->fromClause) != 1)
        return false;

    fromnode = (Node *) linitial(sel->fromClause);
    if (!IsA(fromnode, JoinExpr))
        return false;

    if (!tm_collect_join(fromnode, spec))
        return false;

    return (spec->n >= 2);
}

/*
 * Build the IMMV definition SQL with collision-free output column names, and
 * record (table_idx, src_attno, outname) for each output column so the caller
 * can construct col_map from the created relation.  Uses generated aliases
 * a0..a{n-1} so the definition is independent of the query's aliases.
 */
static void
tm_build_join_immv_sql(const JoinSpec *spec, StringInfo def,
                       ColMapRec *recs, int *nrecs)
{
    char (*used)[NAMEDATALEN];
    int   nused = 0;
    bool  first = true;
    int   t, k;

    used = (char (*)[NAMEDATALEN])
        palloc(sizeof(char[NAMEDATALEN]) * MV_MAX_TABLES * 64);

    *nrecs = 0;
    appendStringInfoString(def, "SELECT ");

    for (t = 0; t < spec->n; t++)
    {
        Oid       relid = spec->tabs[t].oid;
        HeapTuple reltup;
        int       natts;
        int       a;

        reltup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
        if (!HeapTupleIsValid(reltup))
            continue;
        natts = ((Form_pg_class) GETSTRUCT(reltup))->relnatts;
        ReleaseSysCache(reltup);

        for (a = 1; a <= natts; a++)
        {
            HeapTuple          atttup;
            Form_pg_attribute  att;
            char               colname[NAMEDATALEN];
            char               outname[NAMEDATALEN];
            int                u;
            bool               collide;

            atttup = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid),
                                     Int16GetDatum((int16) a));
            if (!HeapTupleIsValid(atttup))
                continue;
            att = (Form_pg_attribute) GETSTRUCT(atttup);
            if (att->attisdropped)
            {
                ReleaseSysCache(atttup);
                continue;
            }
            strlcpy(colname, NameStr(att->attname), NAMEDATALEN);
            ReleaseSysCache(atttup);

            /* Pick a unique output name: bare col, else <table>_<col>, else
             * <table>_<col>_<n>. */
            strlcpy(outname, colname, NAMEDATALEN);
            collide = false;
            for (u = 0; u < nused; u++)
                if (strcmp(used[u], outname) == 0) { collide = true; break; }
            if (collide)
            {
                int sfx = 1;
                bool again;

                snprintf(outname, NAMEDATALEN, "%s_%s",
                         spec->tabs[t].name, colname);
                do
                {
                    again = false;
                    for (u = 0; u < nused; u++)
                        if (strcmp(used[u], outname) == 0) { again = true; break; }
                    if (again)
                        snprintf(outname, NAMEDATALEN, "%s_%s_%d",
                                 spec->tabs[t].name, colname, sfx++);
                } while (again);
            }
            strlcpy(used[nused++], outname, NAMEDATALEN);

            if (!first)
                appendStringInfoString(def, ", ");
            first = false;
            appendStringInfo(def, "a%d.%s AS %s",
                             t, quote_identifier(colname),
                             quote_identifier(outname));

            recs[*nrecs].table_idx = t;
            recs[*nrecs].src_attno = a;
            strlcpy(recs[*nrecs].outname, outname, NAMEDATALEN);
            (*nrecs)++;
        }
    }

    appendStringInfo(def, " FROM %s.%s a0",
                     quote_identifier(spec->tabs[0].schema),
                     quote_identifier(spec->tabs[0].name));
    for (k = 1; k < spec->n; k++)
    {
        appendStringInfo(def, " %s %s.%s a%d ON a%d.%s = a%d.%s",
                         spec->links[k].join_type == JOIN_LEFT ? "LEFT JOIN"
                                                               : "INNER JOIN",
                         quote_identifier(spec->tabs[k].schema),
                         quote_identifier(spec->tabs[k].name), k,
                         spec->links[k].left_idx,
                         quote_identifier(spec->links[k].left_col),
                         k,
                         quote_identifier(spec->links[k].right_col));
    }

    pfree(used);
}

/*
 * Resolve every source table's OID, reject anything unsuitable (missing,
 * non-ordinary-table, system catalog, or an existing auto-MV), and compute a
 * deterministic IMMV name.  Returns false to skip the candidate.
 */
static bool
tm_resolve_join_spec(JoinSpec *spec, char *mv_name_out)
{
    char  buf[NAMEDATALEN * MV_MAX_TABLES];
    int   t;
    size_t len;

    buf[0] = '\0';

    for (t = 0; t < spec->n; t++)
    {
        RangeVar rv;
        Oid      oid;
        char     relkind;
        const char *nm = spec->tabs[t].name;

        if (strncmp(nm, "pg_", 3) == 0)
            return false;
        len = strlen(nm);
        if (len >= 8 && strcmp(nm + len - 8, "_auto_mv") == 0)
            return false;

        memset(&rv, 0, sizeof(rv));
        rv.type           = T_RangeVar;
        rv.schemaname     = spec->tabs[t].schema;
        rv.relname        = spec->tabs[t].name;
        rv.inh            = false;
        rv.relpersistence = RELPERSISTENCE_PERMANENT;
        rv.location       = -1;

        oid = RangeVarGetRelid(&rv, NoLock, true /* missing_ok */);
        if (!OidIsValid(oid))
            return false;

        relkind = get_rel_relkind(oid);
        if (relkind != RELKIND_RELATION)
            return false;   /* only plain tables are valid IMMV sources */

        spec->tabs[t].oid = oid;

        strlcat(buf, nm, sizeof(buf));
        strlcat(buf, "_", sizeof(buf));
    }

    strlcat(buf, "jn_auto_mv", sizeof(buf));

    if (strlen(buf) < NAMEDATALEN)
        strlcpy(mv_name_out, buf, NAMEDATALEN);
    else
    {
        uint32 h = 2166136261u;

        for (t = 0; t < spec->n; t++)
            h = (h ^ (uint32) spec->tabs[t].oid) * 16777619u;
        snprintf(mv_name_out, NAMEDATALEN, "mvjoin_%u_%dt_auto_mv", h, spec->n);
    }
    return true;
}

/* Register a freshly created/located join IMMV in the shared registry. */
static bool
tm_register_join_entry(const JoinSpec *spec, const char *mv_name,
                       const int colmap[MV_MAX_TABLES][64])
{
    bool registered = false;
    int  t, k;

    LWLockAcquire(&top_queries_locks[1].lock, LW_EXCLUSIVE);
    if (mv_registry_state->num_entries < MV_REGISTRY_MAX)
    {
        MVRegistryEntry *entry =
            &mv_registry_state->entries[mv_registry_state->num_entries];

        memset(entry, 0, sizeof(*entry));
        strlcpy(entry->mv_schema, "public", sizeof(entry->mv_schema));
        strlcpy(entry->mv_name, mv_name, sizeof(entry->mv_name));
        entry->num_source_tables = spec->n;

        for (t = 0; t < spec->n; t++)
        {
            strlcpy(entry->source_tables[t].schema, spec->tabs[t].schema,
                    sizeof(entry->source_tables[t].schema));
            strlcpy(entry->source_tables[t].name, spec->tabs[t].name,
                    sizeof(entry->source_tables[t].name));
        }
        for (k = 1; k < spec->n; k++)
        {
            entry->join_links[k].left_idx  = spec->links[k].left_idx;
            entry->join_links[k].join_type = spec->links[k].join_type;
            strlcpy(entry->join_links[k].left_col, spec->links[k].left_col,
                    sizeof(entry->join_links[k].left_col));
            strlcpy(entry->join_links[k].right_col, spec->links[k].right_col,
                    sizeof(entry->join_links[k].right_col));
        }
        memcpy(entry->col_map, colmap, sizeof(entry->col_map));
        entry->has_col_map = true;

        mv_registry_state->num_entries++;
        registered = true;
    }
    LWLockRelease(&top_queries_locks[1].lock);

    if (!registered)
        ereport(LOG,
                (errmsg("table_materializer: MV registry full (%d entries), "
                        "cannot register public.%s", MV_REGISTRY_MAX, mv_name)));
    return registered;
}

/*
 * tm_create_join_mvs — find expensive join queries, create a pre-joined IMMV
 * for each (up to `budget`), and register them.  Returns the number created or
 * re-registered.  Must be called with SPI connected and an active snapshot.
 *
 * Each candidate is processed inside an internal subtransaction so a parse
 * error (truncated text) or a pg_ivm rejection only skips that one candidate
 * rather than aborting the whole pass.
 */
static int
tm_create_join_mvs(int budget)
{
    static const char cand_sql_tmpl[] =
        "SELECT query FROM pg_stat_statements"
        " WHERE upper(ltrim(query)) LIKE 'SELECT%%'"
        "   AND calls >= %d AND mean_exec_time >= %.4f"
        "   AND query ILIKE '%%join%%'"
        " ORDER BY " HEURISTIC_SCORE_EXPR " DESC"
        " LIMIT %d";

    char           cand_sql[1024];
    int            fetch_limit;
    int            created = 0;
    int            ret, i, ncand = 0;
    char         **cands;
    MemoryContext  joinctx, oldctx;

    if (budget <= 0)
        return 0;

    fetch_limit = budget * 4;
    if (fetch_limit < 8)
        fetch_limit = 8;

    joinctx = AllocSetContextCreate(CurrentMemoryContext,
                                    "tm_join_cands", ALLOCSET_SMALL_SIZES);

    snprintf(cand_sql, sizeof(cand_sql), cand_sql_tmpl,
             heuristic_min_calls, heuristic_min_exec_ms, fetch_limit);

    ret = SPI_execute(cand_sql, true, fetch_limit);
    if (ret != SPI_OK_SELECT || SPI_processed == 0)
    {
        MemoryContextDelete(joinctx);
        return 0;
    }

    /* Copy candidate texts into joinctx; it is a child of the SPI procedure
     * context (not a transaction context), so the copies survive the
     * per-candidate subtransactions below. */
    {
        int nrows = (int) SPI_processed;

        oldctx = MemoryContextSwitchTo(joinctx);
        cands  = (char **) palloc(sizeof(char *) * nrows);
        MemoryContextSwitchTo(oldctx);

        for (i = 0; i < nrows; i++)
        {
            bool  isnull;
            Datum d = SPI_getbinval(SPI_tuptable->vals[i],
                                    SPI_tuptable->tupdesc, 1, &isnull);
            if (!isnull)
            {
                char *s = TextDatumGetCString(d);

                oldctx = MemoryContextSwitchTo(joinctx);
                cands[ncand++] = pstrdup(s);
                MemoryContextSwitchTo(oldctx);
                pfree(s);
            }
        }
    }

    for (i = 0; i < ncand && created < budget; i++)
    {
        JoinSpec          spec;
        char              mv_name[NAMEDATALEN];
        int               colmap[MV_MAX_TABLES][64];
        volatile bool     success = false;
        MemoryContext     old = CurrentMemoryContext;

        BeginInternalSubTransaction(NULL);
        MemoryContextSwitchTo(old);

        PG_TRY();
        {
            if (tm_extract_join_spec(cands[i], &spec) &&
                tm_resolve_join_spec(&spec, mv_name))
            {
                bool        immv_exists, already_registered;
                int         j;
                Oid         argt1 = TEXTOID;
                Datum       argv1;
                ColMapRec  *recs;
                int         nrecs = 0;
                RangeVar    mvrv;
                Oid         mv_oid;
                int         r;

                /* Does the IMMV already exist on disk? */
                argv1 = CStringGetTextDatum(mv_name);
                ret = SPI_execute_with_args(
                        "SELECT 1 FROM pg_tables "
                        "WHERE schemaname = 'public' AND tablename = $1",
                        1, &argt1, &argv1, " ", true, 1);
                immv_exists = (ret == SPI_OK_SELECT && SPI_processed > 0);

                already_registered = false;
                LWLockAcquire(&top_queries_locks[1].lock, LW_SHARED);
                for (j = 0; j < mv_registry_state->num_entries; j++)
                    if (strcmp(mv_registry_state->entries[j].mv_name,
                               mv_name) == 0)
                    {
                        already_registered = true;
                        break;
                    }
                LWLockRelease(&top_queries_locks[1].lock);

                if (immv_exists && already_registered)
                {
                    /* Nothing to do; leave success=false so it isn't counted. */
                    ReleaseCurrentSubTransaction();
                    MemoryContextSwitchTo(old);
                }
                else
                {
                    StringInfoData def;

                    recs = (ColMapRec *)
                        palloc(sizeof(ColMapRec) * MV_MAX_TABLES * 64);
                    initStringInfo(&def);
                    tm_build_join_immv_sql(&spec, &def, recs, &nrecs);

                    if (!immv_exists)
                    {
                        Oid   argt2[2] = {TEXTOID, TEXTOID};
                        Datum argv2[2];

                        argv2[0] = CStringGetTextDatum(mv_name);
                        argv2[1] = CStringGetTextDatum(def.data);
                        SPI_execute_with_args(
                            "SELECT pgivm.create_immv("
                            "  format('public.%I', $1), $2)",
                            2, argt2, argv2, "  ", false, 0);

                        ereport(LOG,
                                (errmsg("table_materializer: created %d-table "
                                        "join IMMV public.%s",
                                        spec.n, mv_name)));
                    }

                    /* Build col_map from the created/located relation. */
                    memset(colmap, 0, sizeof(colmap));
                    memset(&mvrv, 0, sizeof(mvrv));
                    mvrv.type           = T_RangeVar;
                    mvrv.schemaname     = "public";
                    mvrv.relname        = mv_name;
                    mvrv.inh            = false;
                    mvrv.relpersistence = RELPERSISTENCE_PERMANENT;
                    mvrv.location       = -1;
                    mv_oid = RangeVarGetRelid(&mvrv, NoLock, true);

                    if (OidIsValid(mv_oid))
                    {
                        for (r = 0; r < nrecs; r++)
                        {
                            AttrNumber mva = get_attnum(mv_oid,
                                                        recs[r].outname);
                            if (AttributeNumberIsValid(mva) &&
                                recs[r].src_attno < 64)
                                colmap[recs[r].table_idx][recs[r].src_attno] =
                                    (int) mva;
                        }
                        success = true;
                    }

                    ReleaseCurrentSubTransaction();
                    MemoryContextSwitchTo(old);
                }
            }
            else
            {
                /* Not a supported join shape — discard the subtransaction. */
                ReleaseCurrentSubTransaction();
                MemoryContextSwitchTo(old);
            }
        }
        PG_CATCH();
        {
            MemoryContextSwitchTo(old);
            EmitErrorReport();
            FlushErrorState();
            RollbackAndReleaseCurrentSubTransaction();
            MemoryContextSwitchTo(old);
            success = false;
        }
        PG_END_TRY();

        if (success)
        {
            if (tm_register_join_entry(&spec, mv_name, colmap))
                created++;
        }
    }

    MemoryContextDelete(joinctx);
    return created;
}

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

    /*
     * ---- Join pass: create pre-joined IMMVs first ----
     * Multi-table join MVs cover more of the workload than single-table
     * mirrors, so they get first claim on the max_mv_count budget.  The
     * single-table pass below fills any remaining slots.
     */
    created = tm_create_join_mvs(max_mv_count);
    if (created >= max_mv_count)
        return created;

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
    for (i = 0; i < num_candidates && created < max_mv_count; i++)
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
