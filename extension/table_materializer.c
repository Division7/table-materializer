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

    /*
     * Decay bookkeeping for workload-shift eviction (see tm_evict_mvs).
     * peak_score   = highest recent-activity score this entry has ever scored
     *                (the EWMA of its hottest source table).
     * cold_ticks   = consecutive BGW ticks the entry's current score has been
     *                below evict_score_frac * peak_score; reset to 0 whenever
     *                it recovers.  When it reaches evict_grace_ticks the IMMV
     *                is dropped (hysteresis against bursty workloads).
     */
    double peak_score;
    int    cold_ticks;
} MVRegistryEntry;

typedef struct
{
    MVRegistryEntry entries[MV_REGISTRY_MAX];
    int             num_entries;
} MVRegistryState;

/* ----------------------------------------------------------------
 * Per-table EWMA score registry  (workload-shift detection)
 *
 * pg_stat_statements is cumulative, so a once-hot table keeps a high score
 * forever — a workload shift would be invisible.  Every BGW tick we compute,
 * per FROM-root table, the DELTA of total_exec_time since the previous tick
 * and fold it into an exponentially-weighted moving average:
 *
 *     ewma = alpha * delta + (1 - alpha) * ewma
 *
 * A table that stops being queried contributes delta = 0 each tick, so its
 * ewma decays geometrically toward zero — that decay is what lets the evictor
 * (tm_evict_mvs) notice a table has gone cold and drop its IMMV.
 * ---------------------------------------------------------------- */

#define SCORE_MAX 64

typedef struct
{
    char   tbl[NAMEDATALEN];
    double ewma;                 /* decayed recent-activity score          */
    double peak;                 /* highest ewma ever seen (for cold gate)  */
    double last_total_exec_ms;   /* cumulative snapshot, for next delta     */
    int64  last_calls;           /* cumulative snapshot (diagnostics only)  */
    bool   in_use;
} TableScore;

typedef struct
{
    TableScore entries[SCORE_MAX];
    int        num;
} TableScoreState;

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
static TableScoreState              *table_score_state   = NULL;
/* slot 0 = top queries, slot 1 = mv registry, slot 2 = table scores */
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

/* ----------------------------------------------------------------
 * Workload-shift / eviction GUC state  (see tm_evict_mvs)
 * ---------------------------------------------------------------- */
static double score_decay_alpha     = 0.5;   /* EWMA weight on newest delta  */
static int    evict_grace_ticks     = 3;     /* cold ticks before a drop     */
static double evict_score_frac      = 0.2;   /* cold = score < frac * peak   */

static volatile sig_atomic_t got_sigterm = false;

void _PG_init(void);
PGDLLEXPORT void bgworker_main(Datum main_arg);
PG_FUNCTION_INFO_V1(top_expensive_queries);
PG_FUNCTION_INFO_V1(force_spawn_mvs);
PG_FUNCTION_INFO_V1(list_materialized_views);

static void tq_shmem_request(void);
static void tq_shmem_startup(void);
static void handle_sigterm(SIGNAL_ARGS);
static void update_top_queries(void);
static int  do_select_and_create_mvs(void);
static int  tm_create_join_mvs(int budget);
static void select_and_create_mvs(void);

static void   update_table_scores(void);
static double tm_score_for_table(const char *name);
static bool   tm_table_is_cold(const char *name);
static double tm_score_for_entry(const MVRegistryEntry *entry);
static bool   tm_drop_immv(const char *mv_name);
static void   tm_unregister_entry(int idx);
static void   tm_drop_and_unregister(const char *mv_name);
static void   tm_evict_mvs(void);
static bool   tm_try_displace(double cand_score);

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
    /*
     * When true, any whole-row / system-column Var (varattno <= 0) on a matched
     * table is treated as unmapped.  A column-subset IMMV has a different
     * rowtype than its base table, so a whole-row reference cannot be served
     * from it — it must disqualify the rewrite.  The join path leaves this false
     * (it preserves the historical kept-table behavior).
     */
    bool disallow_wholerow;
} VarCheckCtx;

static void tm_post_parse_analyze(ParseState *pstate, Query *query,
                                   JumbleState *jstate);
static bool tm_entry_tables_present(Query *query, const MVRegistryEntry *entry,
                                    int matched_rte_indexes[MV_MAX_TABLES]);
static bool tm_rewrite_single(Query *query, const MVRegistryEntry *entry,
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

    DefineCustomRealVariable("table_materializer.score_decay_alpha",
                             "EWMA weight applied to the newest per-tick activity delta.",
                             "Each tick a table's recent-activity score becomes "
                             "alpha*delta + (1-alpha)*score.  Higher reacts faster to "
                             "workload shifts (shorter memory); lower is smoother.",
                             &score_decay_alpha,
                             0.5, 0.0, 1.0,
                             PGC_SIGHUP, 0,
                             NULL, NULL, NULL);

    DefineCustomIntVariable("table_materializer.evict_grace_ticks",
                            "Consecutive cold ticks before an auto-created IMMV is dropped.",
                            "Hysteresis against bursty workloads: an IMMV is only dropped "
                            "after its source table has stayed cold this many BGW intervals.",
                            &evict_grace_ticks,
                            3, 0, 100000,
                            PGC_SIGHUP, 0,
                            NULL, NULL, NULL);

    DefineCustomRealVariable("table_materializer.evict_score_frac",
                             "Cold threshold as a fraction of an IMMV's peak activity score.",
                             "An IMMV is considered cold on a tick when its current score "
                             "falls below this fraction of the highest score it has reached.",
                             &evict_score_frac,
                             0.2, 0.0, 1.0,
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
    RequestAddinShmemSpace(sizeof(TableScoreState));
    RequestNamedLWLockTranche("table_materializer", 3);
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

    table_score_state = ShmemInitStruct("table_materializer_table_scores",
                                        sizeof(TableScoreState),
                                        &found);
    if (!found)
        memset(table_score_state, 0, sizeof(TableScoreState));
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
                if (tm_rewrite_single(query, entry, matched_rte_indexes))
                    return;
                /* column-subset verification failed — try the next candidate */
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
 * Single-table rewrite: point the matched source table's RTE at its IMMV.
 *
 * Two flavors, selected by entry->has_col_map:
 *   - full mirror (has_col_map == false): the IMMV is "SELECT * FROM tbl" with
 *     an identical column layout, so we only repoint the RTE's relid/relkind.
 *   - column subset (has_col_map == true): the IMMV holds a projection of the
 *     base table, so every Var on the matched table must (a) be present in the
 *     subset and (b) have its varattno remapped to the IMMV's column position.
 *     We fail-closed: if any referenced column (or a whole-row/system Var) is
 *     absent from the subset, we leave the query untouched and return false so
 *     the caller can try another registry entry or fall back to the base table.
 *
 * Returns true if the query was rewritten onto the IMMV.
 */
static bool
tm_rewrite_single(Query *query, const MVRegistryEntry *entry,
                  int matched_rte_indexes[MV_MAX_TABLES])
{
    RangeTblEntry *rte;
    RangeVar       rv;
    Oid            mv_oid;
    int            keep = matched_rte_indexes[0] + 1;   /* 1-based varno */
    VarRewriteCtx  ctx;

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
        return false;
    }

    /* Column-subset IMMV: build the remap context and verify mappability. */
    if (entry->has_col_map)
    {
        VarCheckCtx vcheck;
        bool        unmapped;
        ListCell   *lc;

        ctx.varno_keep        = keep;
        ctx.n_tables          = 1;
        ctx.varno_of_table[0] = keep;
        ctx.col_map           = (int (*)[64]) entry->col_map;

        vcheck.varno_keep        = keep;
        vcheck.n_tables          = 1;
        vcheck.varno_of_table[0] = keep;
        vcheck.col_map           = ctx.col_map;
        vcheck.disallow_wholerow = true;   /* subset has a different rowtype */

        unmapped =
            expression_tree_walker((Node *) query->targetList,
                                   tm_has_unmapped_var, &vcheck) ||
            (query->jointree &&
             expression_tree_walker(query->jointree->quals,
                                    tm_has_unmapped_var, &vcheck)) ||
            (query->havingQual &&
             expression_tree_walker(query->havingQual,
                                    tm_has_unmapped_var, &vcheck)) ||
            (query->returningList &&
             expression_tree_walker((Node *) query->returningList,
                                    tm_has_unmapped_var, &vcheck));

        /* GROUP BY expressions live in an RTE_GROUP entry (PG17+). */
        if (!unmapped)
        {
            foreach(lc, query->rtable)
            {
                RangeTblEntry *grte = (RangeTblEntry *) lfirst(lc);

                if (grte->rtekind == RTE_GROUP &&
                    expression_tree_walker((Node *) grte->groupexprs,
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
                    (errmsg("table_materializer: query references columns absent "
                            "from subset MV \"%s\".\"%s\", skipping rewrite",
                            entry->mv_schema, entry->mv_name)));
            return false;
        }
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

    /* Column-subset IMMV: remap every Var's attno to its IMMV position. */
    if (entry->has_col_map)
    {
        ListCell *lc;

        query->targetList =
            (List *) tm_rewrite_var_mutator((Node *) query->targetList, &ctx);
        if (query->jointree)
            query->jointree->quals =
                tm_rewrite_var_mutator(query->jointree->quals, &ctx);
        if (query->havingQual)
            query->havingQual =
                tm_rewrite_var_mutator(query->havingQual, &ctx);
        if (query->returningList)
            query->returningList =
                (List *) tm_rewrite_var_mutator((Node *) query->returningList,
                                                &ctx);

        foreach(lc, query->rtable)
        {
            RangeTblEntry *grte = (RangeTblEntry *) lfirst(lc);

            if (grte->rtekind == RTE_GROUP)
                grte->groupexprs =
                    (List *) tm_rewrite_var_mutator((Node *) grte->groupexprs,
                                                    &ctx);
        }
    }

    ereport(DEBUG1,
            (errmsg("table_materializer: rewrote query to use MV \"%s\".\"%s\"",
                    entry->mv_schema, entry->mv_name)));
    return true;
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
        {
            if (ctx->disallow_wholerow)
                return true;
            return (ctx->varno_of_table[t] != ctx->varno_keep);
        }

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

        vcheck.varno_keep        = ctx.varno_keep;
        vcheck.n_tables          = ctx.n_tables;
        vcheck.col_map           = ctx.col_map;
        vcheck.disallow_wholerow = false;   /* join path: keep historical behavior */
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

                if ((immv_exists && already_registered) ||
                    (!already_registered && tm_table_is_cold(spec.tabs[0].name)))
                {
                    /*
                     * Either the join IMMV is already present, or the workload
                     * has shifted away from this join (its root table has
                     * decayed cold) — do not (re)create it.  The cold check is
                     * what lets the evictor's drop stick after a shift.
                     */
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

/* ================================================================
 * WORKLOAD-SHIFT DETECTION + EVICTION
 *
 * update_table_scores() refreshes a per-table EWMA of recent activity each
 * tick; tm_evict_mvs() drops IMMVs whose source tables have decayed cold; and
 * tm_try_displace() lets a hotter table evict the weakest incumbent when the
 * max_materialized_views budget is full.  Together these let the selected IMMV
 * set track a changing workload instead of only ever growing.
 * ================================================================
 */

/*
 * Refresh the per-table EWMA score from pg_stat_statements.  Must be called
 * with SPI connected and an active snapshot.  Groups by FROM-root table (same
 * regexp the selection heuristic uses) and folds the per-tick delta in CALL
 * COUNT into each table's moving average; tables absent from this tick's result
 * decay toward zero.
 *
 * The score is driven by `calls`, not total_exec_time, on purpose: once a table
 * is materialized its queries get faster, so an exec-time-based score would
 * collapse and the evictor would wrongly drop the very IMMV that sped the table
 * up (a self-defeating feedback loop).  Call volume is materialization-invariant
 * — it stays high while the table is actively queried and falls to zero only
 * when the workload genuinely shifts away, which is exactly the signal we want.
 */
static void
update_table_scores(void)
{
    char  sql[1024];
    int   ret;
    int   i;
    bool  seen[SCORE_MAX];

    snprintf(sql, sizeof(sql),
        "WITH ranked AS ("
        "  SELECT lower((regexp_match(query,"
        "      'FROM[[:space:]]+\"?([[:alpha:]_][[:alnum:]_]*)\"?',"
        "      'i'))[1]) AS tbl,"
        "    total_exec_time AS tot, calls AS ncalls"
        "  FROM pg_stat_statements"
        "  WHERE upper(ltrim(query)) LIKE 'SELECT%%'"
        ")"
        /* sum(bigint) returns numeric in PostgreSQL; cast back to int8 so
         * SPI_getbinval + DatumGetInt64 read it correctly. */
        " SELECT tbl, sum(tot)::float8 AS tot, sum(ncalls)::bigint AS ncalls"
        " FROM ranked"
        " WHERE tbl IS NOT NULL AND tbl NOT LIKE 'pg_%%'"
        " GROUP BY tbl"
        " ORDER BY tot DESC"
        " LIMIT %d", SCORE_MAX);

    ret = SPI_execute(sql, true, SCORE_MAX);
    if (ret != SPI_OK_SELECT)
        return;

    LWLockAcquire(&top_queries_locks[2].lock, LW_EXCLUSIVE);

    memset(seen, 0, sizeof(seen));

    for (i = 0; i < (int) SPI_processed; i++)
    {
        bool   isnull;
        Datum  d;
        char  *tbl;
        double cur_tot;
        int64  cur_calls;
        int    j, slot;
        double delta;

        d = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1,
                          &isnull);
        if (isnull)
            continue;
        tbl = TextDatumGetCString(d);

        d = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2,
                          &isnull);
        cur_tot = isnull ? 0.0 : DatumGetFloat8(d);

        d = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3,
                          &isnull);
        cur_calls = isnull ? 0 : DatumGetInt64(d);

        /* locate or allocate the score slot for this table */
        slot = -1;
        for (j = 0; j < table_score_state->num; j++)
            if (table_score_state->entries[j].in_use &&
                strcmp(table_score_state->entries[j].tbl, tbl) == 0)
            {
                slot = j;
                break;
            }
        if (slot < 0 && table_score_state->num < SCORE_MAX)
        {
            slot = table_score_state->num++;
            memset(&table_score_state->entries[slot], 0, sizeof(TableScore));
            strlcpy(table_score_state->entries[slot].tbl, tbl, NAMEDATALEN);
            table_score_state->entries[slot].in_use = true;
            /* first sighting: treat the whole cumulative value as the delta */
            table_score_state->entries[slot].last_total_exec_ms = 0.0;
        }
        pfree(tbl);
        if (slot < 0)
            continue;   /* score table full */

        /*
         * delta in CALL COUNT since last tick (materialization-invariant).
         * A drop in the cumulative count means pg_stat_statements was reset
         * since the last tick; restart this table's moving average AND peak so
         * a genuinely-active table is not judged cold against a stale
         * historical peak (which would block its IMMV from being created).
         */
        if (cur_calls < table_score_state->entries[slot].last_calls)
        {
            table_score_state->entries[slot].ewma = 0.0;
            table_score_state->entries[slot].peak = 0.0;
            delta = (double) cur_calls;
        }
        else
            delta = (double) (cur_calls -
                              table_score_state->entries[slot].last_calls);

        table_score_state->entries[slot].ewma =
            score_decay_alpha * delta +
            (1.0 - score_decay_alpha) * table_score_state->entries[slot].ewma;
        if (table_score_state->entries[slot].ewma >
            table_score_state->entries[slot].peak)
            table_score_state->entries[slot].peak =
                table_score_state->entries[slot].ewma;
        table_score_state->entries[slot].last_total_exec_ms = cur_tot;
        table_score_state->entries[slot].last_calls = cur_calls;
        seen[slot] = true;
    }

    /* Tables that fell out of pg_stat_statements entirely still decay. */
    for (i = 0; i < table_score_state->num; i++)
        if (table_score_state->entries[i].in_use && !seen[i])
            table_score_state->entries[i].ewma *= (1.0 - score_decay_alpha);

    LWLockRelease(&top_queries_locks[2].lock);
}

/* Current EWMA score for a single table name (0 if unknown). */
static double
tm_score_for_table(const char *name)
{
    double s = 0.0;
    int    j;

    LWLockAcquire(&top_queries_locks[2].lock, LW_SHARED);
    for (j = 0; j < table_score_state->num; j++)
        if (table_score_state->entries[j].in_use &&
            strcmp(table_score_state->entries[j].tbl, name) == 0)
        {
            s = table_score_state->entries[j].ewma;
            break;
        }
    LWLockRelease(&top_queries_locks[2].lock);
    return s;
}

/*
 * Has this table decayed cold relative to its own peak activity?  Creation
 * paths consult this so a table whose recent workload has dried up is NOT
 * re-materialized just because its cumulative pg_stat_statements score is still
 * high — that is what makes an eviction actually stick when the workload shifts
 * (otherwise the evictor would drop the IMMV and the creator would immediately
 * rebuild it on the same tick).
 */
static bool
tm_table_is_cold(const char *name)
{
    bool   cold = false;
    int    j;

    LWLockAcquire(&top_queries_locks[2].lock, LW_SHARED);
    for (j = 0; j < table_score_state->num; j++)
        if (table_score_state->entries[j].in_use &&
            strcmp(table_score_state->entries[j].tbl, name) == 0)
        {
            double peak = table_score_state->entries[j].peak;

            cold = (peak > 0.0 &&
                    table_score_state->entries[j].ewma < evict_score_frac * peak);
            break;
        }
    LWLockRelease(&top_queries_locks[2].lock);
    return cold;
}

/* A registry entry scores as its hottest source table (chain root carries the
 * signal for join IMMVs). */
static double
tm_score_for_entry(const MVRegistryEntry *entry)
{
    double best = 0.0;
    int    t;

    for (t = 0; t < entry->num_source_tables; t++)
    {
        double s = tm_score_for_table(entry->source_tables[t].name);

        if (s > best)
            best = s;
    }
    return best;
}

/*
 * Drop an auto-created IMMV.  Runs DROP TABLE ... CASCADE inside an internal
 * subtransaction so a failure (e.g. concurrent drop) only skips this one view.
 * pg_ivm's sql_drop event trigger removes the source-table IVM triggers and the
 * pgivm.pg_ivm_immv catalog row.  Must be called with SPI connected.
 */
static bool
tm_drop_immv(const char *mv_name)
{
    volatile bool ok  = false;
    MemoryContext old = CurrentMemoryContext;

    BeginInternalSubTransaction(NULL);
    MemoryContextSwitchTo(old);

    PG_TRY();
    {
        char sql[NAMEDATALEN + 64];

        /*
         * Be a polite background dropper: cap how long we wait for the
         * AccessExclusiveLock so a straggler still reading this IMMV is never
         * killed by a deadlock.  If the lock can't be had quickly the DROP
         * errors, this subtransaction rolls back, and the next tick retries
         * once the readers have moved on.  SET LOCAL is scoped to the
         * subtransaction.
         */
        SPI_execute("SET LOCAL lock_timeout = '500ms'", false, 0);

        snprintf(sql, sizeof(sql),
                 "DROP TABLE IF EXISTS public.%s CASCADE",
                 quote_identifier(mv_name));
        SPI_execute(sql, false, 0);
        ReleaseCurrentSubTransaction();
        MemoryContextSwitchTo(old);
        ok = true;
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(old);
        EmitErrorReport();
        FlushErrorState();
        RollbackAndReleaseCurrentSubTransaction();
        MemoryContextSwitchTo(old);
        ok = false;
    }
    PG_END_TRY();

    return ok;
}

/* Remove registry entry `idx`, compacting the array.  Takes the exclusive
 * registry lock; safe against the rewrite hook, which re-resolves the IMMV OID
 * (missing_ok) and skips if it has vanished. */
static void
tm_unregister_entry(int idx)
{
    int j;

    LWLockAcquire(&top_queries_locks[1].lock, LW_EXCLUSIVE);
    if (idx >= 0 && idx < mv_registry_state->num_entries)
    {
        for (j = idx; j < mv_registry_state->num_entries - 1; j++)
            mv_registry_state->entries[j] = mv_registry_state->entries[j + 1];
        mv_registry_state->num_entries--;
        memset(&mv_registry_state->entries[mv_registry_state->num_entries], 0,
               sizeof(MVRegistryEntry));
    }
    LWLockRelease(&top_queries_locks[1].lock);
}

/* Drop+unregister the IMMV named `mv_name` (helper for the eviction paths). */
static void
tm_drop_and_unregister(const char *mv_name)
{
    int idx, j;

    if (!tm_drop_immv(mv_name))
        return;

    idx = -1;
    LWLockAcquire(&top_queries_locks[1].lock, LW_SHARED);
    for (j = 0; j < mv_registry_state->num_entries; j++)
        if (strcmp(mv_registry_state->entries[j].mv_name, mv_name) == 0)
        {
            idx = j;
            break;
        }
    LWLockRelease(&top_queries_locks[1].lock);

    if (idx >= 0)
        tm_unregister_entry(idx);
}

/*
 * Decay-eviction pass: for each registered IMMV, track its peak score and how
 * many consecutive ticks it has been below evict_score_frac of that peak; once
 * that streak reaches evict_grace_ticks, drop the IMMV.  Must run with SPI
 * connected (tm_drop_immv issues SQL).
 */
static void
tm_evict_mvs(void)
{
    struct { char name[MV_NAME_LEN]; double cur; } dec[MV_REGISTRY_MAX];
    char   drop_names[MV_REGISTRY_MAX][MV_NAME_LEN];
    int    ndec = 0, ndrop = 0;
    int    i, j, n;
    MVRegistryEntry snap[MV_REGISTRY_MAX];

    LWLockAcquire(&top_queries_locks[1].lock, LW_SHARED);
    n = mv_registry_state->num_entries;
    if (n > 0)
        memcpy(snap, mv_registry_state->entries, n * sizeof(MVRegistryEntry));
    LWLockRelease(&top_queries_locks[1].lock);

    if (n <= 0)
        return;

    for (i = 0; i < n; i++)
    {
        strlcpy(dec[i].name, snap[i].mv_name, MV_NAME_LEN);
        dec[i].cur = tm_score_for_entry(&snap[i]);   /* takes score lock */
        ndec++;
    }

    /* Update peak/cold counters in place and collect drop candidates. */
    LWLockAcquire(&top_queries_locks[1].lock, LW_EXCLUSIVE);
    for (i = 0; i < ndec; i++)
    {
        for (j = 0; j < mv_registry_state->num_entries; j++)
        {
            MVRegistryEntry *e = &mv_registry_state->entries[j];
            bool             is_cold;

            if (strcmp(e->mv_name, dec[i].name) != 0)
                continue;

            if (dec[i].cur > e->peak_score)
                e->peak_score = dec[i].cur;

            is_cold = (e->peak_score > 0.0 &&
                       dec[i].cur < evict_score_frac * e->peak_score);
            if (is_cold)
                e->cold_ticks++;
            else
                e->cold_ticks = 0;

            if (is_cold && e->cold_ticks >= evict_grace_ticks)
                strlcpy(drop_names[ndrop++], e->mv_name, MV_NAME_LEN);
            break;
        }
    }
    LWLockRelease(&top_queries_locks[1].lock);

    for (i = 0; i < ndrop; i++)
    {
        tm_drop_and_unregister(drop_names[i]);
        ereport(LOG,
                (errmsg("table_materializer: dropped cold IMMV public.%s "
                        "(workload shifted away)", drop_names[i])));
    }
}

/*
 * Budget enforcement / displacement.  When the registry is already at the
 * max_materialized_views budget, a new candidate may only be created if it is
 * hotter (by a 10%% margin) than the weakest incumbent, which is then dropped
 * to free the slot.  Returns true if there is (now) room to register a new MV.
 */
static bool
tm_try_displace(double cand_score)
{
    MVRegistryEntry snap[MV_REGISTRY_MAX];
    char   victim[MV_NAME_LEN];
    double worst = 0.0;
    bool   have_victim = false;
    int    j, n;

    LWLockAcquire(&top_queries_locks[1].lock, LW_SHARED);
    n = mv_registry_state->num_entries;
    if (n > 0)
        memcpy(snap, mv_registry_state->entries, n * sizeof(MVRegistryEntry));
    LWLockRelease(&top_queries_locks[1].lock);

    if (n < max_mv_count)
        return true;   /* under budget — room already exists */

    for (j = 0; j < n; j++)
    {
        double s = tm_score_for_entry(&snap[j]);

        if (!have_victim || s < worst)
        {
            worst = s;
            strlcpy(victim, snap[j].mv_name, MV_NAME_LEN);
            have_victim = true;
        }
    }

    if (!have_victim || cand_score <= worst * 1.1)
        return false;   /* not hot enough to justify evicting an incumbent */

    tm_drop_and_unregister(victim);
    ereport(LOG,
            (errmsg("table_materializer: displaced IMMV public.%s (score %.1f) "
                    "for a hotter table (score %.1f)",
                    victim, worst, cand_score)));
    return true;
}

/* ================================================================
 * SINGLE-TABLE COLUMN-SUBSET PLANNING
 *
 * For a hot single-table query we materialize only the columns the query
 * actually touches (target list + WHERE + ORDER BY + GROUP BY + HAVING) rather
 * than mirroring the whole — possibly very wide — row, and we add a covering
 * index on the equality-predicate and ORDER BY columns.  The narrow subset is
 * far cheaper to scan and to keep incrementally maintained, and the index turns
 * a "WHERE x = ? ORDER BY y LIMIT n" sequential-scan-plus-sort into an index
 * scan.  Queries that reference a column outside the subset fail the rewrite's
 * fail-closed col_map check and simply read the base table.
 * ================================================================ */

#define TM_MAX_PROJ_COLS 63    /* col_map src_attno index must stay < 64 */
#define TM_MAX_SORT_COLS 8

/* One projected output column and the base-table attno it came from. */
typedef struct
{
    int  src_attno;
    char name[NAMEDATALEN];
} ProjCol;

/* Accumulates distinct column names referenced by a query (raw parse tree). */
typedef struct
{
    char names[TM_MAX_PROJ_COLS][NAMEDATALEN];
    int  n;
    bool has_star;     /* a "*" / "t.*" reference — cannot project */
    bool overflow;     /* more distinct columns than we can track  */
} ColRefSet;

/* Equality-predicate and ORDER BY columns that drive the covering index. */
typedef struct
{
    char eq[TM_MAX_PROJ_COLS][NAMEDATALEN];
    int  n_eq;
    struct { char name[NAMEDATALEN]; bool desc; } sort[TM_MAX_SORT_COLS];
    int  n_sort;
} IndexSpec;

/*
 * Extract a bare column name from a raw ColumnRef.  Returns false (and sets
 * *is_star) for "*"/"t.*"; for "col" or "alias"."col" returns the trailing
 * column name (single-table queries make the alias irrelevant).
 */
static bool
tm_colref_name(ColumnRef *cr, char *out, size_t outlen, bool *is_star)
{
    Node *last;

    *is_star = false;
    if (cr->fields == NIL)
        return false;

    last = (Node *) llast(cr->fields);
    if (IsA(last, A_Star))
    {
        *is_star = true;
        return false;
    }
    if (IsA(last, String))
    {
        strlcpy(out, strVal(last), outlen);
        return true;
    }
    return false;
}

/* raw_expression_tree_walker callback: collect ColumnRef names into a ColRefSet. */
static bool
tm_collect_colrefs_walker(Node *node, void *context)
{
    ColRefSet *s = (ColRefSet *) context;

    if (node == NULL)
        return false;

    if (IsA(node, ColumnRef))
    {
        char nm[NAMEDATALEN];
        bool star;
        int  i;

        if (!tm_colref_name((ColumnRef *) node, nm, sizeof(nm), &star))
        {
            if (star)
                s->has_star = true;
            return false;
        }
        for (i = 0; i < s->n; i++)
            if (pg_strcasecmp(s->names[i], nm) == 0)
                return false;       /* already recorded */
        if (s->n >= TM_MAX_PROJ_COLS)
        {
            s->overflow = true;
            return false;
        }
        strlcpy(s->names[s->n++], nm, NAMEDATALEN);
        return false;
    }

    return raw_expression_tree_walker(node, tm_collect_colrefs_walker, context);
}

static void
tm_add_eq_col(IndexSpec *ix, const char *name)
{
    int i;

    for (i = 0; i < ix->n_eq; i++)
        if (pg_strcasecmp(ix->eq[i], name) == 0)
            return;
    if (ix->n_eq < TM_MAX_PROJ_COLS)
        strlcpy(ix->eq[ix->n_eq++], name, NAMEDATALEN);
}

/*
 * Walk a raw WHERE clause collecting columns that appear on one side of a
 * "col = <non-column>" equality (the useful leading index columns).  Only AND
 * conjuncts are descended into; OR branches are skipped (an index on them would
 * not be guaranteed usable).
 */
static void
tm_collect_eq_cols(Node *node, IndexSpec *ix)
{
    if (node == NULL)
        return;

    if (IsA(node, BoolExpr))
    {
        BoolExpr *b = (BoolExpr *) node;
        ListCell *lc;

        if (b->boolop == AND_EXPR)
            foreach(lc, b->args)
                tm_collect_eq_cols((Node *) lfirst(lc), ix);
        return;
    }

    if (IsA(node, A_Expr))
    {
        A_Expr *ae = (A_Expr *) node;
        char    nm[NAMEDATALEN];
        bool    star;

        if (ae->kind != AEXPR_OP || list_length(ae->name) != 1 ||
            strcmp(strVal(linitial(ae->name)), "=") != 0)
            return;

        if (ae->lexpr && IsA(ae->lexpr, ColumnRef) &&
            !(ae->rexpr && IsA(ae->rexpr, ColumnRef)) &&
            tm_colref_name((ColumnRef *) ae->lexpr, nm, sizeof(nm), &star) && !star)
            tm_add_eq_col(ix, nm);
        else if (ae->rexpr && IsA(ae->rexpr, ColumnRef) &&
                 !(ae->lexpr && IsA(ae->lexpr, ColumnRef)) &&
                 tm_colref_name((ColumnRef *) ae->rexpr, nm, sizeof(nm), &star) && !star)
            tm_add_eq_col(ix, nm);
    }
}

/* Collect ORDER BY columns (with direction) for the covering index. */
static void
tm_collect_sort_cols(List *sortClause, IndexSpec *ix)
{
    ListCell *lc;

    foreach(lc, sortClause)
    {
        SortBy *sb = lfirst_node(SortBy, lc);
        char    nm[NAMEDATALEN];
        bool    star;

        if (sb->node && IsA(sb->node, ColumnRef) &&
            tm_colref_name((ColumnRef *) sb->node, nm, sizeof(nm), &star) && !star)
        {
            if (ix->n_sort >= TM_MAX_SORT_COLS)
                return;
            strlcpy(ix->sort[ix->n_sort].name, nm, NAMEDATALEN);
            ix->sort[ix->n_sort].desc = (sb->sortby_dir == SORTBY_DESC);
            ix->n_sort++;
        }
    }
}

/*
 * Plan a column-subset IMMV for single-table candidate `tbl` from its hottest
 * query text `query_text` (normalized text straight out of pg_stat_statements).
 *
 * On success returns true and fills:
 *   proj    — comma-separated, quoted projection column list (attno order)
 *   recs    — (src_attno, name) for each projected column; *nrecs set
 *   idxcols — comma-separated covering-index column list (may be empty)
 *
 * Returns false — caller falls back to a full "SELECT *" mirror — when the
 * query is not a simple single-table SELECT on `tbl`, uses "*"/whole-row refs,
 * references a column with attno >= 64 (cannot be col-mapped), or no referenced
 * column resolves to a live base column.
 *
 * Runs raw_parser, which can ereport on truncated/odd text; the caller wraps
 * the call in a subtransaction.
 */
static bool
tm_plan_single_projection(const char *tbl, const char *query_text,
                          StringInfo proj, ProjCol *recs, int *nrecs,
                          StringInfo idxcols)
{
    List       *pl;
    RawStmt    *rs;
    SelectStmt *sel;
    RangeVar   *rv;
    ColRefSet   set;
    IndexSpec   ix;
    Oid         argt[1];
    Datum       argv[1];
    int         ret, i, c;
    int         pos = 0;

    memset(&set, 0, sizeof(set));
    memset(&ix, 0, sizeof(ix));
    *nrecs = 0;

    pl = raw_parser(query_text, RAW_PARSE_DEFAULT);
    if (list_length(pl) != 1)
        return false;
    rs = linitial_node(RawStmt, pl);
    if (!IsA(rs->stmt, SelectStmt))
        return false;
    sel = (SelectStmt *) rs->stmt;

    /* Plain SELECT over exactly the one base table (no set ops, DISTINCT,
     * join, or subselect/CTE in FROM). */
    if (sel->op != SETOP_NONE || sel->distinctClause != NIL)
        return false;
    if (list_length(sel->fromClause) != 1 ||
        !IsA(linitial(sel->fromClause), RangeVar))
        return false;
    rv = linitial_node(RangeVar, sel->fromClause);
    if (rv->relname == NULL || pg_strcasecmp(rv->relname, tbl) != 0)
        return false;

    /* Gather every column the query references. */
    raw_expression_tree_walker((Node *) sel->targetList,
                               tm_collect_colrefs_walker, &set);
    if (sel->whereClause)
        raw_expression_tree_walker(sel->whereClause,
                                   tm_collect_colrefs_walker, &set);
    if (sel->sortClause)
        raw_expression_tree_walker((Node *) sel->sortClause,
                                   tm_collect_colrefs_walker, &set);
    if (sel->groupClause)
        raw_expression_tree_walker((Node *) sel->groupClause,
                                   tm_collect_colrefs_walker, &set);
    if (sel->havingClause)
        raw_expression_tree_walker(sel->havingClause,
                                   tm_collect_colrefs_walker, &set);

    if (set.has_star || set.overflow || set.n == 0)
        return false;

    tm_collect_eq_cols(sel->whereClause, &ix);
    tm_collect_sort_cols(sel->sortClause, &ix);

    /* Resolve referenced names to live base-table attnos, in attno order. */
    argt[0] = TEXTOID;
    argv[0] = CStringGetTextDatum(tbl);
    ret = SPI_execute_with_args(
            "SELECT a.attnum::int, a.attname::text "
            "FROM pg_attribute a "
            "WHERE a.attrelid = ('public.' || quote_ident($1))::regclass "
            "  AND a.attnum > 0 AND NOT a.attisdropped "
            "ORDER BY a.attnum",
            1, argt, argv, " ", true, 0);
    if (ret != SPI_OK_SELECT || SPI_processed == 0)
        return false;

    for (i = 0; i < (int) SPI_processed; i++)
    {
        bool  isn;
        int   attnum = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i],
                                        SPI_tuptable->tupdesc, 1, &isn));
        char *aname = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[i],
                                        SPI_tuptable->tupdesc, 2, &isn));
        bool  used = false;
        int   k;

        for (k = 0; k < set.n; k++)
            if (pg_strcasecmp(set.names[k], aname) == 0)
            {
                used = true;
                break;
            }
        if (!used)
            continue;
        if (attnum >= 64)
            return false;       /* cannot represent in col_map */

        if (pos > 0)
            appendStringInfoString(proj, ", ");
        appendStringInfoString(proj, quote_identifier(aname));
        recs[pos].src_attno = attnum;
        strlcpy(recs[pos].name, aname, NAMEDATALEN);
        pos++;
    }

    if (pos == 0)
        return false;
    *nrecs = pos;

    /* Covering-index columns: equality columns first (leading, ASC), then
     * ORDER BY columns (with direction).  Only projected columns are eligible. */
    c = 0;
    for (i = 0; i < ix.n_eq; i++)
    {
        int  k;
        bool projected = false;

        for (k = 0; k < pos; k++)
            if (pg_strcasecmp(recs[k].name, ix.eq[i]) == 0) { projected = true; break; }
        if (!projected)
            continue;
        if (c++ > 0)
            appendStringInfoString(idxcols, ", ");
        appendStringInfoString(idxcols, quote_identifier(ix.eq[i]));
    }
    for (i = 0; i < ix.n_sort; i++)
    {
        int  k;
        bool dup = false, projected = false;

        for (k = 0; k < ix.n_eq; k++)
            if (pg_strcasecmp(ix.eq[k], ix.sort[i].name) == 0) { dup = true; break; }
        if (dup)
            continue;
        for (k = 0; k < pos; k++)
            if (pg_strcasecmp(recs[k].name, ix.sort[i].name) == 0) { projected = true; break; }
        if (!projected)
            continue;
        if (c++ > 0)
            appendStringInfoString(idxcols, ", ");
        appendStringInfoString(idxcols, quote_identifier(ix.sort[i].name));
        if (ix.sort[i].desc)
            appendStringInfoString(idxcols, " DESC");
    }

    return true;
}

/*
 * Build the col_map for a single-table IMMV by matching the IMMV's columns to
 * the base table's columns by name (pg_ivm may add trailing "__ivm_*" hidden
 * columns, which do not match any base name and are ignored).  Works whether
 * the IMMV was just created or already existed on disk.
 *
 * col_map[src_attno] = mv_attno for each shared column.  *has_map is set false
 * only for a true identity full mirror (every base column present, same
 * positions) so that case keeps the cheap no-remap rewrite path; any projection
 * or reordering sets it true.
 */
static void
tm_build_single_colmap(const char *mv_name, const char *tbl,
                       int col_map[64], bool *has_map)
{
    Oid   argt[2] = {TEXTOID, TEXTOID};
    Datum argv[2];
    int   ret, i, nmapped = 0, base_natts = -1;
    bool  identity = true;

    memset(col_map, 0, sizeof(int) * 64);
    *has_map = false;

    argv[0] = CStringGetTextDatum(mv_name);
    argv[1] = CStringGetTextDatum(tbl);
    ret = SPI_execute_with_args(
            "WITH base AS ("
            "  SELECT attnum::int AS n, attname FROM pg_attribute"
            "  WHERE attrelid = ('public.' || quote_ident($2))::regclass"
            "    AND attnum > 0 AND NOT attisdropped)"
            " SELECT mv.attnum::int AS mv_n, base.n AS src_n,"
            "        (SELECT count(*)::int FROM base) AS base_natts"
            " FROM pg_attribute mv JOIN base ON base.attname = mv.attname"
            " WHERE mv.attrelid = ('public.' || quote_ident($1))::regclass"
            "   AND mv.attnum > 0 AND NOT mv.attisdropped"
            " ORDER BY mv.attnum",
            2, argt, argv, " ", true, 0);
    if (ret != SPI_OK_SELECT || SPI_processed == 0)
        return;

    for (i = 0; i < (int) SPI_processed; i++)
    {
        bool isn;
        int  mv_n  = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i],
                                    SPI_tuptable->tupdesc, 1, &isn));
        int  src_n = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i],
                                    SPI_tuptable->tupdesc, 2, &isn));
        base_natts = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i],
                                    SPI_tuptable->tupdesc, 3, &isn));

        if (src_n < 64)
        {
            col_map[src_n] = mv_n;
            nmapped++;
            if (mv_n != src_n)
                identity = false;
        }
        else
            identity = false;   /* base attno we cannot map */
    }

    /* Identity full mirror (same columns, same positions) needs no remap. */
    *has_map = !(identity && nmapped == base_natts);
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
        "    query AS qtext,"
        "    " HEURISTIC_SCORE_EXPR " AS score"
        "  FROM pg_stat_statements"
        "  WHERE upper(ltrim(query)) LIKE 'SELECT%%'"
        "    AND calls          >= %d"
        "    AND mean_exec_time >= %.4f"
        "), per_tbl AS ("
        "  SELECT DISTINCT ON (tbl) tbl, qtext, score"
        "  FROM ranked"
        "  WHERE tbl IS NOT NULL"
        "    AND tbl NOT LIKE 'pg_%%'"
        "  ORDER BY tbl, score DESC"
        ")"
        " SELECT tbl, qtext"
        " FROM per_tbl"
        " ORDER BY score DESC"
        " LIMIT %d";

    char  heuristic_sql[1024];
    /* Copied out before Phase 2 SPI calls overwrite SPI_tuptable. */
    char  candidates[MV_REGISTRY_MAX][NAMEDATALEN];
    /* The candidate's hottest query text — drives column-subset projection. */
    char  cand_query[MV_REGISTRY_MAX][QUERY_LEN];
    int   num_candidates = 0;
    int   created = 0;
    int   ret;
    int   i;

    /*
     * ---- Score + evict pass ----
     * Refresh per-table recent-activity scores, then drop any IMMV whose
     * source tables have gone cold (workload shifted away).  Runs before
     * creation so freed budget can be reused by newly-hot tables this tick.
     */
    update_table_scores();
    tm_evict_mvs();

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

    /* ---- Phase 1: collect candidate table names + their hottest query ---- */
    ret = SPI_execute(heuristic_sql, true, max_mv_count);
    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        int nrows = (int) SPI_processed;

        for (i = 0; i < nrows && num_candidates < MV_REGISTRY_MAX; i++)
        {
            bool  isnull;
            Datum d = SPI_getbinval(SPI_tuptable->vals[i],
                                    SPI_tuptable->tupdesc, 1, &isnull);
            Datum q;
            bool  qnull;

            if (isnull)
                continue;
            strlcpy(candidates[num_candidates],
                    TextDatumGetCString(d), NAMEDATALEN);

            q = SPI_getbinval(SPI_tuptable->vals[i],
                              SPI_tuptable->tupdesc, 2, &qnull);
            if (!qnull)
                strlcpy(cand_query[num_candidates],
                        TextDatumGetCString(q), QUERY_LEN);
            else
                cand_query[num_candidates][0] = '\0';

            num_candidates++;
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
        int         colmap_local[64];
        bool        has_colmap = false;
        bool        colmap_built = false;

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

        /*
         * Recency gate: do not (re-)create an IMMV for a table whose recent
         * activity has decayed cold relative to its peak, even if its
         * cumulative pg_stat_statements score still ranks it.  Prevents
         * recreating a view the evictor just dropped after a workload shift.
         */
        if (!already_registered && tm_table_is_cold(tbl))
            continue;

        /*
         * Respect the max_materialized_views budget.  If the registry is full,
         * only proceed when this candidate is hot enough to displace the
         * weakest incumbent (which tm_try_displace then drops).  Otherwise skip
         * it this tick rather than growing the set past the budget.
         */
        if (!already_registered && !tm_try_displace(tm_score_for_table(tbl)))
            continue;

        if (!immv_exists)
        {
            StringInfoData proj, idxcols, def;
            ProjCol        recs[TM_MAX_PROJ_COLS];
            int            nrecs = 0;
            bool           did_project = false;

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
             * Plan a column subset from the candidate's hottest query.  The
             * raw_parser call can throw on truncated/odd text, so run it inside
             * a subtransaction; on any failure we fall back to a full mirror.
             */
            initStringInfo(&proj);
            initStringInfo(&idxcols);

            if (cand_query[i][0] != '\0')
            {
                MemoryContext old = CurrentMemoryContext;

                BeginInternalSubTransaction(NULL);
                MemoryContextSwitchTo(old);
                PG_TRY();
                {
                    did_project = tm_plan_single_projection(tbl, cand_query[i],
                                                            &proj, recs, &nrecs,
                                                            &idxcols);
                    ReleaseCurrentSubTransaction();
                    MemoryContextSwitchTo(old);
                }
                PG_CATCH();
                {
                    MemoryContextSwitchTo(old);
                    FlushErrorState();
                    RollbackAndReleaseCurrentSubTransaction();
                    MemoryContextSwitchTo(old);
                    did_project = false;
                }
                PG_END_TRY();
            }

            /*
             * Create the IMMV via pg_ivm.  format('%I', ...) safely quotes the
             * IMMV name; the projection column list and FROM target are built
             * from quote_identifier() output so they are injection-safe too.
             *
             * create_immv() populates the view immediately and installs
             * triggers on the source table that keep it incrementally
             * up-to-date on every INSERT/UPDATE/DELETE — no REFRESH needed.
             */
            initStringInfo(&def);
            if (did_project)
                appendStringInfo(&def, "SELECT %s FROM public.%s",
                                 proj.data, quote_identifier(tbl));
            else
                appendStringInfo(&def, "SELECT * FROM public.%s",
                                 quote_identifier(tbl));

            arg_types[0]  = TEXTOID;
            arg_types[1]  = TEXTOID;
            arg_values[0] = CStringGetTextDatum(mv_name);
            arg_values[1] = CStringGetTextDatum(def.data);
            ret = SPI_execute_with_args(
                    "SELECT pgivm.create_immv(format('public.%I', $1), $2)",
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
                    (errmsg("table_materializer: created %s IMMV "
                            "public.%s for table public.%s",
                            did_project ? "column-subset" : "full-mirror",
                            mv_name, tbl)));

            /*
             * Covering index on the predicate/sort columns so the IMMV answers
             * "WHERE x = ? ORDER BY y LIMIT n" with an index scan rather than a
             * sequential scan + sort.  pg_ivm IMMVs are plain tables, so the
             * index is maintained automatically by the maintenance triggers.
             */
            if (did_project && idxcols.len > 0)
            {
                StringInfoData isql;
                char           idxname[NAMEDATALEN + 24];

                snprintf(idxname, sizeof(idxname), "%s_cov_idx", mv_name);
                initStringInfo(&isql);
                appendStringInfo(&isql,
                                 "CREATE INDEX IF NOT EXISTS %s ON public.%s (%s)",
                                 quote_identifier(idxname),
                                 quote_identifier(mv_name), idxcols.data);

                if (SPI_execute(isql.data, false, 0) < 0)
                    ereport(LOG,
                            (errmsg("table_materializer: covering index on "
                                    "public.%s failed, continuing without it",
                                    mv_name)));
                else
                    ereport(DEBUG1,
                            (errmsg("table_materializer: built covering index "
                                    "%s (%s)", idxname, idxcols.data)));
            }

            /*
             * Build the col_map for the just-created projection via get_attnum
             * (syscache), NOT an MVCC heap scan of pg_attribute: the IMMV was
             * created in this same transaction, so a heap scan under the active
             * snapshot may not see its catalog rows yet and would yield an empty
             * map — which would then be mis-registered as a full mirror and
             * crash the planner with un-remapped base attnos.  The syscache is
             * command-counter-consistent and sees the new columns immediately.
             */
            if (did_project)
            {
                RangeVar mvrv;
                Oid      mv_oid;
                int      r;

                memset(colmap_local, 0, sizeof(colmap_local));
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
                        AttrNumber mva = get_attnum(mv_oid, recs[r].name);

                        if (AttributeNumberIsValid(mva) && recs[r].src_attno < 64)
                            colmap_local[recs[r].src_attno] = (int) mva;
                    }
                    has_colmap   = true;
                    colmap_built = true;
                }
            }
        }

        /*
         * For a full-mirror create or a pre-existing IMMV (committed catalog),
         * derive the col_map from the IMMV's actual columns.  A projection sets
         * has_colmap = true; a true identity full mirror leaves it false to keep
         * the cheap no-remap rewrite path.
         */
        if (!colmap_built)
            tm_build_single_colmap(mv_name, tbl, colmap_local, &has_colmap);

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
            entry->has_col_map = has_colmap;
            if (has_colmap)
                memcpy(entry->col_map[0], colmap_local,
                       sizeof(entry->col_map[0]));
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

/* ----------------------------------------------------------------
 * SQL-callable function: table_materializer_list_mvs()
 *
 * Returns the current auto-created IMMV registry with each entry's live
 * recent-activity score and cold-tick counter, for observing how the selected
 * set shifts as the workload changes.
 * ---------------------------------------------------------------- */

/* One row of the list_materialized_views() result snapshot. */
typedef struct
{
    char   mv_name[MV_NAME_LEN];
    int    num_source_tables;
    double score;
    int    cold_ticks;
} MVListRow;

Datum
list_materialized_views(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    MVListRow       *rows;

    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext   oldctx;
        TupleDesc       tupdesc;
        MVRegistryEntry snap[MV_REGISTRY_MAX];
        int             n = 0;
        int             i;

        funcctx = SRF_FIRSTCALL_INIT();
        oldctx  = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        if (mv_registry_state != NULL)
        {
            LWLockAcquire(&top_queries_locks[1].lock, LW_SHARED);
            n = mv_registry_state->num_entries;
            if (n > 0)
                memcpy(snap, mv_registry_state->entries,
                       n * sizeof(MVRegistryEntry));
            LWLockRelease(&top_queries_locks[1].lock);
        }

        rows = (MVListRow *) palloc0(sizeof(MVListRow) * (n > 0 ? n : 1));
        for (i = 0; i < n; i++)
        {
            strlcpy(rows[i].mv_name, snap[i].mv_name, MV_NAME_LEN);
            rows[i].num_source_tables = snap[i].num_source_tables;
            rows[i].score             = tm_score_for_entry(&snap[i]);
            rows[i].cold_ticks        = snap[i].cold_ticks;
        }

        funcctx->user_fctx = rows;
        funcctx->max_calls = n;

        tupdesc = CreateTemplateTupleDesc(4);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "mv_name",
                           TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "num_source_tables",
                           INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "score",
                           FLOAT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "cold_ticks",
                           INT4OID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        MemoryContextSwitchTo(oldctx);
    }

    funcctx = SRF_PERCALL_SETUP();
    rows    = (MVListRow *) funcctx->user_fctx;

    if (funcctx->call_cntr < funcctx->max_calls)
    {
        int       i = funcctx->call_cntr;
        Datum     values[4];
        bool      nulls[4] = {false, false, false, false};
        HeapTuple tuple;

        values[0] = CStringGetTextDatum(rows[i].mv_name);
        values[1] = Int32GetDatum(rows[i].num_source_tables);
        values[2] = Float8GetDatum(rows[i].score);
        values[3] = Int32GetDatum(rows[i].cold_ticks);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
}
