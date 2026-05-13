#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"
#include "portability/instr_time.h"

PG_MODULE_MAGIC;

static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type   prev_ExecutorEnd   = NULL;

static instr_time query_start_time;

static void timing_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void timing_ExecutorEnd(QueryDesc *queryDesc);

void _PG_init(void);
void _PG_fini(void);

void
_PG_init(void)
{
    prev_ExecutorStart = ExecutorStart_hook;
    ExecutorStart_hook = timing_ExecutorStart;

    prev_ExecutorEnd = ExecutorEnd_hook;
    ExecutorEnd_hook = timing_ExecutorEnd;
}

void
_PG_fini(void)
{
    ExecutorStart_hook = prev_ExecutorStart;
    ExecutorEnd_hook   = prev_ExecutorEnd;
}

static void
timing_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    INSTR_TIME_SET_CURRENT(query_start_time);

    if (prev_ExecutorStart)
        prev_ExecutorStart(queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);
}

static void
timing_ExecutorEnd(QueryDesc *queryDesc)
{
    instr_time end_time;
    double     elapsed_ms;

    INSTR_TIME_SET_CURRENT(end_time);
    INSTR_TIME_SUBTRACT(end_time, query_start_time);
    elapsed_ms = INSTR_TIME_GET_MILLISEC(end_time);

    elog(NOTICE, "Query latency: %.3f ms", elapsed_ms);

    if (prev_ExecutorEnd)
        prev_ExecutorEnd(queryDesc);
    else
        standard_ExecutorEnd(queryDesc);
}
