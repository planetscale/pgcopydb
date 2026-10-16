#include "copydb.h"
#include "log.h"
#include "pgsql.h"
#include "string_utils.h"
#include "target_state.h"


static bool
target_start_time(PGSQL *dst, char *startTime, size_t size)
{
	SingleValueResultContext context = { { 0 }, PGSQL_RESULT_STRING, false };

	if (!pgsql_execute_with_params(dst,
								   "select pg_postmaster_start_time()::text",
								   0, NULL, NULL,
								   &context, &parseSingleValueResult) ||
		!context.parsedOk ||
		context.strVal == NULL)
	{
		return false;
	}

	strlcpy(startTime, context.strVal, size);

	return true;
}


static void
target_log_index_memory(CopyDataSpec *specs, PGSQL *dst)
{
	SingleValueResultContext budget = { { 0 }, PGSQL_RESULT_STRING, false };

	char jobs[BUFSIZE] = { 0 };
	sformat(jobs, sizeof(jobs), "%d", specs->indexJobs);

	const char *paramValues[1] = { jobs };

	const char *sql =
		"select format('CREATE INDEX memory on the target: %s processes, "
		"maintenance_work_mem %s, max_parallel_maintenance_workers %s, "
		"total up to %s', "
		"$1::bigint, "
		"current_setting('maintenance_work_mem'), "
		"current_setting('max_parallel_maintenance_workers'), "
		"pg_size_pretty(pg_size_bytes(current_setting('maintenance_work_mem')) "
		"* $1::bigint))";

	if (pgsql_execute_with_params(dst, sql, 1, NULL, paramValues,
								  &budget, &parseSingleValueResult) &&
		budget.parsedOk &&
		budget.strVal != NULL)
	{
		log_info("%s", budget.strVal);
	}
	else
	{
		log_warn("Failed to compute the CREATE INDEX memory on the target");
	}
}


void
copydb_target_inspect(CopyDataSpec *specs)
{
	if (!IS_EMPTY_STRING_BUFFER(specs->targetStartTime))
	{
		return;
	}

	PGSQL dst = { 0 };

	if (!pgsql_init(&dst, specs->connStrings.target_pguri, PGSQL_CONN_TARGET) ||
		!pgsql_set_gucs(&dst, dstSettings))
	{
		log_warn("Failed to read the target server state");
		(void) pgsql_finish(&dst);
		return;
	}

	if (target_start_time(&dst,
						  specs->targetStartTime,
						  sizeof(specs->targetStartTime)))
	{
		log_info("Target server is up since %s", specs->targetStartTime);
	}
	else
	{
		log_warn("Failed to read the target server start time");
	}

	(void) target_log_index_memory(specs, &dst);
	(void) pgsql_finish(&dst);
}


void
copydb_target_report(CopyDataSpec *specs, PGSQL *failed)
{
	if (!pgsql_state_is_connection_error(failed))
	{
		return;
	}

	log_error("Lost the target connection (SQLSTATE %s), "
			  "checking the target server state",
			  IS_EMPTY_STRING_BUFFER(failed->sqlstate) ?
			  "unknown" : failed->sqlstate);

	PGSQL dst = { 0 };
	char startTime[BUFSIZE] = { 0 };
	SingleValueResultContext recovery = { { 0 }, PGSQL_RESULT_BOOL, false };

	if (!pgsql_init(&dst, specs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		return;
	}

	dst.connectionStatementType = PGSQL_CONNECTION_MULTI_STATEMENT;

	if (!target_start_time(&dst, startTime, sizeof(startTime)) ||
		!pgsql_execute_with_params(&dst, "select pg_is_in_recovery()",
								   0, NULL, NULL,
								   &recovery, &parseSingleValueResult) ||
		!recovery.parsedOk)
	{
		log_error("Failed to connect to the target server again: "
				  "the server is down, restarting, or failing over");
		(void) pgsql_finish(&dst);
		return;
	}

	(void) pgsql_finish(&dst);

	if (IS_EMPTY_STRING_BUFFER(specs->targetStartTime))
	{
		log_error("Target server is up since %s", startTime);
	}
	else if (!streq(startTime, specs->targetStartTime))
	{
		log_error("Target server restarted at %s, it was up since %s "
				  "before this step started",
				  startTime,
				  specs->targetStartTime);
	}
	else
	{
		log_error("Target server did not restart, it is up since %s: "
				  "a process on the target terminated this session, "
				  "for example pg_terminate_backend()",
				  startTime);
	}

	if (recovery.boolVal)
	{
		log_error("Target server is in recovery: a failover can be in progress");
	}
}
