#include <errno.h>
#include <sys/wait.h>
#include <unistd.h>

#include "cli_common.h"
#include "cli_root.h"
#include "commandline.h"
#include "defaults.h"
#include "env_utils.h"
#include "log.h"
#include "pgsql.h"
#include "string_utils.h"

static void cli_internal_log_sample(int argc, char **argv);
static void cli_internal_log_stress(int argc, char **argv);

static CommandLine log_sample_command =
	make_command("log-sample",
				 "Write a fixed set of log records at every level",
				 "", "", NULL, cli_internal_log_sample);

static CommandLine log_stress_command =
	make_command("log-stress",
				 "Write log records from many processes at the same time",
				 "<procs> <count> <size>", "", NULL, cli_internal_log_stress);

static void cli_internal_log_extra(int argc, char **argv);
static void cli_internal_pg_errors(int argc, char **argv);

static CommandLine log_extra_command =
	make_command("log-extra",
				 "Write log records that the old logger could not write",
				 "", "", NULL, cli_internal_log_extra);

static CommandLine pg_errors_command =
	make_command("pg-errors",
				 "Cause Postgres errors on the source to show how they are logged",
				 "", "", NULL, cli_internal_pg_errors);

static CommandLine *internal_subcommands[] = {
	&log_sample_command,
	&log_stress_command,
	&log_extra_command,
	&pg_errors_command,
	NULL
};

CommandLine internal_commands =
	make_command_set("internal",
					 "Commands used by the test suites",
					 NULL, NULL, NULL, internal_subcommands);


static char *
repeat_char(char c, int size)
{
	char *buffer = (char *) calloc(size + 1, sizeof(char));

	if (buffer == NULL)
	{
		log_fatal(ALLOCATION_FAILED_ERROR);
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	memset(buffer, c, size);

	return buffer;
}


static void
cli_internal_log_sample(int argc, char **argv)
{
	log_set_level(LOG_TRACE);

	log_trace("level sample: %s", "trace");
	log_sqlite("level sample: %s", "sqlite");
	log_debug("level sample: %s", "debug");
	log_sql("level sample: %s", "sql");
	log_notice("level sample: %s", "notice");
	log_info("level sample: %s", "info");
	log_warn("level sample: %s", "warn");
	log_error("level sample: %s", "error");
	log_fatal("level sample: %s", "fatal");

	char *medium = repeat_char('m', 1500);
	log_info("size sample 1500: %s", medium);

	errno = ENOENT;
	log_error("errno sample: %m");

	log_warn("multi-line sample: line one\nline two");
	log_info("json sample: \"quoted\" back\\slash tab\there /path/to \x01 end");
	log_info("utf8 sample: caf\xc3\xa9");
	log_level(LOG_NOTICE, "log_level sample: %d", 42);
}


static void
cli_internal_log_extra(int argc, char **argv)
{
	char *large = repeat_char('x', 20000);
	log_info("large sample: %s END", large);

	log_info("invalid utf8 sample: \xff\xfe end");

	errno = EACCES;
	log_info("errno kept sample: first");
	log_info("errno kept sample: %m");

	for (int i = 0; i < 1000; i++)
	{
		log_info("closed pipe sample: %d", i);
	}

	log_info("log-extra done");
}


static void
cli_internal_pg_errors(int argc, char **argv)
{
	char pguri[MAXCONNINFO] = { 0 };

	if (!get_env_copy(PGCOPYDB_SOURCE_PGURI, pguri, sizeof(pguri)))
	{
		log_fatal("Failed to read %s", PGCOPYDB_SOURCE_PGURI);
		exit(EXIT_CODE_BAD_ARGS);
	}

	PGSQL pgsql = { 0 };

	if (!pgsql_init(&pgsql, pguri, PGSQL_CONN_SOURCE))
	{
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	(void) pgsql_execute(&pgsql,
						 "do $$ begin raise exception 'sample error' "
						 "using detail = 'sample detail', hint = 'sample hint'; "
						 "end $$");

	const Oid paramTypes[1] = { INT4OID };
	const char *paramValues[1] = { "42" };

	(void) pgsql_execute_with_params(&pgsql, "select $1::int / 0",
									 1, paramTypes, paramValues,
									 NULL, NULL);

	if (pgsql_open_connection(&pgsql) != NULL)
	{
		(void) pg_copy_from_stdin(&pgsql, "public.logging_does_not_exist");
	}

	PGSQL bad = { 0 };

	if (!pgsql_init(&bad,
					"postgres://postgres@127.0.0.1:1/postgres",
					PGSQL_CONN_TARGET))
	{
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	pgsql_set_retry_policy(&(bad.retryPolicy), 0, 0, 0, 0);
	(void) pgsql_open_connection(&bad);

	log_info("pg-errors done");
}


static void
cli_internal_log_stress(int argc, char **argv)
{
	int procs = 0;
	int count = 0;
	int size = 0;

	if (argc != 3 ||
		!stringToInt(argv[0], &procs) ||
		!stringToInt(argv[1], &count) ||
		!stringToInt(argv[2], &size) ||
		procs < 1 || count < 1 || size < 0)
	{
		commandline_print_usage(&log_stress_command, stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	fflush(stdout);
	fflush(stderr);

	for (int p = 0; p < procs; p++)
	{
		pid_t fpid = fork();

		switch (fpid)
		{
			case -1:
			{
				log_fatal("Failed to fork a log stress process: %m");
				exit(EXIT_CODE_INTERNAL_ERROR);
			}

			case 0:
			{
				for (int seq = 0; seq < count; seq++)
				{
					char *payload = repeat_char('a' + (seq % 26), size);

					log_info("stress pid=%d seq=%d size=%d payload=%s END",
							 getpid(), seq, size, payload);

					free(payload);
				}

				exit(EXIT_CODE_QUIT);
			}

			default:
			{
				break;
			}
		}
	}

	int errors = 0;

	for (int p = 0; p < procs; p++)
	{
		int status;

		if (wait(&status) == -1)
		{
			log_fatal("Failed to wait for a log stress process: %m");
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
		{
			++errors;
		}
	}

	exit(errors == 0 ? EXIT_CODE_QUIT : EXIT_CODE_INTERNAL_ERROR);
}
