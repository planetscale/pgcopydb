/*
 * src/bin/pgcopydb/snapshot.c
 *     Implementation of a CLI to copy a database between two Postgres instances
 */

#include <errno.h>
#include <inttypes.h>

#include "copydb.h"
#include "log.h"
#include "pgsql.h"

/*
 * XID wraparound proximity thresholds expressed as percentages of the
 * 2^31 (2,147,483,648) XID space. A REPEATABLE READ snapshot pins xmin,
 * preventing vacuum from freezing tuples, which can push a database
 * toward transaction ID wraparound.
 */
#define XID_WRAPAROUND_WARN_PCT 75
#define XID_WRAPAROUND_FAIL_PCT 95
#define XID_WRAPAROUND_LIMIT 2147483648ULL

/*
 * copydb_copy_snapshot initializes a new TransactionSnapshot from another
 * snapshot that's been exported already, copying the connection string and the
 * snapshot identifier.
 */
bool
copydb_copy_snapshot(CopyDataSpec *specs, TransactionSnapshot *snapshot)
{
	PGSQL pgsql = { 0 };
	TransactionSnapshot *source = &(specs->sourceSnapshot);

	/* copy our source snapshot data into the new snapshot instance */
	snapshot->pgsql = pgsql;
	snapshot->connectionType = source->connectionType;

	/* this is set at set/export/CREATE_REPLICATION_SLOT time */
	snapshot->kind = SNAPSHOT_KIND_UNKNOWN;

	/* remember if the replication slot has been created already */
	snapshot->exportedCreateSlotSnapshot = source->exportedCreateSlotSnapshot;
	snapshot->pguri = strdup(source->pguri);
	strlcpy(snapshot->snapshot, source->snapshot, sizeof(snapshot->snapshot));
	snapshot->isReadOnly = source->isReadOnly;

	return true;
}


/*
 * copydb_open_snapshot opens a snapshot on the given connection.
 *
 * This is needed in the main process, so that COPY processes can then re-use
 * the snapshot, and thus we get a consistent view of the database all along.
 */
bool
copydb_export_snapshot(TransactionSnapshot *snapshot)
{
	PGSQL *pgsql = &(snapshot->pgsql);

	log_debug("copydb_export_snapshot");

	snapshot->kind = SNAPSHOT_KIND_SQL;

	if (!pgsql_init(pgsql, snapshot->pguri, snapshot->connectionType))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * As Postgres docs for SET TRANSACTION SNAPSHOT say:
	 *
	 * Furthermore, the transaction must already be set to SERIALIZABLE or
	 * REPEATABLE READ isolation level (otherwise, the snapshot would be
	 * discarded immediately, since READ COMMITTED mode takes a new snapshot
	 * for each command).
	 *
	 * When --filters are used, pgcopydb creates TEMP tables on the source
	 * database to then implement the filtering as JOINs with the Postgres
	 * catalogs. And even TEMP tables need read-write transaction.
	 */
	IsolationLevel level = ISOLATION_REPEATABLE_READ;
	bool deferrable = true;

	if (!pgsql_is_in_recovery(pgsql, &(snapshot->isReadOnly)))
	{
		/* errors have already been logged */
		(void) pgsql_finish(pgsql);
		return false;
	}

	if (!pgsql_begin(pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_set_transaction(pgsql, level, snapshot->isReadOnly, deferrable))
	{
		/* errors have already been logged */
		(void) pgsql_finish(pgsql);
		return false;
	}

	if (!pgsql_export_snapshot(pgsql,
							   snapshot->snapshot,
							   sizeof(snapshot->snapshot)))
	{
		/* errors have already been logged */
		(void) pgsql_finish(pgsql);
		return false;
	}

	snapshot->state = SNAPSHOT_STATE_EXPORTED;

	log_info("Exported snapshot \"%s\" from the source database",
			 snapshot->snapshot);

	/* also set our GUC values for the source connection */
	if (!pgsql_server_version(pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	GUC *settings =
		pgsql->pgversion_num < 90600 ? srcSettings95 : srcSettings;

	if (!pgsql_set_gucs(pgsql, settings))
	{
		log_fatal("Failed to set our GUC settings on the source connection, "
				  "see above for details");
		return false;
	}

	return true;
}


/*
 * copydb_set_snapshot opens a transaction and set it to re-use an existing
 * snapshot.
 */
bool
copydb_set_snapshot(CopyDataSpec *copySpecs)
{
	TransactionSnapshot *snapshot = &(copySpecs->sourceSnapshot);
	PGSQL *pgsql = &(snapshot->pgsql);

	snapshot->kind = SNAPSHOT_KIND_SQL;

	if (!pgsql_init(pgsql, snapshot->pguri, snapshot->connectionType))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * Detect read-only standby if not already known. In clone --follow the
	 * snapshot comes from the logical replication slot and isReadOnly may
	 * not have been set yet. We must use READ ONLY transaction mode on
	 * standbys to avoid "cannot set transaction read-write mode during
	 * recovery" errors.
	 */
	if (!snapshot->isReadOnly)
	{
		if (!pgsql_is_in_recovery(pgsql, &(snapshot->isReadOnly)))
		{
			log_error("Failed to check if source is in recovery");
			(void) pgsql_finish(pgsql);
			return false;
		}
	}

	if (!pgsql_begin(pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	if (copySpecs->consistent)
	{
		/*
		 * As Postgres docs for SET TRANSACTION SNAPSHOT say:
		 *
		 * Furthermore, the transaction must already be set to SERIALIZABLE or
		 * REPEATABLE READ isolation level (otherwise, the snapshot would be
		 * discarded immediately, since READ COMMITTED mode takes a new
		 * snapshot for each command).
		 *
		 * When --filters are used, pgcopydb creates TEMP tables on the source
		 * database to then implement the filtering as JOINs with the Postgres
		 * catalogs. And even TEMP tables need read-write transaction.
		 */
		IsolationLevel level = ISOLATION_REPEATABLE_READ;
		bool deferrable = true;

		if (!pgsql_set_transaction(pgsql, level, snapshot->isReadOnly, deferrable))
		{
			/* errors have already been logged */
			(void) pgsql_finish(pgsql);
			return false;
		}

		if (!pgsql_set_snapshot(pgsql, snapshot->snapshot))
		{
			/* errors have already been logged */
			(void) pgsql_finish(pgsql);
			return false;
		}

		copySpecs->sourceSnapshot.state = SNAPSHOT_STATE_SET;
	}
	else
	{
		copySpecs->sourceSnapshot.state = SNAPSHOT_STATE_NOT_CONSISTENT;
	}

	/* also set our GUC values for the source connection */
	if (!pgsql_server_version(pgsql))
	{
		/* errors have already been logged */
		return false;
	}

	GUC *settings =
		pgsql->pgversion_num < 90600 ? srcSettings95 : srcSettings;

	if (!pgsql_set_gucs(pgsql, settings))
	{
		log_fatal("Failed to set our GUC settings on the source connection, "
				  "see above for details");
		return false;
	}

	return true;
}


/*
 * copydb_close_snapshot closes the snapshot on Postgres by committing the
 * transaction and finishing the connection.
 */
bool
copydb_close_snapshot(CopyDataSpec *copySpecs)
{
	TransactionSnapshot *snapshot = &(copySpecs->sourceSnapshot);
	PGSQL *pgsql = &(snapshot->pgsql);

	if (snapshot->state == SNAPSHOT_STATE_SET ||
		snapshot->state == SNAPSHOT_STATE_EXPORTED ||
		snapshot->state == SNAPSHOT_STATE_NOT_CONSISTENT)
	{
		/* we might need to close our logical stream connection, if any */
		if (snapshot->kind == SNAPSHOT_KIND_LOGICAL)
		{
			(void) pgsql_finish(&(snapshot->stream.pgsql));
		}
		else if (snapshot->kind == SNAPSHOT_KIND_SQL)
		{
			/* only COMMIT sql snapshot kinds, no need for logical rep ones */
			if (!pgsql_commit(pgsql))
			{
				log_fatal("Failed to close snapshot \"%s\" on \"%s\"",
						  snapshot->snapshot,
						  snapshot->safeURI.pguri);
				return false;
			}
		}

		(void) pgsql_finish(pgsql);
	}

	copySpecs->sourceSnapshot.state = SNAPSHOT_STATE_CLOSED;

	if (snapshot->state == SNAPSHOT_STATE_EXPORTED)
	{
		if (!unlink_file(copySpecs->cfPaths.snfile))
		{
			/* errors have already been logged */
			return false;
		}
	}

	return true;
}


/*
 * copydb_check_xid_wraparound queries age(datfrozenxid) on the source
 * database and checks proximity to XID wraparound. Returns false only
 * when the XID age exceeds the fail threshold and --skip-xid-check was
 * not used.
 */
static bool
copydb_check_xid_wraparound(CopyDataSpec *copySpecs)
{
	if (copySpecs->skipXidCheck)
	{
		log_notice("Skipping XID wraparound check per --skip-xid-check");
		return true;
	}

	PGSQL pgsql = { 0 };
	char *pguri = copySpecs->connStrings.source_pguri;

	if (!pgsql_init(&pgsql, pguri, PGSQL_CONN_SOURCE))
	{
		log_warn("Failed to init connection for XID wraparound check, "
				 "skipping");
		return true;
	}

	SingleValueResultContext parseContext = {
		{ 0 }, PGSQL_RESULT_BIGINT, false
	};

	const char *sql =
		"SELECT age(datfrozenxid) FROM pg_database "
		"WHERE datname = current_database()";

	if (!pgsql_execute_with_params(&pgsql, sql,
								   0, NULL, NULL,
								   &parseContext,
								   &parseSingleValueResult))
	{
		log_warn("Failed to query age(datfrozenxid) on source database, "
				 "skipping wraparound check");
		(void) pgsql_finish(&pgsql);
		return true;
	}

	(void) pgsql_finish(&pgsql);

	if (!parseContext.parsedOk || parseContext.isNull)
	{
		log_warn("Could not determine XID age on source database, "
				 "skipping wraparound check");
		return true;
	}

	uint64_t xidAge = parseContext.bigint;
	uint64_t warnThreshold =
		(XID_WRAPAROUND_LIMIT * XID_WRAPAROUND_WARN_PCT) / 100;
	uint64_t failThreshold =
		(XID_WRAPAROUND_LIMIT * XID_WRAPAROUND_FAIL_PCT) / 100;

	double pctUsed =
		(double) xidAge / (double) XID_WRAPAROUND_LIMIT * 100.0;

	if (xidAge >= failThreshold)
	{
		log_error("Source database XID age is %" PRIu64
				  " (%.1f%% of 2^31 limit), "
				  "which is dangerously close to XID wraparound",
				  xidAge, pctUsed);
		log_error("Taking a REPEATABLE READ snapshot would pin xmin and "
				  "prevent vacuum from freezing tuples");
		log_error("Use --skip-xid-check to bypass this safety check");
		return false;
	}

	if (xidAge >= warnThreshold)
	{
		log_warn("Source database XID age is %" PRIu64
				 " (%.1f%% of 2^31 limit)",
				 xidAge, pctUsed);
		log_warn("The clone snapshot will pin xmin, preventing vacuum from "
				 "freezing tuples. Monitor for wraparound during the clone.");
	}
	else
	{
		log_info("Source database XID age is %" PRIu64
				 " (%.1f%% of 2^31 limit)",
				 xidAge, pctUsed);
	}

	return true;
}


/*
 * copydb_prepare_snapshot connects to the source database and either export a
 * new Postgres snapshot, or set the transaction's snapshot to the given
 * already exported snapshot (see --snapshot and PGCOPYDB_SNAPSHOT).
 */
bool
copydb_prepare_snapshot(CopyDataSpec *copySpecs)
{
	/*
	 * Allow this function to be called within a context where a snapshot has
	 * already been prepared. Typically copydb_fetch_schema_and_prepare_specs
	 * needs to prepare the snapshot, but some higher-level functions already
	 * did.
	 */
	if (copySpecs->sourceSnapshot.state != SNAPSHOT_STATE_UNKNOWN &&
		copySpecs->sourceSnapshot.state != SNAPSHOT_STATE_CLOSED)
	{
		log_debug("copydb_prepare_snapshot: snapshot \"%s\" already prepared, "
				  "skipping",
				  copySpecs->sourceSnapshot.snapshot);
		return true;
	}

	/* when --not-consistent is used, we have nothing to do here */
	if (!copySpecs->consistent)
	{
		copySpecs->sourceSnapshot.state = SNAPSHOT_STATE_SKIPPED;
		log_debug("copydb_prepare_snapshot: --not-consistent, skipping");
		return true;
	}

	/* check XID wraparound proximity before pinning xmin */
	if (!copydb_check_xid_wraparound(copySpecs))
	{
		/* errors have already been logged */
		return false;
	}

	/*
	 * First, we need to open a snapshot that we're going to re-use in all our
	 * connections to the source database. When the --snapshot option has been
	 * used, instead of exporting a new snapshot, we can just re-use it.
	 */
	TransactionSnapshot *sourceSnapshot = &(copySpecs->sourceSnapshot);

	if (IS_EMPTY_STRING_BUFFER(sourceSnapshot->snapshot))
	{
		if (!copydb_export_snapshot(sourceSnapshot))
		{
			log_fatal("Failed to export a snapshot on \"%s\"",
					  sourceSnapshot->pguri);
			return false;
		}
	}
	else
	{
		if (!copydb_set_snapshot(copySpecs))
		{
			log_fatal("Failed to use given --snapshot \"%s\"",
					  sourceSnapshot->snapshot);
			return false;
		}

		log_info("[SNAPSHOT] Using snapshot \"%s\" on the source database",
				 sourceSnapshot->snapshot);
	}

	/* store the snapshot in a file, to support --resume --snapshot ... */
	if (!file_exists(copySpecs->cfPaths.snfile))
	{
		if (!write_file(sourceSnapshot->snapshot,
						strlen(sourceSnapshot->snapshot),
						copySpecs->cfPaths.snfile))
		{
			log_fatal("Failed to create the snapshot file \"%s\"",
					  copySpecs->cfPaths.snfile);
			return false;
		}

		log_notice("Wrote snapshot \"%s\" to file \"%s\"",
				   sourceSnapshot->snapshot,
				   copySpecs->cfPaths.snfile);
	}

	return true;
}


/*
 * copydb_should_export_snapshot returns true when a snapshot should be
 * exported to be able to implement the command.
 */
bool
copydb_should_export_snapshot(CopyDataSpec *copySpecs)
{
	/* when --not-consistent is used, we have nothing to do here */
	if (!copySpecs->consistent)
	{
		copySpecs->sourceSnapshot.state = SNAPSHOT_STATE_SKIPPED;
		log_debug("copydb_prepare_snapshot: --not-consistent, skipping");
		return false;
	}

	/*
	 * When the --snapshot option has been used, instead of exporting a new
	 * snapshot, we can just re-use it.
	 */
	TransactionSnapshot *sourceSnapshot = &(copySpecs->sourceSnapshot);

	return IS_EMPTY_STRING_BUFFER(sourceSnapshot->snapshot);
}


/*
 * copydb_create_logical_replication_slot uses Postgres logical replication
 * protocol command CREATE_REPLICATION_SLOT to create a replication slot on the
 * source database, and exports a snapshot while doing so.
 */
bool
copydb_create_logical_replication_slot(CopyDataSpec *copySpecs,
									   const char *logrep_pguri,
									   ReplicationSlot *slot)
{
	TransactionSnapshot *sourceSnapshot = &(copySpecs->sourceSnapshot);

	/*
	 * Now is the time to check if a previous command such as
	 *
	 *   pgcopydb snapshot --follow --plugin ... --slot-name ...
	 *
	 * did create the replication slot for us while exporting the snapshot. we
	 * can then re-use the replication slot and the exported snapshot here.
	 *
	 * On the other hand, if a snapshot was exported without the --follow
	 * option then we can't re-use that snapshot.
	 */
	if (slot->lsn != InvalidXLogRecPtr &&
		!IS_EMPTY_STRING_BUFFER(slot->snapshot))
	{
		log_info("Re-using replication slot \"%s\" "
				 "created at %X/%X with snapshot \"%s\"",
				 slot->slotName,
				 LSN_FORMAT_ARGS(slot->lsn),
				 slot->snapshot);
		return true;
	}
	else if (!IS_EMPTY_STRING_BUFFER(sourceSnapshot->snapshot))
	{
		log_fatal("Failed to use --snapshot \"%s\" which was not created by "
				  "the replication protocol command CREATE_REPLICATION_SLOT",
				  sourceSnapshot->snapshot);
		log_info("Consider using pgcopydb snapshot --follow");
		return false;
	}

	/* check XID wraparound proximity before creating replication slot */
	if (!copydb_check_xid_wraparound(copySpecs))
	{
		/* errors have already been logged */
		return false;
	}

	sourceSnapshot->kind = SNAPSHOT_KIND_LOGICAL;

	LogicalStreamClient *stream = &(sourceSnapshot->stream);

	if (!pgsql_init_stream(stream,
						   logrep_pguri,
						   slot->plugin,
						   slot->slotName,
						   InvalidXLogRecPtr,
						   InvalidXLogRecPtr))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_create_logical_replication_slot(stream, slot))
	{
		log_error("Failed to create a logical replication slot "
				  "and export a snapshot, see above for details");
		return false;
	}

	/* expose the replication slot snapshot as the main transaction snapshot */
	strlcpy(sourceSnapshot->snapshot,
			slot->snapshot,
			sizeof(sourceSnapshot->snapshot));

	sourceSnapshot->state = SNAPSHOT_STATE_EXPORTED;
	sourceSnapshot->exportedCreateSlotSnapshot = true;

	/*
	 * Detect if the source database is a read-only standby. This must be
	 * done here in the main process before forking workers, so that forked
	 * child processes inherit the isReadOnly flag via the copied
	 * sourceSnapshot structure.
	 *
	 * The replication protocol connection used above does not support
	 * pg_is_in_recovery(), so we open a temporary standard connection.
	 */
	{
		PGSQL tmpSrc = { 0 };

		if (!pgsql_init(&tmpSrc, sourceSnapshot->pguri,
						sourceSnapshot->connectionType))
		{
			log_error("Failed to init connection for recovery check");
			return false;
		}

		if (!pgsql_is_in_recovery(&tmpSrc, &(sourceSnapshot->isReadOnly)))
		{
			log_error("Failed to check if source is in recovery");
			pgsql_finish(&tmpSrc);
			return false;
		}

		pgsql_finish(&tmpSrc);
	}

	/* store the snapshot in a file, to support --resume --snapshot ... */
	if (!write_file(sourceSnapshot->snapshot,
					strlen(sourceSnapshot->snapshot),
					copySpecs->cfPaths.snfile))
	{
		log_fatal("Failed to create the snapshot file \"%s\"",
				  copySpecs->cfPaths.snfile);
		return false;
	}

	/* store the replication slot information in a file, same reasons */
	if (!snapshot_write_slot(copySpecs->cfPaths.cdc.slotfile, slot))
	{
		log_fatal("Failed to create the slot file \"%s\"",
				  copySpecs->cfPaths.cdc.slotfile);
		return false;
	}

	return true;
}


/*
 * snapshot_write_slot writes a replication slot information to file.
 */
bool
snapshot_write_slot(const char *filename, ReplicationSlot *slot)
{
	PQExpBuffer contents = createPQExpBuffer();

	appendPQExpBuffer(contents, "%s\n", slot->slotName);
	appendPQExpBuffer(contents, "%X/%X\n", LSN_FORMAT_ARGS(slot->lsn));
	appendPQExpBuffer(contents, "%s\n", slot->snapshot);
	appendPQExpBuffer(contents, "%s\n", OutputPluginToString(slot->plugin));
	appendPQExpBuffer(contents, "%s\n", boolToString(slot->wal2jsonNumericAsString));

	if (PQExpBufferBroken(contents))
	{
		log_error("Failed to allocate memory");
		destroyPQExpBuffer(contents);
		return false;
	}

	if (!write_file(contents->data, contents->len, filename))
	{
		log_fatal("Failed to create slot file \"%s\"", filename);

		destroyPQExpBuffer(contents);
		return false;
	}

	destroyPQExpBuffer(contents);
	return true;
}


/*
 * snapshot_read_slot reads a replication slot information from file.
 */
bool
snapshot_read_slot(const char *filename, ReplicationSlot *slot)
{
	char *contents = NULL;
	long fileSize = 0L;

	log_trace("snapshot_read_slot: %s", filename);

	if (!read_file(filename, &contents, &fileSize))
	{
		/* errors have already been logged */
		return false;
	}

	/* make sure to use only the first line of the file, without \n */
	LinesBuffer lbuf = { 0 };

	if (!splitLines(&lbuf, contents))
	{
		/* errors have already been logged */
		return false;
	}

	if (lbuf.count != 5)
	{
		log_error("Failed to parse replication slot file \"%s\"", filename);
		return false;
	}

	/* 1. slotName */
	int length = strlcpy(slot->slotName, lbuf.lines[0], sizeof(slot->slotName));

	if (length >= sizeof(slot->slotName))
	{
		log_error("Failed to read replication slot name \"%s\" from file \"%s\", "
				  "length is %lld bytes which exceeds maximum %lld bytes",
				  lbuf.lines[0],
				  filename,
				  (long long) strlen(lbuf.lines[0]),
				  (long long) sizeof(slot->slotName));
		return false;
	}

	/* 2. LSN (consistent_point) */
	if (!parseLSN(lbuf.lines[1], &(slot->lsn)))
	{
		log_error("Failed to parse LSN \"%s\" from file \"%s\"",
				  lbuf.lines[1],
				  filename);
		return false;
	}

	/* 3. snapshot */
	length = strlcpy(slot->snapshot, lbuf.lines[2], sizeof(slot->snapshot));

	if (length >= sizeof(slot->snapshot))
	{
		log_error("Failed to read replication snapshot \"%s\" from file \"%s\", "
				  "length is %lld bytes which exceeds maximum %lld bytes",
				  lbuf.lines[2],
				  filename,
				  (long long) strlen(lbuf.lines[2]),
				  (long long) sizeof(slot->snapshot));
		return false;
	}

	/* 4. plugin */
	slot->plugin = OutputPluginFromString(lbuf.lines[3]);

	if (slot->plugin == STREAM_PLUGIN_UNKNOWN)
	{
		log_error("Failed to read plugin \"%s\" from file \"%s\"",
				  lbuf.lines[3],
				  filename);
		return false;
	}

	/* 5. wal2json-numeric-as-string */
	parse_bool(lbuf.lines[4], &(slot->wal2jsonNumericAsString));

	if (slot->wal2jsonNumericAsString &&
		slot->plugin != STREAM_PLUGIN_WAL2JSON)
	{
		log_error("Failed to read wal2json-numeric-as-string \"%s\" from file \"%s\" "
				  "because the plugin is not wal2json",
				  lbuf.lines[4],
				  filename);
	}


	log_notice("Read replication slot file \"%s\" with snapshot \"%s\", "
			   "slot \"%s\", lsn %X/%X, and plugin \"%s\"",
			   filename,
			   slot->snapshot,
			   slot->slotName,
			   LSN_FORMAT_ARGS(slot->lsn),
			   OutputPluginToString(slot->plugin));

	return true;
}
