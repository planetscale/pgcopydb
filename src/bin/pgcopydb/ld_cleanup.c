/*
 * src/bin/pgcopydb/ld_cleanup.c
 *     CDC file cleanup watchdog for pgcopydb.
 *
 *     Periodically scans the CDC directory and removes .json and .sql files
 *     that have already been applied (fileLSN < replayLSN) once total applied
 *     file bytes exceed the configured threshold.
 */

#include <dirent.h>
#include <errno.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "postgres.h"
#include "postgres_fe.h"
#include "access/xlog_internal.h"
#include "access/xlogdefs.h"

#include "copydb.h"
#include "file_utils.h"
#include "ld_cleanup.h"
#include "ld_stream.h"
#include "log.h"
#include "signals.h"
#include "string_utils.h"


#define CDC_CLEANUP_CYCLE_SECONDS 30
#define CDC_CLEANUP_MAX_FILES 4096


typedef struct CDCFileEntry
{
	char path[MAXPGPATH];
	uint64_t lsn;
	off_t size;
	time_t mtime;
} CDCFileEntry;


/*
 * cdc_file_is_eligible returns true when a CDC file is eligible for cleanup:
 * its LSN is behind the replay position and it is at least minAgeSeconds old.
 */
bool
cdc_file_is_eligible(uint64_t fileLSN,
					 uint64_t replayLSN,
					 time_t fileMtime,
					 time_t now,
					 int minAgeSeconds)
{
	return fileLSN < replayLSN &&
		   difftime(now, fileMtime) >= minAgeSeconds;
}


/*
 * compare_cdc_file_entry_by_mtime is a qsort comparator that sorts CDCFileEntry
 * entries oldest-first by mtime.
 */
static int
compare_cdc_file_entry_by_mtime(const void *a, const void *b)
{
	const CDCFileEntry *ea = (const CDCFileEntry *) a;
	const CDCFileEntry *eb = (const CDCFileEntry *) b;

	if (ea->mtime < eb->mtime)
	{
		return -1;
	}

	if (ea->mtime > eb->mtime)
	{
		return 1;
	}

	return 0;
}


/*
 * cdc_cleanup_loop is the main watchdog loop that runs in a forked subprocess.
 * It periodically scans the CDC directory and removes old applied files when
 * the total size of applied files exceeds the configured threshold.
 */
bool
cdc_cleanup_loop(struct StreamSpecs *specs)
{
	uint64_t thresholdBytes = specs->cleanupThresholdBytes;
	int minAgeSeconds = specs->cleanupMinAgeSeconds;
	uint32_t WalSegSz = specs->WalSegSz;
	char *cdcDir = specs->paths.dir;

	log_info("CDC cleanup watchdog started: threshold %llu bytes, "
			 "min age %d seconds, dir %s",
			 (unsigned long long) thresholdBytes,
			 minAgeSeconds,
			 cdcDir);

	while (true)
	{
		/*
		 * Sleep in 1-second increments for CDC_CLEANUP_CYCLE_SECONDS,
		 * checking signal flags each second.
		 */
		for (int i = 0; i < CDC_CLEANUP_CYCLE_SECONDS; i++)
		{
			if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
			{
				log_info("CDC cleanup watchdog received shutdown signal");
				return true;
			}

			pg_usleep(1000000L);    /* 1 second */
		}

		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			log_info("CDC cleanup watchdog received shutdown signal");
			return true;
		}

		/*
		 * If WalSegSz hasn't been populated yet (the receive process
		 * writes context files on first connect), try to read it now.
		 */
		if (WalSegSz == 0)
		{
			if (!file_exists(specs->paths.walsegsizefile))
			{
				log_debug("CDC cleanup: context files not ready yet, "
						  "will retry next cycle");
				continue;
			}

			if (!stream_read_context(specs))
			{
				log_warn("CDC cleanup: failed to read context, "
						 "will retry next cycle");
				continue;
			}

			WalSegSz = specs->WalSegSz;

			if (WalSegSz == 0)
			{
				log_debug("CDC cleanup: WalSegSz still unknown, "
						  "will retry next cycle");
				continue;
			}
		}

		/* Read the current replay_lsn from the sentinel */
		CopyDBSentinel sentinel = { 0 };

		if (!sentinel_get(specs->sourceDB, &sentinel))
		{
			log_warn("CDC cleanup: failed to read sentinel, "
					 "will retry next cycle");
			continue;
		}

		uint64_t replayLSN = sentinel.replay_lsn;

		if (replayLSN == 0)
		{
			log_debug("CDC cleanup: replay_lsn is 0, nothing to clean");
			continue;
		}

		/* Scan the CDC directory */
		DIR *dir = opendir(cdcDir);

		if (dir == NULL)
		{
			log_warn("CDC cleanup: failed to open directory %s: %m", cdcDir);
			continue;
		}

		CDCFileEntry *entries = (CDCFileEntry *) calloc(CDC_CLEANUP_MAX_FILES,
														sizeof(CDCFileEntry));

		if (entries == NULL)
		{
			log_error("CDC cleanup: failed to allocate file entry array");
			closedir(dir);
			continue;
		}

		int entryCount = 0;
		uint64_t totalAppliedBytes = 0;
		struct dirent *de;

		while ((de = readdir(dir)) != NULL)
		{
			char *name = de->d_name;
			size_t nameLen = strlen(name);

			/* only consider .json and .sql files */
			bool isJson = (nameLen > 5 &&
						   strcmp(name + nameLen - 5, ".json") == 0);

			bool isSql = (nameLen > 4 &&
						  strcmp(name + nameLen - 4, ".sql") == 0);

			if (!isJson && !isSql)
			{
				continue;
			}

			/* strip the suffix to get the bare WAL name */
			char barename[MAXPGPATH];
			strlcpy(barename, name, MAXPGPATH);
			char *dot = strchr(barename, '.');
			if (dot != NULL)
				*dot = '\0';

			if (!IsXLogFileName(barename))
			{
				log_debug("CDC cleanup: skipping non-WAL file %s", name);
				continue;
			}

			TimeLineID tli;
			XLogSegNo segno;
			XLogFromFileName(barename, &tli, &segno, WalSegSz);

			uint64_t fileLSN = 0;
			XLogSegNoOffsetToRecPtr(segno, 0, WalSegSz, fileLSN);

			/* only consider files whose LSN is behind the replay position */
			if (fileLSN >= replayLSN)
			{
				continue;
			}

			/* stat for size and mtime */
			char fullpath[MAXPGPATH] = { 0 };

			sformat(fullpath, sizeof(fullpath), "%s/%s", cdcDir, name);

			struct stat st;

			if (stat(fullpath, &st) != 0)
			{
				log_debug("CDC cleanup: stat failed for %s: %m", fullpath);
				continue;
			}

			totalAppliedBytes += st.st_size;

			if (entryCount < CDC_CLEANUP_MAX_FILES)
			{
				CDCFileEntry *entry = &entries[entryCount++];

				strlcpy(entry->path, fullpath, MAXPGPATH);
				entry->lsn = fileLSN;
				entry->size = st.st_size;
				entry->mtime = st.st_mtime;
			}
		}

		if (entryCount >= CDC_CLEANUP_MAX_FILES)
		{
			log_warn("CDC cleanup: more than %d applied files found; "
					 "excess files are not tracked for deletion",
					 CDC_CLEANUP_MAX_FILES);
		}

		closedir(dir);

		log_debug("CDC cleanup: found %d applied files, "
				  "total %llu bytes (threshold %llu)",
				  entryCount,
				  (unsigned long long) totalAppliedBytes,
				  (unsigned long long) thresholdBytes);

		/* if under threshold, nothing to do */
		if (totalAppliedBytes <= thresholdBytes)
		{
			free(entries);
			continue;
		}

		/* sort oldest-first so we delete the oldest files first */
		qsort(entries, entryCount, sizeof(CDCFileEntry),
			  compare_cdc_file_entry_by_mtime);

		time_t now = time(NULL);
		uint64_t bytesToFree = totalAppliedBytes - thresholdBytes;
		uint64_t freedBytes = 0;
		int deletedCount = 0;

		/*
		 * First pass: delete files that are old enough (>= minAgeSeconds).
		 */
		for (int i = 0; i < entryCount && freedBytes < bytesToFree; i++)
		{
			CDCFileEntry *entry = &entries[i];

			if (!cdc_file_is_eligible(entry->lsn, replayLSN,
									  entry->mtime, now, minAgeSeconds))
			{
				log_debug("CDC cleanup: skipping %s (too young, age %.0fs)",
						  entry->path,
						  difftime(now, entry->mtime));
				continue;
			}

			if (unlink(entry->path) != 0)
			{
				log_warn("CDC cleanup: failed to delete %s: %m", entry->path);
				continue;
			}

			freedBytes += entry->size;
			deletedCount++;

			log_debug("CDC cleanup: deleted %s (%lld bytes, age %.0fs)",
					 entry->path,
					 (long long) entry->size,
					 difftime(now, entry->mtime));

			/* mark as deleted so second pass skips it */
			entry->path[0] = '\0';
		}

		/*
		 * Second pass: if old-enough files alone couldn't bring us under
		 * threshold, override the age floor (disk pressure).
		 */
		if (freedBytes < bytesToFree)
		{
			for (int i = 0; i < entryCount && freedBytes < bytesToFree; i++)
			{
				CDCFileEntry *entry = &entries[i];

				/* skip already-deleted entries */
				if (entry->path[0] == '\0')
				{
					continue;
				}

				log_notice("CDC cleanup: disk pressure override, "
						   "deleting young file %s (age %.0fs)",
						   entry->path,
						   difftime(now, entry->mtime));

				if (unlink(entry->path) != 0)
				{
					log_warn("CDC cleanup: failed to delete %s: %m",
							 entry->path);
					continue;
				}

				freedBytes += entry->size;
				deletedCount++;
			}
		}

		if (deletedCount > 0)
		{
			log_info("CDC cleanup: deleted %d files, freed %llu bytes",
					 deletedCount,
					 (unsigned long long) freedBytes);
		}

		free(entries);
	}

	return true;
}
