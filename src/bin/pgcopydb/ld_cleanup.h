/*
 * src/bin/pgcopydb/ld_cleanup.h
 *     CDC file cleanup watchdog for pgcopydb
 */

#ifndef LD_CLEANUP_H
#define LD_CLEANUP_H

#include <stdbool.h>
#include <stdint.h>
#include <time.h>

/* Forward declaration -- full definition in ld_stream.h */
struct StreamSpecs;

bool cdc_file_is_eligible(uint64_t fileLSN,
						  uint64_t replayLSN,
						  time_t fileMtime,
						  time_t now,
						  int minAgeSeconds);

bool cdc_cleanup_loop(struct StreamSpecs *specs);

#endif /* LD_CLEANUP_H */
