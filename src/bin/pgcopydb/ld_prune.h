/*
 * src/bin/pgcopydb/ld_prune.h
 *     CDC file prune watchdog for pgcopydb
 */

#ifndef LD_PRUNE_H
#define LD_PRUNE_H

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

bool cdc_prune_loop(struct StreamSpecs *specs);

#endif /* LD_PRUNE_H */
