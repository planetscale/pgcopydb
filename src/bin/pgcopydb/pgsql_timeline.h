/*
 * src/bin/pgcopydb/pgsql_timeline.h
 *	 API for sending SQL commands about timelines to a PostgreSQL server
 */
#ifndef PGSQL_TIMELINE_H
#define PGSQL_TIMELINE_H

#include "pgsql.h"
#include "schema.h"

/* pgsql_timeline.c */
bool pgsql_identify_system(PGSQL *pgsql, IdentifySystem *system);

#endif /* PGSQL_TIMELINE_H */
