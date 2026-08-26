/*
 * src/bin/pgcopydb/ld_pgoutput.h
 *   pgoutput logical decoding plugin support for pgcopydb.
 *
 * See ld_pgoutput.c for the wire format. ld_stream.h includes this header to
 * embed the relation cache in StreamContext, so it must NOT include
 * ld_stream.h back.
 */

#ifndef LD_PGOUTPUT_H
#define LD_PGOUTPUT_H

#include <stdbool.h>
#include <stdint.h>

#include "pgsql.h"
#include "uthash.h"


typedef struct PgoutputAttrCache
{
	int colIndex;                   /* hash key: zero-based position */
	char attname[PG_NAMEDATALEN];
	uint32_t typeOID;
	bool isReplicaIdentity;         /* flags bit 0x01 */
	UT_hash_handle hh;
} PgoutputAttrCache;


/* built from the 'R' (RELATION) messages, keyed by relOid */
typedef struct PgoutputRelationCache
{
	uint32_t relOid;                /* hash key */
	char nspname[PG_NAMEDATALEN];
	char relname[PG_NAMEDATALEN];
	char replicaIdentity;           /* 'd', 'i', 'f', 'n' */
	int natts;
	PgoutputAttrCache *attrs;
	UT_hash_handle hh;
} PgoutputRelationCache;


typedef struct PgoutputColumn
{
	char name[PG_NAMEDATALEN];
	char status;                    /* 't' value, 'n' null, 'u' unchanged TOAST */
	uint32_t typeOID;
	char *value;                    /* non-NULL only when status='t' */
} PgoutputColumn;


/* one TRUNCATE message may name several relations */
typedef struct PgoutputTruncateRel
{
	char nspname[PG_NAMEDATALEN];
	char relname[PG_NAMEDATALEN];
} PgoutputTruncateRel;


typedef struct PgoutputMessage
{
	/* char codes, not StreamAction, to avoid including ld_stream.h */
	char action;                    /* 'B','C','I','U','D','T' */
	uint32_t xid;
	uint64_t lsn;

	char nspname[PG_NAMEDATALEN];
	char relname[PG_NAMEDATALEN];

	char oldType;                   /* 'K' key-only, 'O' full-old, 0 absent */
	int ncols_old;
	PgoutputColumn *old_cols;       /* NULL when oldType==0 */

	int ncols_new;
	PgoutputColumn *new_cols;       /* NULL for DELETE */

	int ntruncate;
	PgoutputTruncateRel *truncate;  /* NULL unless action=='T' */
} PgoutputMessage;


struct StreamContext;

bool parsePgoutputMessageActionAndXid(LogicalStreamContext *context);
bool preparePgoutputMessage(LogicalStreamContext *context);
char * pgoutputTruncateJSON(struct StreamContext *privateContext, int relIndex);
void free_pgoutput_message(PgoutputMessage *msg);


#endif  /* LD_PGOUTPUT_H */
