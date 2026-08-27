/*
 * src/bin/pgcopydb/ld_pgoutput.c
 *   pgoutput logical decoding plugin support for pgcopydb.
 *
 * pgoutput uses a binary wire protocol (proto_version=1) with typed messages.
 * All integers are big-endian (network byte order).
 *
 * Message layout for proto_version=1 (no streaming, no xid in DML):
 *
 *  BEGIN:    'B' u64(final_lsn) u64(commit_time) u32(xid)
 *  COMMIT:   'C' u8(flags) u64(commit_lsn) u64(end_lsn) u64(commit_time)
 *  RELATION: 'R' u32(relOid) cstr(nspname) cstr(relname) u8(replident)
 *                u16(natts) per-col[u8(flags) cstr(name) u32(typeOid) i32(typmod)]
 *  INSERT:   'I' u32(relOid) 'N' tuple
 *  UPDATE:   'U' u32(relOid) [('K'|'O') old_tuple] 'N' new_tuple
 *  DELETE:   'D' u32(relOid) ('K'|'O') old_tuple
 *  TRUNCATE: 'T' u32(nrelids) u8(flags) nrelids*u32(relOid)
 *  TYPE:     'Y' ...  (filtered out)
 *  ORIGIN:   'O' ...  (filtered out)
 *
 * Tuple: u16(ncols) per-col[u8(status) if-'t': i32(len) + len bytes]
 *   status: 'n'=null, 'u'=unchanged TOAST, 't'=text value
 *
 * The decoded message is serialised as a wal2json-shaped JSON object so that
 * the transform and apply steps reuse the existing wal2json code path.
 */

#include <errno.h>
#include <inttypes.h>
#include <string.h>

#include "postgres.h"
#include "postgres_fe.h"
#include "access/xlogdefs.h"
#include "pqexpbuffer.h"

#include "defaults.h"
#include "ld_pgoutput.h"
#include "ld_stream.h"
#include "log.h"
#include "pgsql.h"
#include "string_utils.h"

/*
 * Replica identity flag from proto.c (not exported in a header we can use).
 */
#define PGOUT_IS_REPLICA_IDENTITY 0x01

/*
 * The only two type OIDs the transform step keys on: json takes a ::text cast
 * in the WHERE clause, bytea needs wal2json's \x handling. Every other type is
 * reported with no "type" property, which binds the value as text.
 */
#define PGOUT_BYTEAOID 17
#define PGOUT_JSONOID 114


/*
 * Big-endian readers. All bounds-check against bufLen and return 0/NULL on
 * overflow.
 */
static uint8_t
pgout_u8(const char *buf, int *pos, int bufLen)
{
	if (*pos + 1 > bufLen)
	{
		log_error("pgoutput: buffer underflow reading u8 at pos %d (len %d)",
				  *pos, bufLen);
		return 0;
	}

	uint8_t v = (uint8_t) buf[*pos];
	*pos += 1;

	return v;
}


static int16_t
pgout_i16(const char *buf, int *pos, int bufLen)
{
	if (*pos + 2 > bufLen)
	{
		log_error("pgoutput: buffer underflow reading i16 at pos %d (len %d)",
				  *pos, bufLen);
		return 0;
	}

	uint16_t v =
		((uint16_t) (uint8_t) buf[*pos] << 8) |
		((uint16_t) (uint8_t) buf[*pos + 1]);

	*pos += 2;

	return (int16_t) v;
}


static uint32_t
pgout_u32(const char *buf, int *pos, int bufLen)
{
	if (*pos + 4 > bufLen)
	{
		log_error("pgoutput: buffer underflow reading u32 at pos %d (len %d)",
				  *pos, bufLen);
		return 0;
	}

	uint32_t v =
		((uint32_t) (uint8_t) buf[*pos] << 24) |
		((uint32_t) (uint8_t) buf[*pos + 1] << 16) |
		((uint32_t) (uint8_t) buf[*pos + 2] << 8) |
		((uint32_t) (uint8_t) buf[*pos + 3]);

	*pos += 4;

	return v;
}


static int32_t
pgout_i32(const char *buf, int *pos, int bufLen)
{
	return (int32_t) pgout_u32(buf, pos, bufLen);
}


static uint64_t
pgout_u64(const char *buf, int *pos, int bufLen)
{
	if (*pos + 8 > bufLen)
	{
		log_error("pgoutput: buffer underflow reading u64 at pos %d (len %d)",
				  *pos, bufLen);
		return 0;
	}

	uint64_t v = 0;

	for (int i = 0; i < 8; i++)
	{
		v = (v << 8) | (uint64_t) (uint8_t) buf[*pos + i];
	}

	*pos += 8;

	return v;
}


/*
 * pgout_cstr returns a pointer into buf at the current position and advances
 * *pos past the terminating NUL byte. The returned pointer is not a copy.
 */
static const char *
pgout_cstr(const char *buf, int *pos, int bufLen)
{
	int start = *pos;

	while (*pos < bufLen && buf[*pos] != '\0')
	{
		++(*pos);
	}

	if (*pos >= bufLen)
	{
		log_error("pgoutput: unterminated string at pos %d (len %d)",
				  start, bufLen);
		return NULL;
	}

	/* skip the NUL byte */
	++(*pos);

	return buf + start;
}


/* ----------
 * Relation cache management.
 * ----------
 */

/*
 * pgoutput_cache_relation parses a RELATION ('R') message starting at pos and
 * inserts the relation in privateContext->pgoutputRelationCache.
 */
static bool
pgoutput_cache_relation(StreamContext *privateContext,
						const char *buf, int bufLen, int pos)
{
	/* pos is already past the 'R' type byte */
	uint32_t relOid = pgout_u32(buf, &pos, bufLen);

	const char *nspname = pgout_cstr(buf, &pos, bufLen);

	if (nspname == NULL)
	{
		return false;
	}

	/* empty namespace means pg_catalog */
	if (nspname[0] == '\0')
	{
		nspname = "pg_catalog";
	}

	const char *relname = pgout_cstr(buf, &pos, bufLen);

	if (relname == NULL)
	{
		return false;
	}

	uint8_t replident = pgout_u8(buf, &pos, bufLen);

	int16_t natts = pgout_i16(buf, &pos, bufLen);

	/* a relation is re-sent when its definition changes: drop the stale entry */
	PgoutputRelationCache *rel = NULL;
	HASH_FIND_INT(privateContext->pgoutputRelationCache, &relOid, rel);

	if (rel != NULL)
	{
		HASH_DEL(privateContext->pgoutputRelationCache, rel);

		PgoutputAttrCache *attr, *tmp;
		HASH_ITER(hh, rel->attrs, attr, tmp)
		{
			HASH_DEL(rel->attrs, attr);
			free(attr);
		}

		free(rel);
	}

	rel = (PgoutputRelationCache *) calloc(1, sizeof(PgoutputRelationCache));

	if (rel == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	rel->relOid = relOid;
	strlcpy(rel->nspname, nspname, sizeof(rel->nspname));
	strlcpy(rel->relname, relname, sizeof(rel->relname));
	rel->replicaIdentity = (char) replident;
	rel->natts = natts;

	for (int i = 0; i < natts; i++)
	{
		uint8_t flags = pgout_u8(buf, &pos, bufLen);
		const char *attname = pgout_cstr(buf, &pos, bufLen);

		if (attname == NULL)
		{
			free(rel);
			return false;
		}

		uint32_t typeOID = pgout_u32(buf, &pos, bufLen);
		pgout_i32(buf, &pos, bufLen);  /* typmod - not used */

		PgoutputAttrCache *attr =
			(PgoutputAttrCache *) calloc(1, sizeof(PgoutputAttrCache));

		if (attr == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			free(rel);
			return false;
		}

		attr->colIndex = i;
		strlcpy(attr->attname, attname, sizeof(attr->attname));
		attr->typeOID = typeOID;
		attr->isReplicaIdentity = (flags & PGOUT_IS_REPLICA_IDENTITY) != 0;

		HASH_ADD_INT(rel->attrs, colIndex, attr);
	}

	HASH_ADD_INT(privateContext->pgoutputRelationCache, relOid, rel);

	log_debug("pgoutput: cached relation %u %s.%s replident=%c natts=%d",
			  relOid, rel->nspname, rel->relname, rel->replicaIdentity, natts);

	return true;
}


/* ----------
 * Tuple decoder.
 * ----------
 */

/*
 * decode_tuple reads a binary tuple from buf at *pos, allocating *cols_out and
 * setting *ncols_out to the tuple width reported on the wire.
 */
static bool
decode_tuple(const char *buf, int bufLen, int *pos,
			 PgoutputRelationCache *rel,
			 PgoutputColumn **cols_out, int *ncols_out)
{
	int16_t ncols = pgout_i16(buf, pos, bufLen);

	if (ncols < 0)
	{
		log_error("pgoutput: negative column count %d in tuple", ncols);
		return false;
	}

	*ncols_out = ncols;

	if (ncols == 0)
	{
		*cols_out = NULL;
		return true;
	}

	PgoutputColumn *cols =
		(PgoutputColumn *) calloc(ncols, sizeof(PgoutputColumn));

	if (cols == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return false;
	}

	for (int i = 0; i < ncols; i++)
	{
		uint8_t status = pgout_u8(buf, pos, bufLen);
		cols[i].status = (char) status;

		/* get the column name and type from the relation cache */
		if (rel != NULL)
		{
			PgoutputAttrCache *attr = NULL;
			HASH_FIND_INT(rel->attrs, &i, attr);

			if (attr != NULL)
			{
				strlcpy(cols[i].name, attr->attname, sizeof(cols[i].name));
				cols[i].typeOID = attr->typeOID;
			}
		}

		if (status == 't')
		{
			int32_t len = pgout_i32(buf, pos, bufLen);

			if (len < 0)
			{
				log_error("pgoutput: negative column value length %d", len);
				free(cols);
				return false;
			}

			if (*pos + len > bufLen)
			{
				log_error("pgoutput: buffer underflow reading column value "
						  "(need %d bytes at pos %d, bufLen %d)",
						  len, *pos, bufLen);
				free(cols);
				return false;
			}

			cols[i].value = strndup(buf + *pos, len);

			if (cols[i].value == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				free(cols);
				return false;
			}

			*pos += len;
		}
		else if (status == 'b')
		{
			/* binary column: read and discard, we never ask for binary */
			int32_t len = pgout_i32(buf, pos, bufLen);

			if (len >= 0 && *pos + len <= bufLen)
			{
				*pos += len;
			}

			cols[i].status = 'u';   /* treat as unchanged for our purposes */
		}

		/* 'n' and 'u' have no payload */
	}

	*cols_out = cols;

	return true;
}


/* ----------
 * JSON serialisation, matching the wal2json format-version 2 shape.
 * ----------
 */

/*
 * appendJSONString appends str to buf as a quoted JSON string literal.
 */
static void
appendJSONString(PQExpBuffer buf, const char *str)
{
	appendPQExpBufferChar(buf, '"');

	for (const unsigned char *p = (const unsigned char *) str; *p; p++)
	{
		switch (*p)
		{
			case '"':
			{
				appendPQExpBufferStr(buf, "\\\"");
				break;
			}

			case '\\':
			{
				appendPQExpBufferStr(buf, "\\\\");
				break;
			}

			case '\b':
			{
				appendPQExpBufferStr(buf, "\\b");
				break;
			}

			case '\f':
			{
				appendPQExpBufferStr(buf, "\\f");
				break;
			}

			case '\n':
			{
				appendPQExpBufferStr(buf, "\\n");
				break;
			}

			case '\r':
			{
				appendPQExpBufferStr(buf, "\\r");
				break;
			}

			case '\t':
			{
				appendPQExpBufferStr(buf, "\\t");
				break;
			}

			default:
			{
				if (*p < 0x20)
				{
					appendPQExpBuffer(buf, "\\u%04x", *p);
				}
				else
				{
					appendPQExpBufferChar(buf, (char) *p);
				}

				break;
			}
		}
	}

	appendPQExpBufferChar(buf, '"');
}


/*
 * appendColumn appends one {"name":..,"type":..,"value":..} object. Values are
 * emitted as JSON strings, never numbers: pgoutput already sends the type's
 * text output, and a JSON number would cost precision on numeric and float8.
 */
static void
appendColumn(PQExpBuffer buf, PgoutputColumn *col, bool first)
{
	appendPQExpBufferStr(buf, first ? "{" : ",{");

	appendPQExpBufferStr(buf, "\"name\":");
	appendJSONString(buf, col->name);

	/* bytea reports the value without \x, matching wal2json, which puts it back */
	if (col->typeOID == PGOUT_JSONOID)
	{
		appendPQExpBufferStr(buf, ",\"type\":\"json\"");
	}
	else if (col->typeOID == PGOUT_BYTEAOID)
	{
		appendPQExpBufferStr(buf, ",\"type\":\"bytea\"");
	}

	appendPQExpBufferStr(buf, ",\"value\":");

	if (col->status == 'n' || col->value == NULL)
	{
		appendPQExpBufferStr(buf, "null");
	}
	else if (col->typeOID == PGOUT_BYTEAOID)
	{
		const char *value = col->value;

		/* strip the \x prefix, the transform step puts it back */
		if (value[0] == '\\' && value[1] == 'x')
		{
			value += 2;
		}

		appendJSONString(buf, value);
	}
	else
	{
		appendJSONString(buf, col->value);
	}

	appendPQExpBufferChar(buf, '}');
}


/*
 * appendTuple appends a JSON array of columns under the given property name.
 *
 * Status 'u' is always skipped, the value was never sent. In a 'K' section
 * (REPLICA IDENTITY DEFAULT) status 'n' marks a non-key placeholder and must
 * not reach the WHERE clause; in an 'O' section or a new tuple the same status
 * is a genuine NULL and is kept.
 */
static void
appendTuple(PQExpBuffer buf, const char *property,
			PgoutputColumn *cols, int ncols, bool keySection)
{
	appendPQExpBuffer(buf, ",\"%s\":[", property);

	bool first = true;

	for (int i = 0; i < ncols; i++)
	{
		if (cols[i].status == 'u')
		{
			continue;
		}

		if (keySection && cols[i].status == 'n')
		{
			continue;
		}

		appendColumn(buf, &(cols[i]), first);
		first = false;
	}

	appendPQExpBufferChar(buf, ']');
}


/*
 * appendRelation appends the "schema" and "table" properties.
 */
static void
appendRelation(PQExpBuffer buf, const char *nspname, const char *relname)
{
	appendPQExpBufferStr(buf, ",\"schema\":");
	appendJSONString(buf, nspname);

	appendPQExpBufferStr(buf, ",\"table\":");
	appendJSONString(buf, relname);
}


/*
 * finishJSON returns a copy of the buffer contents and destroys the buffer.
 */
static char *
finishJSON(PQExpBuffer buf)
{
	if (PQExpBufferBroken(buf))
	{
		log_error("pgoutput: failed to prepare JSON message: out of memory");
		destroyPQExpBuffer(buf);
		return NULL;
	}

	char *json = strdup(buf->data);

	destroyPQExpBuffer(buf);

	if (json == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
	}

	return json;
}


/*
 * pgoutputMessageToJSON serialises the decoded message as a wal2json-shaped
 * JSON object.
 */
static char *
pgoutputMessageToJSON(PgoutputMessage *msg)
{
	PQExpBuffer buf = createPQExpBuffer();

	if (buf == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return NULL;
	}

	appendPQExpBuffer(buf, "{\"action\":\"%c\",\"xid\":%u",
					  msg->action, msg->xid);

	switch (msg->action)
	{
		case 'B':
		case 'C':
		{
			/* transaction control messages only carry the xid */
			break;
		}

		case 'I':
		{
			appendRelation(buf, msg->nspname, msg->relname);
			appendTuple(buf, "columns", msg->new_cols, msg->ncols_new, false);
			break;
		}

		case 'U':
		{
			appendRelation(buf, msg->nspname, msg->relname);
			appendTuple(buf, "columns", msg->new_cols, msg->ncols_new, false);
			appendTuple(buf, "identity", msg->old_cols, msg->ncols_old,
						msg->oldType == 'K');
			break;
		}

		case 'D':
		{
			appendRelation(buf, msg->nspname, msg->relname);
			appendTuple(buf, "identity", msg->old_cols, msg->ncols_old,
						msg->oldType == 'K');
			break;
		}

		case 'T':
		{
			appendRelation(buf, msg->nspname, msg->relname);
			break;
		}

		default:
		{
			break;
		}
	}

	appendPQExpBufferChar(buf, '}');

	return finishJSON(buf);
}


/*
 * pgoutputTruncateJSON builds the JSON message for one relation of a TRUNCATE
 * that targets several relations at once. Index 0 is already emitted by
 * preparePgoutputMessage.
 */
char *
pgoutputTruncateJSON(StreamContext *privateContext, int relIndex)
{
	PgoutputMessage *msg = &(privateContext->pgoutputMsg);

	if (relIndex < 0 || relIndex >= msg->ntruncate)
	{
		log_error("BUG: pgoutputTruncateJSON called with index %d of %d",
				  relIndex, msg->ntruncate);
		return NULL;
	}

	PQExpBuffer buf = createPQExpBuffer();

	if (buf == NULL)
	{
		log_error(ALLOCATION_FAILED_ERROR);
		return NULL;
	}

	appendPQExpBuffer(buf, "{\"action\":\"T\",\"xid\":%u", msg->xid);

	appendRelation(buf,
				   msg->truncate[relIndex].nspname,
				   msg->truncate[relIndex].relname);

	appendPQExpBufferChar(buf, '}');

	return finishJSON(buf);
}


/* ----------
 * Public API: parsePgoutputMessageActionAndXid
 * ----------
 */

/*
 * parsePgoutputMessageActionAndXid reads the first byte of the pgoutput binary
 * message in context->buffer and sets metadata->action, metadata->xid and
 * metadata->filterOut.
 */
bool
parsePgoutputMessageActionAndXid(LogicalStreamContext *context)
{
	StreamContext *privateContext = (StreamContext *) context->private;
	LogicalMessageMetadata *metadata = &(privateContext->metadata);

	const char *buf = context->buffer;
	int bufLen = context->bufferLen;

	if (bufLen < 1)
	{
		log_error("pgoutput: empty message (bufLen=%d)", bufLen);
		return false;
	}

	char msgtype = buf[0];
	int pos = 1;

	switch (msgtype)
	{
		case 'B':               /* BEGIN */
		{
			if (bufLen < 21)    /* 1 + 8 + 8 + 4 */
			{
				log_error("pgoutput: BEGIN message too short (%d bytes)",
						  bufLen);
				return false;
			}

			pgout_u64(buf, &pos, bufLen);   /* final_lsn */
			pgout_u64(buf, &pos, bufLen);   /* commit_time */
			uint32_t xid = pgout_u32(buf, &pos, bufLen);

			metadata->action = STREAM_ACTION_BEGIN;
			metadata->xid = xid;
			privateContext->currentXid = xid;
			break;
		}

		case 'C':               /* COMMIT */
		{
			metadata->action = STREAM_ACTION_COMMIT;
			metadata->xid = privateContext->currentXid;
			break;
		}

		case 'R':               /* RELATION - cache it, filter out */
		{
			if (!pgoutput_cache_relation(privateContext, buf, bufLen, pos))
			{
				log_error("pgoutput: failed to cache RELATION message");
				return false;
			}

			metadata->filterOut = true;
			break;
		}

		case 'Y':               /* TYPE - filter out */
		case 'O':               /* ORIGIN - filter out */
		{
			metadata->filterOut = true;
			break;
		}

		case 'I':               /* INSERT */
		case 'U':               /* UPDATE */
		case 'D':               /* DELETE */
		case 'T':               /* TRUNCATE */
		{
			if (bufLen < 5)
			{
				log_error("pgoutput: DML message too short (%d bytes)", bufLen);
				return false;
			}

			switch (msgtype)
			{
				case 'I':
				{
					metadata->action = STREAM_ACTION_INSERT;
					break;
				}

				case 'U':
				{
					metadata->action = STREAM_ACTION_UPDATE;
					break;
				}

				case 'D':
				{
					metadata->action = STREAM_ACTION_DELETE;
					break;
				}

				case 'T':
				{
					metadata->action = STREAM_ACTION_TRUNCATE;
					break;
				}
			}

			metadata->xid = privateContext->currentXid;

			/*
			 * A TRUNCATE message starts with the relation count, not with a
			 * relation OID, so the pgcopydb schema check below only applies to
			 * the DML messages.
			 */
			if (msgtype == 'T')
			{
				break;
			}

			uint32_t relOid = pgout_u32(buf, &pos, bufLen);

			PgoutputRelationCache *rel = NULL;
			HASH_FIND_INT(privateContext->pgoutputRelationCache, &relOid, rel);

			if (rel != NULL && streq(rel->nspname, "pgcopydb"))
			{
				log_debug("pgoutput: filtering out %c message for pgcopydb.%s",
						  msgtype, rel->relname);
				metadata->filterOut = true;
			}

			break;
		}

		default:
		{
			log_debug("pgoutput: unknown message type '%c' (0x%02x), "
					  "filtering out",
					  msgtype, (unsigned char) msgtype);
			metadata->filterOut = true;
			break;
		}
	}

	return true;
}


/* ----------
 * Public API: preparePgoutputMessage
 * ----------
 */

/*
 * preparePgoutputMessage decodes the binary pgoutput message into
 * privateContext->pgoutputMsg and then serialises it into
 * metadata->jsonBuffer, using the same JSON shape that wal2json produces.
 */
bool
preparePgoutputMessage(LogicalStreamContext *context)
{
	StreamContext *privateContext = (StreamContext *) context->private;
	LogicalMessageMetadata *metadata = &(privateContext->metadata);
	PgoutputMessage *msg = &(privateContext->pgoutputMsg);

	const char *buf = context->buffer;
	int bufLen = context->bufferLen;

	/* reset the message struct */
	free_pgoutput_message(msg);
	memset(msg, 0, sizeof(PgoutputMessage));

	if (bufLen < 1)
	{
		log_error("pgoutput: empty message (bufLen=%d)", bufLen);
		return false;
	}

	char msgtype = buf[0];
	int pos = 1;

	msg->action = msgtype;
	msg->xid = metadata->xid;
	msg->lsn = metadata->lsn;

	switch (msgtype)
	{
		case 'B':
		{
			/* already parsed in parsePgoutputMessageActionAndXid */
			break;
		}

		case 'C':
		{
			/* clear the XID tracking after COMMIT is decoded */
			privateContext->currentXid = 0;
			break;
		}

		case 'I':
		{
			uint32_t relOid = pgout_u32(buf, &pos, bufLen);
			PgoutputRelationCache *rel = NULL;
			HASH_FIND_INT(privateContext->pgoutputRelationCache, &relOid, rel);

			if (rel == NULL)
			{
				log_error("pgoutput: INSERT for uncached relOid %u", relOid);
				return false;
			}

			strlcpy(msg->nspname, rel->nspname, sizeof(msg->nspname));
			strlcpy(msg->relname, rel->relname, sizeof(msg->relname));

			uint8_t marker = pgout_u8(buf, &pos, bufLen);

			if (marker != 'N')
			{
				log_error("pgoutput: INSERT expected 'N' marker, got '%c'",
						  marker);
				return false;
			}

			if (!decode_tuple(buf, bufLen, &pos, rel,
							  &msg->new_cols, &msg->ncols_new))
			{
				return false;
			}

			msg->oldType = 0;
			break;
		}

		case 'U':
		{
			uint32_t relOid = pgout_u32(buf, &pos, bufLen);
			PgoutputRelationCache *rel = NULL;
			HASH_FIND_INT(privateContext->pgoutputRelationCache, &relOid, rel);

			if (rel == NULL)
			{
				log_error("pgoutput: UPDATE for uncached relOid %u", relOid);
				return false;
			}

			strlcpy(msg->nspname, rel->nspname, sizeof(msg->nspname));
			strlcpy(msg->relname, rel->relname, sizeof(msg->relname));

			uint8_t next = pgout_u8(buf, &pos, bufLen);

			if (next == 'K' || next == 'O')
			{
				msg->oldType = (char) next;

				if (!decode_tuple(buf, bufLen, &pos, rel,
								  &msg->old_cols, &msg->ncols_old))
				{
					return false;
				}

				/* read the 'N' marker for the new tuple */
				uint8_t n_marker = pgout_u8(buf, &pos, bufLen);

				if (n_marker != 'N')
				{
					log_error("pgoutput: UPDATE expected 'N' after old tuple, "
							  "got '%c'", n_marker);
					return false;
				}
			}
			else if (next == 'N')
			{
				/*
				 * No old tuple sent: REPLICA IDENTITY is DEFAULT and the key
				 * columns did not change. Synthesize the old key tuple from
				 * the new tuple below.
				 */
				msg->oldType = 'K';
			}
			else
			{
				log_error("pgoutput: UPDATE unexpected marker '%c'", next);
				return false;
			}

			if (!decode_tuple(buf, bufLen, &pos, rel,
							  &msg->new_cols, &msg->ncols_new))
			{
				return false;
			}

			if (next == 'N')
			{
				/* count the replica identity columns */
				int nkey = 0;

				for (int i = 0; i < msg->ncols_new; i++)
				{
					PgoutputAttrCache *attr = NULL;
					HASH_FIND_INT(rel->attrs, &i, attr);

					if (attr != NULL && attr->isReplicaIdentity)
					{
						nkey++;
					}
				}

				if (nkey == 0)
				{
					log_error("pgoutput: UPDATE without old tuple and no "
							  "replica identity columns for %s.%s",
							  rel->nspname, rel->relname);
					return false;
				}

				msg->ncols_old = nkey;
				msg->old_cols =
					(PgoutputColumn *) calloc(nkey, sizeof(PgoutputColumn));

				if (msg->old_cols == NULL)
				{
					log_error(ALLOCATION_FAILED_ERROR);
					return false;
				}

				int ki = 0;

				for (int i = 0; i < msg->ncols_new && ki < nkey; i++)
				{
					PgoutputAttrCache *attr = NULL;
					HASH_FIND_INT(rel->attrs, &i, attr);

					if (attr == NULL || !attr->isReplicaIdentity)
					{
						continue;
					}

					msg->old_cols[ki] = msg->new_cols[i];

					/* deep-copy so both tuples own their data */
					if (msg->new_cols[i].status == 't' &&
						msg->new_cols[i].value != NULL)
					{
						msg->old_cols[ki].value =
							strdup(msg->new_cols[i].value);

						if (msg->old_cols[ki].value == NULL)
						{
							log_error(ALLOCATION_FAILED_ERROR);
							return false;
						}
					}

					ki++;
				}
			}

			break;
		}

		case 'D':
		{
			uint32_t relOid = pgout_u32(buf, &pos, bufLen);
			PgoutputRelationCache *rel = NULL;
			HASH_FIND_INT(privateContext->pgoutputRelationCache, &relOid, rel);

			if (rel == NULL)
			{
				log_error("pgoutput: DELETE for uncached relOid %u", relOid);
				return false;
			}

			strlcpy(msg->nspname, rel->nspname, sizeof(msg->nspname));
			strlcpy(msg->relname, rel->relname, sizeof(msg->relname));

			uint8_t marker = pgout_u8(buf, &pos, bufLen);

			if (marker != 'K' && marker != 'O')
			{
				log_error("pgoutput: DELETE expected 'K' or 'O', got '%c'",
						  marker);
				return false;
			}

			msg->oldType = (char) marker;

			if (!decode_tuple(buf, bufLen, &pos, rel,
							  &msg->old_cols, &msg->ncols_old))
			{
				return false;
			}

			break;
		}

		case 'T':
		{
			uint32_t nrelids = pgout_u32(buf, &pos, bufLen);
			pgout_u8(buf, &pos, bufLen);    /* flags (cascade, restart_seqs) */

			if (nrelids == 0)
			{
				log_error("pgoutput: TRUNCATE message with no relation");
				return false;
			}

			msg->truncate = (PgoutputTruncateRel *)
							calloc(nrelids, sizeof(PgoutputTruncateRel));

			if (msg->truncate == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				return false;
			}

			for (uint32_t i = 0; i < nrelids; i++)
			{
				uint32_t relOid = pgout_u32(buf, &pos, bufLen);

				PgoutputRelationCache *rel = NULL;
				HASH_FIND_INT(privateContext->pgoutputRelationCache,
							  &relOid, rel);

				if (rel == NULL)
				{
					log_error("pgoutput: TRUNCATE for uncached relOid %u",
							  relOid);
					return false;
				}

				strlcpy(msg->truncate[i].nspname, rel->nspname,
						sizeof(msg->truncate[i].nspname));
				strlcpy(msg->truncate[i].relname, rel->relname,
						sizeof(msg->truncate[i].relname));
			}

			msg->ntruncate = (int) nrelids;

			/* the first relation is the one emitted by the main JSON buffer */
			strlcpy(msg->nspname, msg->truncate[0].nspname,
					sizeof(msg->nspname));
			strlcpy(msg->relname, msg->truncate[0].relname,
					sizeof(msg->relname));

			break;
		}

		default:
		{
			/* filtered-out type - nothing to decode */
			break;
		}
	}

	metadata->jsonBuffer = pgoutputMessageToJSON(msg);

	if (metadata->jsonBuffer == NULL)
	{
		log_error("pgoutput: failed to serialise the %c message", msgtype);
		return false;
	}

	return true;
}


/*
 * free_pgoutput_message releases the memory owned by a decoded message.
 */
void
free_pgoutput_message(PgoutputMessage *msg)
{
	if (msg == NULL)
	{
		return;
	}

	for (int i = 0; i < msg->ncols_old; i++)
	{
		if (msg->old_cols != NULL && msg->old_cols[i].value != NULL)
		{
			free(msg->old_cols[i].value);
		}
	}

	for (int i = 0; i < msg->ncols_new; i++)
	{
		if (msg->new_cols != NULL && msg->new_cols[i].value != NULL)
		{
			free(msg->new_cols[i].value);
		}
	}

	if (msg->old_cols != NULL)
	{
		free(msg->old_cols);
	}

	if (msg->new_cols != NULL)
	{
		free(msg->new_cols);
	}

	if (msg->truncate != NULL)
	{
		free(msg->truncate);
	}

	msg->old_cols = NULL;
	msg->new_cols = NULL;
	msg->truncate = NULL;
	msg->ncols_old = 0;
	msg->ncols_new = 0;
	msg->ntruncate = 0;
}
