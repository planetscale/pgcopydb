/*
 * Copyright (c) 2017 rxi
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

#include <errno.h>
#include <limits.h>
#include <poll.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "snprintf.h"

#include "log.h"

#define LOG_MAX_SINKS 8
#define LOG_STACK_BUFSIZE 16384
#define LOG_PENDING_SIZE 65536
#define LOG_PENDING_MAX_MESSAGE 4096
#define LOG_TRUNCATED " ... [truncated]"

typedef enum
{
	LOG_FORMAT_TEXT = 0,
	LOG_FORMAT_JSON
} logFormat;

typedef enum
{
	LOG_STYLE_STDERR = 0,
	LOG_STYLE_FILE
} logStyle;

typedef enum
{
	LOG_KIND_UNKNOWN = 0,
	LOG_KIND_ATOMIC,
	LOG_KIND_STREAM
} logKind;

typedef struct LogSink
{
	int fd;
	logFormat format;
	logStyle style;
	logKind kind;
	int level;
	bool followVerbose;
	bool runId;
	size_t maxMessageSize;
	bool enabled;
	bool failed;
	bool failureReported;
	int failedErrno;
} LogSink;

typedef struct LogRecord
{
	int level;
	const char *file;
	int line;
	int pid;
	const char *message;
	size_t messageLength;
	const char *longTime;
	const char *stderrTime;
	const char *fileLine;
	bool showLineNumber;
	const LogField *fields;
	int fieldCount;
	bool prefixEachLine;
} LogRecord;

static struct
{
	void *udata;
	log_LockFn lock;
	int level;
	int quiet;
	int showLineNumber;
	int useColors;
	logFormat fileFormat;
	int fileSink;
	char tformat[128];
	char role[64];
	char runId[64];
	int depth;
	int sinkCount;
	LogSink sinks[LOG_MAX_SINKS];
	int pendingPid;
	bool pendingClosed;
	size_t pendingLength;
	char pending[LOG_PENDING_SIZE];
} L = {
	.fileSink = -1,
	.sinkCount = 1,
	.sinks = {
		{ .fd = STDERR_FILENO, .level = LOG_LEVEL_FOLLOW, .enabled = true }
	}
};


static const char *level_names[] = {
	"TRACE",
	"SQLite",
	"DEBUG",
	"SQL",
	"NOTICE",
	"INFO",
	"WARN",
	"ERROR",
	"FATAL"
};

static const char *level_colors[] = {
	"\x1b[90m",
	"\x1b[30m",
	"\x1b[34m",
	"\x1b[30m",
	"\x1b[36m",
	"\x1b[32m",
	"\x1b[33m",
	"\x1b[31m",
	"\x1b[35m"
};


static int
add_sink(int fd, logFormat format, logStyle style, int level, size_t maxMessageSize)
{
	if (L.sinkCount >= LOG_MAX_SINKS)
	{
		return -1;
	}

	L.sinks[L.sinkCount] = (LogSink) {
		.fd = fd,
		.format = format,
		.style = style,
		.level = level,
		.maxMessageSize = maxMessageSize,
		.enabled = true
	};

	return L.sinkCount++;
}


void
log_set_udata(void *udata)
{
	L.udata = udata;
}


void
log_set_lock(log_LockFn fn)
{
	L.lock = fn;
}


void
log_set_fp(FILE *fp)
{
	log_set_fd(fp == NULL ? -1 : fileno(fp));
}


void
log_set_fd(int fd)
{
	if (fd < 0)
	{
		if (L.fileSink >= 0)
		{
			L.sinks[L.fileSink].enabled = false;
		}
		return;
	}

	if (L.fileSink >= 0)
	{
		L.sinks[L.fileSink] = (LogSink) {
			.fd = fd,
			.format = L.fileFormat,
			.style = LOG_STYLE_FILE,
			.level = LOG_LEVEL_FOLLOW,
			.enabled = true
		};
		return;
	}

	L.fileSink = add_sink(fd, L.fileFormat, LOG_STYLE_FILE, LOG_LEVEL_FOLLOW, 0);
}


void
log_set_level(int level)
{
	L.level = level;
}


int
log_get_level(void)
{
	return L.level;
}


void
log_set_quiet(int enable)
{
	L.quiet = enable ? 1 : 0;
}


void
log_use_colors(int enable)
{
	L.useColors = enable ? 1 : 0;
}


void
log_use_json(int enable)
{
	L.sinks[0].format = enable ? LOG_FORMAT_JSON : LOG_FORMAT_TEXT;
}


void
log_use_json_file(int enable)
{
	L.fileFormat = enable ? LOG_FORMAT_JSON : LOG_FORMAT_TEXT;

	if (L.fileSink >= 0)
	{
		L.sinks[L.fileSink].format = L.fileFormat;
	}
}


void
log_show_file_line(int enable)
{
	L.showLineNumber = enable ? 1 : 0;
}


void
log_set_tformat(const char *tformat)
{
	strlcpy(L.tformat, tformat, sizeof(L.tformat));
}


void
log_set_role(const char *role)
{
	strlcpy(L.role, role == NULL ? "" : role, sizeof(L.role));
}


void
log_set_run_id(const char *runId)
{
	strlcpy(L.runId, runId == NULL ? "" : runId, sizeof(L.runId));
}


int
log_level_from_name(const char *name)
{
	for (int level = LOG_TRACE; level <= LOG_FATAL; level++)
	{
		if (strcasecmp(name, level_names[level]) == 0)
		{
			return level;
		}
	}

	return -1;
}


int
log_add_sink(int fd, int json, int level, int followVerbose,
			 size_t maxMessageSize)
{
	int sink = add_sink(fd,
						json ? LOG_FORMAT_JSON : LOG_FORMAT_TEXT,
						LOG_STYLE_FILE,
						level,
						maxMessageSize);

	if (sink >= 0)
	{
		L.sinks[sink].followVerbose = followVerbose != 0;
		L.sinks[sink].runId = true;
	}

	return sink;
}


void
log_set_sink_level(int sink, int level)
{
	if (sink >= 0 && sink < L.sinkCount)
	{
		L.sinks[sink].level = level;
	}
}


static int
sink_min_level(LogSink *sink)
{
	if (sink->level == LOG_LEVEL_FOLLOW)
	{
		return L.level;
	}

	if (sink->followVerbose && L.level < sink->level)
	{
		return L.level;
	}

	return sink->level;
}


static bool
sink_accepts(int index, int level)
{
	LogSink *sink = &(L.sinks[index]);

	if (!sink->enabled || sink->failed || (index == 0 && L.quiet))
	{
		return false;
	}

	return level >= sink_min_level(sink);
}


static bool
pending_capturing(void)
{
	return !L.pendingClosed &&
		   (L.pendingPid == 0 || L.pendingPid == (int) getpid());
}


static logKind
sink_kind(LogSink *sink)
{
	if (sink->kind == LOG_KIND_UNKNOWN)
	{
		struct stat st;

		if (fstat(sink->fd, &st) == 0 &&
			(S_ISFIFO(st.st_mode) || S_ISSOCK(st.st_mode)))
		{
			sink->kind = LOG_KIND_STREAM;
		}
		else
		{
			sink->kind = LOG_KIND_ATOMIC;
		}
	}

	return sink->kind;
}


static bool
write_all(int fd, const char *buffer, size_t length)
{
	int waits = 0;

	while (length > 0)
	{
		ssize_t written = write(fd, buffer, length);

		if (written > 0)
		{
			buffer += written;
			length -= written;
			continue;
		}

		if (written < 0 && errno == EINTR)
		{
			continue;
		}

		if (written < 0 &&
			(errno == EAGAIN || errno == EWOULDBLOCK) &&
			waits++ < 50)
		{
			struct pollfd pfd = { .fd = fd, .events = POLLOUT };

			(void) poll(&pfd, 1, 100);
			continue;
		}

		if (written == 0)
		{
			errno = EIO;
		}

		return false;
	}

	return true;
}


static void
sink_write(LogSink *sink, const char *buffer, size_t length)
{
	bool locked = false;

	if (length > PIPE_BUF &&
		L.lock != NULL &&
		sink_kind(sink) == LOG_KIND_STREAM)
	{
		L.lock(L.udata, 1);
		locked = true;
	}

	bool success = write_all(sink->fd, buffer, length);
	int writeErrno = errno;

	if (locked)
	{
		L.lock(L.udata, 0);
	}

	if (!success)
	{
		sink->failed = true;
		sink->failedErrno = writeErrno;
	}
}


static size_t
utf8_sequence_length(const unsigned char *string, size_t length)
{
	unsigned char c = string[0];
	unsigned char low = 0x80;
	unsigned char high = 0xBF;
	size_t expected = 0;

	if (c < 0x80)
	{
		return 1;
	}
	else if (c >= 0xC2 && c <= 0xDF)
	{
		expected = 2;
	}
	else if (c == 0xE0)
	{
		expected = 3;
		low = 0xA0;
	}
	else if ((c >= 0xE1 && c <= 0xEC) || c == 0xEE || c == 0xEF)
	{
		expected = 3;
	}
	else if (c == 0xED)
	{
		expected = 3;
		high = 0x9F;
	}
	else if (c == 0xF0)
	{
		expected = 4;
		low = 0x90;
	}
	else if (c >= 0xF1 && c <= 0xF3)
	{
		expected = 4;
	}
	else if (c == 0xF4)
	{
		expected = 4;
		high = 0x8F;
	}
	else
	{
		return 0;
	}

	if (length < expected || string[1] < low || string[1] > high)
	{
		return 0;
	}

	for (size_t i = 2; i < expected; i++)
	{
		if (string[i] < 0x80 || string[i] > 0xBF)
		{
			return 0;
		}
	}

	return expected;
}


static size_t
json_escape(char *out, const char *string, size_t length)
{
	const unsigned char *bytes = (const unsigned char *) string;
	size_t written = 0;
	size_t i = 0;

	while (i < length)
	{
		unsigned char c = bytes[i];
		const char *escape = NULL;
		char unicode[8];
		size_t consumed = 1;

		switch (c)
		{
			case '"':
			{
				escape = "\\\"";
				break;
			}

			case '\\':
			{
				escape = "\\\\";
				break;
			}

			case '/':
			{
				escape = "\\/";
				break;
			}

			case '\b':
			{
				escape = "\\b";
				break;
			}

			case '\f':
			{
				escape = "\\f";
				break;
			}

			case '\n':
			{
				escape = "\\n";
				break;
			}

			case '\r':
			{
				escape = "\\r";
				break;
			}

			case '\t':
			{
				escape = "\\t";
				break;
			}

			default:
			{
				if (c < 0x20)
				{
					snprintf(unicode, sizeof(unicode), "\\u%04x", c);
					escape = unicode;
				}
				else if (c >= 0x80)
				{
					consumed = utf8_sequence_length(bytes + i, length - i);

					if (consumed == 0)
					{
						escape = "\\ufffd";
						consumed = 1;
					}
				}
				break;
			}
		}

		if (escape != NULL)
		{
			size_t escapeLength = strlen(escape);

			if (out != NULL)
			{
				memcpy(out + written, escape, escapeLength);
			}
			written += escapeLength;
		}
		else
		{
			if (out != NULL)
			{
				memcpy(out + written, string + i, consumed);
			}
			written += consumed;
		}

		i += consumed;
	}

	return written;
}


static size_t
append_bytes(char *out, size_t position, const char *string, size_t length)
{
	if (out != NULL)
	{
		memcpy(out + position, string, length);
	}

	return position + length;
}


static size_t
append_string(char *out, size_t position, const char *string)
{
	return append_bytes(out, position, string, strlen(string));
}


static size_t
append_json(char *out, size_t position, const char *string, size_t length)
{
	return position + json_escape(out == NULL ? NULL : out + position,
								  string,
								  length);
}


static size_t
append_int(char *out, size_t position, int value)
{
	char buffer[16];
	int length = snprintf(buffer, sizeof(buffer), "%d", value);

	return append_bytes(out, position, buffer, length);
}


static size_t
build_text_prefix(char *out, size_t size, LogSink *sink, LogRecord *record)
{
	int length = 0;

	if (sink->style == LOG_STYLE_FILE)
	{
		length = pg_snprintf(out, size, "%s %d %s %s ",
							 record->longTime,
							 record->pid,
							 level_names[record->level],
							 record->fileLine);
	}
	else if (L.useColors)
	{
		length = pg_snprintf(out, size, "%s %d %s%-6s\x1b[0m ",
							 record->stderrTime,
							 record->pid,
							 level_colors[record->level],
							 level_names[record->level]);

		if (record->showLineNumber)
		{
			length += pg_snprintf(out + length, size - length,
								  "\x1b[90m%-25s\x1b[0m ",
								  record->fileLine);
		}
	}
	else
	{
		length = pg_snprintf(out, size, "%s %d %-6s ",
							 record->stderrTime,
							 record->pid,
							 level_names[record->level]);

		if (record->showLineNumber)
		{
			length += pg_snprintf(out + length, size - length,
								  "%-25s ",
								  record->fileLine);
		}
	}

	return length < 0 ? 0 : (size_t) length;
}


static size_t
build_record(char *out, LogSink *sink, LogRecord *record,
			 const char *prefix, size_t prefixLength)
{
	size_t messageLength = record->messageLength;
	bool truncated = false;

	if (sink->maxMessageSize > 0 && messageLength > sink->maxMessageSize)
	{
		messageLength = sink->maxMessageSize;
		truncated = true;
	}

	size_t position = 0;

	if (sink->format == LOG_FORMAT_TEXT)
	{
		size_t start = 0;

		do {
			size_t end = messageLength;

			if (record->prefixEachLine)
			{
				const char *newline = memchr(record->message + start, '\n',
											 messageLength - start);

				if (newline != NULL)
				{
					end = newline - record->message;
				}
			}

			position = append_bytes(out, position, prefix, prefixLength);
			position = append_bytes(out, position,
									record->message + start,
									end - start);

			if (end == messageLength && truncated)
			{
				position = append_string(out, position, LOG_TRUNCATED);
			}

			position = append_string(out, position, "\n");
			start = end + 1;
		} while (start <= messageLength);

		return position;
	}

	position = append_string(out, position, "{\"timestamp\":\"");
	position = append_json(out, position, record->longTime,
						   strlen(record->longTime));
	position = append_string(out, position, "\",\"pid\":");
	position = append_int(out, position, record->pid);
	position = append_string(out, position, ",\"error_level\":");
	position = append_int(out, position, record->level);
	position = append_string(out, position, ",\"error_severity\":\"");
	position = append_string(out, position, level_names[record->level]);
	position = append_string(out, position, "\",\"file_name\":\"");
	position = append_json(out, position, record->file, strlen(record->file));
	position = append_string(out, position, "\",\"file_line_num\":");
	position = append_int(out, position, record->line);
	position = append_string(out, position, ",\"message\":\"");
	position = append_json(out, position, record->message, messageLength);

	if (truncated)
	{
		position = append_string(out, position, LOG_TRUNCATED);
	}

	position = append_string(out, position, "\"");

	for (int i = 0; i < record->fieldCount; i++)
	{
		const LogField *field = &(record->fields[i]);

		if (field->key == NULL || field->value == NULL)
		{
			continue;
		}

		size_t valueLength = strlen(field->value);
		bool valueTruncated = false;

		if (sink->maxMessageSize > 0 && valueLength > sink->maxMessageSize)
		{
			valueLength = sink->maxMessageSize;
			valueTruncated = true;
		}

		position = append_string(out, position, ",\"");
		position = append_json(out, position, field->key, strlen(field->key));
		position = append_string(out, position, "\":");

		if (field->number && !valueTruncated)
		{
			position = append_bytes(out, position, field->value, valueLength);
			continue;
		}

		position = append_string(out, position, "\"");
		position = append_json(out, position, field->value, valueLength);

		if (valueTruncated)
		{
			position = append_string(out, position, LOG_TRUNCATED);
		}

		position = append_string(out, position, "\"");
	}

	if (L.role[0] != '\0')
	{
		position = append_string(out, position, ",\"role\":\"");
		position = append_json(out, position, L.role, strlen(L.role));
		position = append_string(out, position, "\"");
	}

	if (sink->runId && L.runId[0] != '\0')
	{
		position = append_string(out, position, ",\"run_id\":\"");
		position = append_json(out, position, L.runId, strlen(L.runId));
		position = append_string(out, position, "\"");
	}

	return append_string(out, position, "}\n");
}


static void
pending_capture(LogRecord *record, char *buffer, size_t size)
{
	LogSink jsonSink = {
		.format = LOG_FORMAT_JSON,
		.style = LOG_STYLE_FILE,
		.runId = true,
		.maxMessageSize = LOG_PENDING_MAX_MESSAGE
	};

	L.pendingPid = (int) getpid();

	size_t length = build_record(NULL, &jsonSink, record, NULL, 0);

	if (length + 1 > size ||
		L.pendingLength + length + 1 > sizeof(L.pending))
	{
		L.pendingClosed = true;
		return;
	}

	(void) build_record(buffer, &jsonSink, record, NULL, 0);

	L.pending[L.pendingLength++] = (char) record->level;
	memcpy(L.pending + L.pendingLength, buffer, length);
	L.pendingLength += length;
}


void
log_replay_pending(int index)
{
	if (index < 0 || index >= L.sinkCount || !pending_capturing())
	{
		L.pendingClosed = true;
		return;
	}

	LogSink *sink = &(L.sinks[index]);
	size_t position = 0;

	while (position < L.pendingLength && !sink->failed)
	{
		int level = L.pending[position++];
		const char *line = L.pending + position;
		const char *end = memchr(line, '\n', L.pendingLength - position);

		if (end == NULL)
		{
			break;
		}

		size_t length = end - line + 1;

		if (level >= sink_min_level(sink))
		{
			sink_write(sink, line, length);
		}

		position += length;
	}

	L.pendingClosed = true;
	L.pendingLength = 0;
}


static void
format_time(char *out, size_t size, const char *tformat, struct tm *tm, int millis)
{
	char timebuf[32] = { 0 };

	timebuf[strftime(timebuf, sizeof(timebuf), tformat, tm)] = '\0';
	snprintf(out, size, "%s.%03d", timebuf, millis);
}


static void log_nested(const char *fmt, va_list args)
	__attribute__((format(printf, 1, 0)));

static void
log_nested(const char *fmt, va_list args)
{
	char message[1024];
	int length = pg_vsnprintf(message, sizeof(message) - 1, fmt, args);

	if (length < 0)
	{
		return;
	}

	if ((size_t) length > sizeof(message) - 2)
	{
		length = sizeof(message) - 2;
	}

	message[length++] = '\n';
	(void) write_all(STDERR_FILENO, message, length);
}


static void
report_failed_sinks(void)
{
	for (int i = 0; i < L.sinkCount; i++)
	{
		LogSink *sink = &(L.sinks[i]);

		if (sink->failed && !sink->failureReported)
		{
			sink->failureReported = true;

			log_error("Failed to write to the log on fd %d: %s, "
					  "process %d stops writing to it",
					  sink->fd,
					  strerror(sink->failedErrno),
					  (int) getpid());
		}
	}
}


static void log_vlog(int level, const char *file, int line,
					 const LogField *fields, int fieldCount,
					 bool prefixEachLine, int savedErrno,
					 const char *fmt, va_list args)
	__attribute__((format(printf, 8, 0)));


void
log_log(int level, const char *file, int line, const char *fmt, ...)
{
	int savedErrno = errno;
	va_list args;

	va_start(args, fmt);
	log_vlog(level, file, line, NULL, 0, false, savedErrno, fmt, args);
	va_end(args);

	errno = savedErrno;
}


void
log_log_fields(int level, const char *file, int line,
			   const LogField *fields, int fieldCount,
			   const char *fmt, ...)
{
	int savedErrno = errno;
	va_list args;

	va_start(args, fmt);
	log_vlog(level, file, line, fields, fieldCount, true, savedErrno, fmt, args);
	va_end(args);

	errno = savedErrno;
}


static void
log_vlog(int level, const char *file, int line,
		 const LogField *fields, int fieldCount,
		 bool prefixEachLine, int savedErrno,
		 const char *fmt, va_list args)
{
	if (fmt == NULL)
	{
		return;
	}

	bool accepted = pending_capturing();

	for (int i = 0; i < L.sinkCount && !accepted; i++)
	{
		accepted = sink_accepts(i, level);
	}

	if (!accepted)
	{
		return;
	}

	va_list copy;

	if (L.depth > 0)
	{
		va_copy(copy, args);
		errno = savedErrno;
		log_nested(fmt, copy);
		va_end(copy);
		return;
	}

	++L.depth;

	if (L.tformat[0] == '\0')
	{
		strlcpy(L.tformat, LOG_TFORMAT_LONG, sizeof(L.tformat));
	}

	struct timeval tv;
	struct tm tm;

	gettimeofday(&tv, NULL);
	localtime_r(&(tv.tv_sec), &tm);

	char longTime[128];
	char stderrTime[128];
	int millis = (int) tv.tv_usec / 1000;

	format_time(longTime, sizeof(longTime), LOG_TFORMAT_LONG, &tm, millis);
	format_time(stderrTime, sizeof(stderrTime), L.tformat, &tm, millis);

	char fileLine[25] = { 0 };
	pg_snprintf(fileLine, sizeof(fileLine), "%s:%d", file, line);

	char messageStack[LOG_STACK_BUFSIZE];
	char *messageHeap = NULL;
	char *message = messageStack;
	size_t messageLength = 0;

	va_copy(copy, args);
	errno = savedErrno;
	int needed = pg_vsnprintf(messageStack, sizeof(messageStack), fmt, copy);
	va_end(copy);

	if (needed < 0)
	{
		messageLength = strlcpy(messageStack,
								"Failed to format a log message",
								sizeof(messageStack));
	}
	else if ((size_t) needed < sizeof(messageStack))
	{
		messageLength = needed;
	}
	else if ((messageHeap = malloc(needed + 1)) != NULL)
	{
		va_copy(copy, args);
		errno = savedErrno;
		(void) pg_vsnprintf(messageHeap, needed + 1, fmt, copy);
		va_end(copy);

		message = messageHeap;
		messageLength = needed;
	}
	else
	{
		size_t keep = sizeof(messageStack) - sizeof(LOG_TRUNCATED);

		memcpy(messageStack + keep, LOG_TRUNCATED, sizeof(LOG_TRUNCATED));
		messageLength = keep + strlen(LOG_TRUNCATED);
	}

	LogRecord record = {
		.level = level,
		.file = file,
		.line = line,
		.pid = (int) getpid(),
		.message = message,
		.messageLength = messageLength,
		.longTime = longTime,
		.stderrTime = stderrTime,
		.fileLine = fileLine,
		.showLineNumber = L.showLineNumber || L.level <= LOG_DEBUG,
		.fields = fields,
		.fieldCount = fieldCount,
		.prefixEachLine = prefixEachLine
	};

	char lineStack[LOG_STACK_BUFSIZE];

	for (int i = 0; i < L.sinkCount; i++)
	{
		if (!sink_accepts(i, level))
		{
			continue;
		}

		LogSink *sink = &(L.sinks[i]);

		char prefix[512];
		size_t prefixLength = 0;

		if (sink->format == LOG_FORMAT_TEXT)
		{
			prefixLength = build_text_prefix(prefix, sizeof(prefix), sink, &record);
		}

		size_t length = build_record(NULL, sink, &record, prefix, prefixLength);
		char *lineHeap = NULL;
		char *out = lineStack;

		if (length > sizeof(lineStack))
		{
			lineHeap = malloc(length);
			out = lineHeap;
		}

		if (out == NULL)
		{
			LogSink cut = *sink;

			cut.maxMessageSize = sizeof(lineStack) / 8;
			length = build_record(lineStack, &cut, &record, prefix, prefixLength);
			sink_write(sink, lineStack, length);
			continue;
		}

		(void) build_record(out, sink, &record, prefix, prefixLength);
		sink_write(sink, out, length);

		free(lineHeap);
	}

	if (pending_capturing())
	{
		pending_capture(&record, lineStack, sizeof(lineStack));
	}

	free(messageHeap);

	--L.depth;

	report_failed_sinks();
}
