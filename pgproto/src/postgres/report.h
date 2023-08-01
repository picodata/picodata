#pragma once

#include <module.h>

#include <stdbool.h>
#include <string.h>
#include <assert.h>

struct pg_port;

/**
 * Sqlstate error codes.
 * @note: See https://www.postgresql.org/docs/current/errcodes-appendix.html for
 * the list of all of them.
 */
#define ERRCODE_INTERNAL_ERROR		  "XX000"
#define ERRCODE_INVALID_PASSWORD	  "28P01"
#define ERRCODE_PROTOCOL_VIOLATION	  "08P01"
#define ERRCODE_FEATURE_NOT_SUPPORTED	  "0A000"
#define ERRCODE_CONNECTION_DOES_NOT_EXIST "08003"
#define ERRCODE_SUCCESSFUL_COMPLETION	  "00000"

/** Send ErrorResponse packet to the frontend.  */
void
send_message_to_frontend(int level, struct pg_port *port,
			 const char *sql_error_code, const char *fmt, ...);

/** Log a debug message. Message is not sent to the client. */
#define pg_debug(...) \
	say_debug(__VA_ARGS__)

/** Log an info message. Message is not sent to the client. */
#define pg_info(...) \
	say_info(__VA_ARGS__)

/** Log a warning message.  Message is not sent to the client. */
#define pg_warning(format, ...) \
	say_warning(format, __VA_ARGS__)

/** Log an error message.  Message is sent to the client if port != NULL. */
#define pg_error(port, sql_code, ...) do {	\
	say_error(__VA_ARGS__);	\
	send_message_to_frontend(S_ERROR, port, sql_code, __VA_ARGS__); \
} while (0)

/** Send a notice message to the client. Writes only to debug server log. */
#define pg_notice(port, ...) do {	\
	say_debug(__VA_ARGS__);	\
	send_message_to_frontend(S_INFO, port, ERRCODE_SUCCESSFUL_COMPLETION, \
				 __VA_ARGS__); \
} while (0)
