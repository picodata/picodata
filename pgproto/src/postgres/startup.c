/**
 * PostgreSQL Database Management System
 * (formerly known as Postgres, then as Postgres95)
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 1994, The Regents of the University of California
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

#include "port.h"
#include "startup.h"
#include "report.h"
#include "tarantool/diag.h"

#include <inttypes.h>
#include <string.h>

/** Extract major protocol version from the version number. */
static uint16_t
pg_protocol_major(uint32_t version)
{
	return ((version) >> 16);
}
/** Extract minor protocol version from the version number. */
static uint16_t
pg_protocol_minor(uint32_t version)
{
	return ((version) & 0x0000ffff);
}

/** Get a protocol version from major and minor versions. */
#define PG_PROTOCOL(m,n) (((m) << 16) | (n))

enum {
	/**
	 * The earliest and latest frontend/backend protocol version supported.
	 * (Only protocol version 3 is currently supported)
	 */
	PG_PROTOCOL_EARLIEST = PG_PROTOCOL(3,0),
	PG_PROTOCOL_LATEST = PG_PROTOCOL(3,0),
};

/** Cancel request message format. */
struct cancel_request_message {
	/** Note that each field is stored in network byte order! */

	/** code to identify a cancel request */
	uint32_t cancel_request_code;
	/** PID of client's backend */
	uint32_t backend_pid;
	/** secret key to authorize cancel */
	uint32_t cancel_auth_code;
};

enum {
	/**
	 * A client can also start by sending a request to get a secure channel.
	 */

	/** SSL negotiation request. */
	NEGOTIATE_SSL_CODE = PG_PROTOCOL(1234,5679),
	/** GSSAPI negotiation request. */
	NEGOTIATE_GSS_CODE = PG_PROTOCOL(1234,5680),

	/*
	 * A client can also send a cancel-current-operation request to the
	 * sever.
	 * The cancel request code must not match any protocol version number
	 * we're ever likely to use. This random choice should do.
	 */
	CANCEL_REQUEST_CODE = PG_PROTOCOL(1234,5678),

	/**
	 * In protocol 3.0 and later, the startup packet length is not fixed,
	 * but we set an arbitrary limit on it anyway. This is just to prevent
	 * simple denial-of-service attacks via sending enough data to run the
	 * server out of memory.
	 */
	MAX_STARTUP_PACKET_LENGTH = 10000,

	/**
	 * Startup message must contain at least size and request code fields.
	 * Both are represented by uint32_t type. The type byte is not included
	 * to the length, and BTW, startup packet doesn't have a type byte.
	 */
	MIN_STARTUP_PACKET_LENGTH = sizeof(uint32_t) + sizeof(uint32_t)

};
/** Structure of startup message packet. */
struct startup_message {
	/** Packet length, including len field itself. */
	uint32_t len;
	/** Protocol version or request code. */
	uint32_t version;
	/**
	 * Parameters in form of parameter name and value pairs.
	 *  Possible names: user, database, options, replication,
	 *		    _pq_.*, etc.
	 * There must be at least a user parameter.
	 */
	char *parameters;
};

/**
 * Receive startup packet pending on the port.
 */
static int
pg_recv_startup_message(struct pg_port *port,
			struct startup_message *message)
{
	memset(message, 0, sizeof(*message));

	if (pg_read_uint32(port, &message->len) < 0) {
		/*
		 * If we get no data, don't clutter the log with a complaint;
		 * such cases often occur for legitimate reasons.
		 * An example is that we might be here after responding to
		 * NEGOTIATE_SSL_CODE, and if the client didn't like our
		 * response, it'll probably just drop the connection.
		 * Service-monitoring software also often just opens and
		 * closes a connection without sending anything.
		 */
		return -1;
	}

	if (message->len < MIN_STARTUP_PACKET_LENGTH ||
	    message->len > MAX_STARTUP_PACKET_LENGTH) {
		pg_error(NULL, ERRCODE_PROTOCOL_VIOLATION,
			 "invalid startup message length: %"PRIu32,
			 message->len);
		return  -1;
	}

	if (pg_read_uint32(port, &message->version) < 0) {
		pg_error(NULL, ERRCODE_PROTOCOL_VIOLATION,
			 "incomplete startup message: no protocol");
		return -1;
	}

	if (message->version == CANCEL_REQUEST_CODE) {
		if (message->len != sizeof(struct cancel_request_message))
			pg_error(NULL, ERRCODE_PROTOCOL_VIOLATION,
				 "invalid length of startup packet");
		/** Not really an error, but we don't want to proceed further */
		return -1;
	}

	if (message->version == NEGOTIATE_SSL_CODE) {
		pg_error(port, ERRCODE_FEATURE_NOT_SUPPORTED,
			 "SSL is not supported");
		return -1;
	}
	size_t to_read = message->len -
		sizeof(message->len) - sizeof(message->version);
	message->parameters = pg_read_bytes(port, to_read);
	if (message->parameters == NULL) {
		pg_error(NULL, ERRCODE_PROTOCOL_VIOLATION,
			 "incomplete startup message: no parameters");
		return -1;
	}

	pg_read_gc(port);
	return 0;
}

/** Check that the protocol version is in the supported range. */
static int
pg_check_protocol_version(uint32_t version)
{
	uint16_t frontend_protocol_major = pg_protocol_major(version);
	uint16_t frontend_protocol_minor = pg_protocol_minor(version);
	uint16_t protocol_major_earliest =
		pg_protocol_major(PG_PROTOCOL_EARLIEST);
	uint16_t protocol_major_latest =
		pg_protocol_major(PG_PROTOCOL_LATEST);
	uint16_t protocol_minor_earliest =
		pg_protocol_minor(PG_PROTOCOL_EARLIEST);
	uint16_t protocol_minor_latest =
		pg_protocol_minor(PG_PROTOCOL_LATEST);
	return frontend_protocol_major >= protocol_major_earliest &&
	       frontend_protocol_major <= protocol_major_latest &&
	       frontend_protocol_minor >= protocol_minor_earliest &&
	       frontend_protocol_minor <= protocol_minor_latest;
}

int
pg_process_startup_message(struct pg_port *port)
{
	/**
	 * @note: see ProcessStartupPacket()
	 *        from backend/postmaster/postmaster.c in postgres sources.
	 */

	struct startup_message message;
	if (pg_recv_startup_message(port, &message) != 0)
		return -1;

	if (!pg_check_protocol_version(message.version)) {
		pg_error(port, ERRCODE_FEATURE_NOT_SUPPORTED,
			 "unsupported frontend protocol %u.%u: "
			 "server supports %u.0 to %u.%u",
			 pg_protocol_major(message.version),
			 pg_protocol_minor(message.version),
			 pg_protocol_major(PG_PROTOCOL_EARLIEST),
			 pg_protocol_major(PG_PROTOCOL_LATEST),
			 pg_protocol_minor(PG_PROTOCOL_LATEST));
		return -1;
	}

	const char *namevalue = message.parameters;
	while (*namevalue != '\0') {
		const char *name = namevalue;
		const char *value = name + strlen(name) + 1;
		if (strcmp(name, "user") == 0)
			port->user = xstrdup(value);
		else
			pg_error(NULL, ERRCODE_FEATURE_NOT_SUPPORTED,
				 "%s:%s is not supported for now",
				 name, value);

		namevalue = value + strlen(value) + 1;
	}

	if (port->user == NULL || port->user[0] == '\0') {
		pg_error(port, ERRCODE_PROTOCOL_VIOLATION,
			 "incomplete startup message: no user");
		return -1;
	}

	pg_debug("processed startup message for user \"%s\"", port->user);
	return 0;
}
