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

#include "auth.h"
#include "port.h"
#include "report.h"

#include <module.h>
#include <msgpuck.h>
#include <inttypes.h>

/**
 * * Taken from Postgres sources *
 * These are the authentication request codes sent by the backend.
 */
enum PG_AUTH_REQUEST_CODE {
	AUTH_REQ_OK	   = 0,	 /* User is authenticated  */
	AUTH_REQ_KRB4	   = 1,  /* Kerberos V4. Not supported any more. */
	AUTH_REQ_KRB5	   = 2,	 /* Kerberos V5. Not supported any more. */
	AUTH_REQ_PASSWORD  = 3,	 /* Password */
	AUTH_REQ_CRYPT	   = 4,	 /* crypt password. Not supported any more. */
	AUTH_REQ_MD5	   = 5,	 /* md5 password */
	AUTH_REQ_SCM_CREDS = 6,	 /* transfer SCM credentials */
	AUTH_REQ_GSS	   = 7,	 /* GSSAPI without wrap() */
	AUTH_REQ_GSS_CONT  = 8,	 /* Continue GSS exchanges */
	AUTH_REQ_SSPI	   = 9,	 /* SSPI negotiate without wrap() */
	AUTH_REQ_SASL	   = 10, /* Begin SASL authentication */
	AUTH_REQ_SASL_CONT = 11, /* Continue SASL authentication */
	AUTH_REQ_SASL_FIN  = 12	 /* Final SASL message */
};

/** Send AuthRequest packet */
static void
pg_send_auth_request(struct pg_port *port, enum PG_AUTH_REQUEST_CODE code,
		     const char *extra_data, size_t extra_len)
{
	pg_begin_msg(port, 'R');
	pg_write_uint32(port, code);
	if (extra_data != NULL)
		pg_write_bytes(port, extra_data, extra_len);
	pg_end_msg(port);

	/**
	 * Flush the message so the client can see it and send response.
	 * In case of AUTH_REQ_OK flush is performed when we are ready for
	 * queries.
	 */
	if (code != AUTH_REQ_OK)
		pg_flush(port);

	pg_debug("sent auth request(%d) to user \'%s\'", code, port->user);
}

/** AuthResponse packet format. */
struct auth_response {
	/* Packet type, equals to 'p' for auth response. */
	uint8_t type;
	/* Packet length, including len field itself. */
	uint32_t len;
	/* Authentication data. The context depends on authentication type. */
	char *data;
};

/** Receive AuthResponse packet. */
static int
pg_recv_auth_response(struct pg_port *port, struct auth_response *packet)
{
	if (pg_read_uint8(port, &packet->type) < 0) {
		/*
		 * If the client just disconnects without offering a password,
		 * don't make a log entry. This is legal per protocol spec and
		 * in fact commonly done by psql, so complaining just clutters
		 * the log.
		 *
		 * In case of error the corresponding message is already written
		 * to the log.
		 */
		return -1;
	}
	if (packet->type != 'p') {
		pg_error(port, ERRCODE_PROTOCOL_VIOLATION,
			 "expected password response, got \'%c\' message type",
			 packet->type);
		return -1;
	}

	if (pg_read_uint32(port, &packet->len) < 0)
		return -1;

	if (packet->len < sizeof(packet->len)) {
		pg_error(port, ERRCODE_PROTOCOL_VIOLATION,
			 "invalid auth response message length: %"PRIu32,
			 packet->len);
		return -1;
	}
	uint32_t to_read = packet->len - sizeof(packet->len);

	packet->data = pg_read_bytes(port, to_read);
	if (packet->data == NULL)
		return -1;

	pg_debug("received auth response from user \'%s\'", port->user);
	pg_read_gc(port);
	return 0;
}

enum {
	/**
	 * 64 seems to be enough because it is almost twice the payload:
	 * 35 bytes for password and 3 bytes for method name.
	 */
	MD5_AUTH_PACKET_SIZE = 64,
};

/**
 * Encode an auth_packet in format that is used in `authenticate` function.
 * Packet is allocated on box region.
 */
void
encode_md5_auth_packet(char *buff,
		       const char *client_pass, size_t pass_len)
{
	const char *method = "md5";
	const size_t method_len = strlen("md5");
	uint32_t packet_size = mp_sizeof_array(2) + mp_sizeof_str(method_len) +
			       mp_sizeof_str(pass_len);
	assert(packet_size < MD5_AUTH_PACKET_SIZE);
	(void)packet_size;

	char *tuple = buff;
	tuple = mp_encode_array(tuple, 2);
	tuple = mp_encode_str(tuple, method, method_len);
	tuple = mp_encode_str(tuple, client_pass, pass_len);
}

/**
 * Perform md5 authentication message exchange and try to authenticate the user.
 * Return values are 0 in case of success and non-zero value in case of error.
 */
static int
pg_authenticate_md5(struct pg_port *port)
{
	uint32_t salt;
	random_bytes((char *)&salt, sizeof(salt));
	pg_send_auth_request(port, AUTH_REQ_MD5, (char *)&salt, sizeof(salt));

	struct auth_response response;
	if (pg_recv_auth_response(port, &response) != 0)
		return -1;

	const char *client_pass = response.data;
	size_t pass_len = strlen(response.data);

	char auth_packet[MD5_AUTH_PACKET_SIZE];
	encode_md5_auth_packet(auth_packet, client_pass, pass_len);
	int ret = authenticate(port->user, strlen(port->user),
			       (const char *)&salt, auth_packet);
	return ret;
}

/** Notify the client of successful authentication. */
static void
pg_send_auth_ok(struct pg_port *port)
{
	pg_send_auth_request(port, AUTH_REQ_OK, NULL, 0);
}

int
pg_authenticate(struct pg_port *port)
{
	/**
	 * @note: see ClientAuthentication()
	 *        from backend/libpq/auth.c and so on in postgres sources
	 */
	assert(port->user != NULL && "startup message was not processed");

	const char *auth_method =
		user_auth_method_name(port->user, strlen(port->user));
	if (auth_method == NULL)
		goto auth_failed;

	int rc = -1;
	if (strcmp(auth_method, "md5") == 0) {
		rc = pg_authenticate_md5(port);
	} else {
		pg_debug("unknown auth method %s", auth_method);
		goto auth_failed;
	}
	if (rc != 0)
		goto auth_failed;

	pg_send_auth_ok(port);
	pg_info("authenticated");
	return 0;

auth_failed:
	pg_error(port, ERRCODE_INVALID_PASSWORD,
		 "%s authentication failed for user \'%s\'",
		 auth_method != NULL ? auth_method : "", port->user);
	return -1;
}
