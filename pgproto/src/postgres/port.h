#pragma once

#include <stdint.h>

#include <module.h>
#include <stdarg.h>

enum PG_PORT_STATUS {
	/** No error happened. */
	PG_OK = 0,
	/** Connection error. */
	PG_ERR = -1,
	/** EOF. */
	PG_EOF = -2,
};

/**
 * Read and write buffer.
 * It is used to avoid repeating inefficient malloc/free patterns
 * while reading/writing packets and to reduce the number of I/O
 * operations.
 */
struct pg_port_buff {
	/** Data stored in the buffer */
	char *data;
	/** Memory allocated. */
	uint32_t size;
	/**
	 * Range from data and data + used considered as payload.
	 * For the read buffer it is the total amount of data read.
	 * For the write buffer it is the total amount of data written.
	 */
	uint32_t used;
	/**
	 * Position of the first byte to be processed.
	 * For the read buffer it is the position of data to be read.
	 * For the write buffer is the beginning of the message being
	 * sent.
	 */
	uint32_t pos;
};

/** Port represents a single connection with a postgres frontend. */
struct pg_port {
	/** iostream allows to exchange messages with the frontend */
	struct iostream io;

	/** Read buffers. See the comment to pg_port_buff. */
	struct pg_port_buff read_buff;
	/** Write buffers. See the comment to pg_port_buff. */
	struct pg_port_buff write_buff;

	/** Contains the corresponding status if the I/O operation failed. */
	enum PG_PORT_STATUS status;

	/**
	 * Parameters sent in startup message.
	 * NULL means that the value wasn't set.
	 */

	/** User name. */
	char *user;
};

/**
 * Create a port from the given iostream.
 * iostream will be moved so it shouldn't be used after the call.
 */
void
pg_port_create(struct pg_port *port, struct iostream *io);

/**
 * Destroy port and close the connection.
 * The client is not notified, so the reason to close the connection should
 * be explained via pg_error().
 */
void
pg_port_close(struct pg_port *port);

/**
 * Read a null-terminated string to the port buffer, set size to its length
 * excluding the terminator and return a pointer to the data read.
 *
 * @retval not-NULL on success.
 * @retval NULL on EOF or error,
 *         check pg_port::state to understand what happened.
 */
char *
pg_read_cstr(struct pg_port *port, size_t *size);

/**
 * Read size bytes to the port buffer and return a pointer to the data read.
 *
 * @retval not-NULL on success.
 * @retval NULL on EOF or error,
 *         check pg_port::state to understand what happened.
 */
void *
pg_read_bytes(struct pg_port *port, size_t size);

/**
 * Read uint8_t from the port.
 * Possible return values are PGCONN_OK, PGCONN_ERR and PGCON_EOF.
 */
int
pg_read_uint8(struct pg_port *port, uint8_t *result);

/**
 * Read uint16_t from the port.
 * Possible return values are PGCONN_OK, PGCONN_ERR and PGCON_EOF.
 */
int
pg_read_uint16(struct pg_port *port, uint16_t *result);

/**
 * Read uint32_t from the port.
 * Possible return values are PGCONN_OK, PGCONN_ERR and PGCON_EOF.
 */
int
pg_read_uint32(struct pg_port *port, uint32_t *result);

/**
 * Remove procced data from read buffer. After the call all pointers received
 * from pg_read becomes invalid.
 * Memory is ** not ** freed so there is no need to call it in case of an error.
 */
void
pg_read_gc(struct pg_port *port);

/**
 * Start forming a message to send to the frontend.
 */
void
pg_begin_msg(struct pg_port *port, uint8_t type);

/**
 * Send a formed message to the frontend.
 */
void
pg_end_msg(struct pg_port *port);

/** Append uint8_t value at the end of the message. Never fails. */
void
pg_write_uint8(struct pg_port *port, uint8_t value);

/** Append uint16_t value at the end of the message. Never fails. */
void
pg_write_uint16(struct pg_port *port, uint16_t value);

/** Append uint32_t value at the end of the message. Never fails. */
void
pg_write_uint32(struct pg_port *port, uint32_t value);

/** Append n bytes at the end of the message. Never fails. */
void
pg_write_bytes(struct pg_port *port, const void *bytes, size_t n);

/** The same as pg_write_uint8. */
static inline void
pg_write_byte(struct pg_port *port, uint8_t byte)
{
	return pg_write_uint8(port, byte);
}

/**
 * Append a null-terminated string according to the format at the and of the
 * message using variadic arguments. Never fails.
 */
void
pg_write_str_va(struct pg_port *port, const char *fmt, va_list args);

/**
 * Append a null-terminated string according to the format at the and of the
 * message. Never fails.
 */
void
pg_write_str(struct pg_port *port, const char *format, ...);

/**
 * Append the string length and the string itself without a trailing zero.
 */
void
pg_write_len_str(struct pg_port *port, const char *format, ...);

/**
 * Force sending of messages from the write buffer.
 * Flushing is performed automatically when the buffer becomes too large,
 * but in some cases the flushing is needed to avoid deadlock.
 */
void
pg_flush(struct pg_port *port);
