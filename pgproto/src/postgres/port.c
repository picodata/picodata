#include "port.h"
#include "report.h"
#include "tarantool/trivia/util.h"
#include "tarantool/diag.h"


#include <arpa/inet.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

static void
buff_create(struct pg_port_buff *buff, size_t size)
{
	buff->data = xmalloc(size);
	buff->size = size;
	buff->pos = 0;
	buff->used = 0;
}

static void
buff_destroy(struct pg_port_buff *buff)
{

	free(buff->data);
}

static void
buff_resize(struct pg_port_buff *buff, size_t new_size)
{
	buff->data = xrealloc(buff->data, new_size);
	buff->size = new_size;
}

static void
buff_gc(struct pg_port_buff *buff)
{
	size_t new_used = buff->used - buff->pos;
	/* Move unprocessed data to the beginning. */
	memmove(buff->data, buff->data + buff->pos, new_used);
	buff->pos = 0;
	buff->used = new_used;
}

static bool
buff_verify(struct pg_port_buff *buff)
{
	return (buff->used <= buff->size) && (buff->pos <= buff->used);
}

enum {
	/** Initial size of a read buffer. The same as in Postgres. */
	READ_BUFF_INITIAL_SIZE = 1 << 13,
	/** Initial size of a write buffer. The same as in Postgres. */
	WRITE_BUFF_INITIAL_SIZE = 1 << 13,
	/** Size above which flushing is performed. */
	WRITE_BUFF_FLUSH_IS_NEEDED_PAYLOAD = WRITE_BUFF_INITIAL_SIZE,
};

void
pg_port_create(struct pg_port *port, struct iostream *io)
{
	memset(port, 0, sizeof(*port));
	iostream_move(&port->io, io);
	buff_create(&port->read_buff, READ_BUFF_INITIAL_SIZE);
	buff_create(&port->write_buff, WRITE_BUFF_INITIAL_SIZE);
}

void
pg_port_close(struct pg_port *port)
{
	pg_flush(port);
	buff_destroy(&port->read_buff);
	buff_destroy(&port->write_buff);
	iostream_close(&port->io);
	free(port->user);
}

void *
pg_read_bytes(struct pg_port *port, size_t size)
{
	struct pg_port_buff *buff = &port->read_buff;
	assert(buff_verify(buff));

	if (buff->size < buff->pos + size)
		buff_resize(buff, buff->pos + size);

	if (buff->used - buff->pos < size) {
		char *data_used = buff->data + buff->used;
		size_t to_read = buff->pos + size - buff->used;
		size_t to_read_ahead = buff->size - buff->used;
		ssize_t nread = coio_read_ahead(&port->io, data_used,
					        to_read, to_read_ahead);
		if (nread == 0) {
			port->status = PG_EOF;
			return NULL;
		} else if (nread == -1) {
			if (! fiber_is_cancelled())
				/* Cancelled fiber is not an error. It
				 * happens when the server stops. */
				pg_error(NULL, ERRCODE_INTERNAL_ERROR,
					 "couldn't read data from "
					 "client \'%s\': %s",
				 	 port->user ? port->user : "",
					 box_error_message(box_error_last()));
			/* In case of fiber_is_cancelled() it is not really an
			 * error but we want to stop this fiber. */
			port->status = PG_ERR;
			return NULL;
		}
		buff->used += nread;
	}

	char *ret = buff->data + buff->pos;
	buff->pos += size;
	assert(buff_verify(buff) && "pg_read has broken the buff");
	return (void *)ret;
}

int
pg_read_uint8(struct pg_port *port, uint8_t *result)
{
	uint8_t *data = pg_read_bytes(port, sizeof(*data));
	if (data != NULL)
		*result = *data;
	return port->status;
}

int
pg_read_uint16(struct pg_port *port, uint16_t *result)
{
	uint16_t *data = pg_read_bytes(port, sizeof(*data));
	if (data != NULL)
		*result = ntohs(*data);
	return port->status;
}

int
pg_read_uint32(struct pg_port *port, uint32_t *result)
{
	uint32_t *data = pg_read_bytes(port, sizeof(*data));
	if (data != NULL)
		*result = ntohl(*data);
	return port->status;
}

void
pg_read_gc(struct pg_port *port)
{
	struct pg_port_buff *buff = &port->read_buff;
	assert(buff_verify(buff));
	buff_gc(buff);
}

void
pg_write_bytes(struct pg_port *port, const void *bytes, size_t len)
{
	struct pg_port_buff *buff = &port->write_buff;
	assert(buff_verify(buff));
	if (buff->size - buff->used < len)
		buff_resize(buff, buff->used + len);

	memcpy(buff->data + buff->used, bytes, len);
	buff->used += len;
	assert(buff_verify(buff) && "pg_write_bytes has broken the buff");
}

void
pg_begin_msg(struct pg_port *port, uint8_t type)
{
	struct pg_port_buff *buff = &port->write_buff;
	assert(buff_verify(buff));
	/** Type byte + size filed */
	uint8_t msg_header[1 + 4];
	/**
	 * We can't set the size at the moment, but we need to have room for it
	 * so we can set it later.
	 */
	msg_header[0] = type;
	pg_write_bytes(port, msg_header, sizeof(msg_header));
}

void
pg_end_msg(struct pg_port *port)
{
	struct pg_port_buff *buff = &port->write_buff;
	assert(buff_verify(buff));
	/** Exclude the type byte. */
	uint32_t net_size = htonl(buff->used - buff->pos - 1);
	/** Now we can set the size that comes right after the type. */
	memcpy(buff->data + buff->pos + 1, &net_size, sizeof(net_size));
	/** Message is completed, advance current position. */
	buff->pos = buff->used;
	if (buff->used >= WRITE_BUFF_FLUSH_IS_NEEDED_PAYLOAD)
		pg_flush(port);
}

void
pg_write_uint8(struct pg_port *port, uint8_t value)
{
	pg_write_bytes(port, &value, sizeof(value));
}

void
pg_write_uint16(struct pg_port *port, uint16_t value)
{
	value = htons(value);
	pg_write_bytes(port, &value, sizeof(value));
}

void
pg_write_uint32(struct pg_port *port, uint32_t value)
{
	value = htonl(value);
	pg_write_bytes(port, &value, sizeof(value));
}

void
pg_write_str(struct pg_port *port, const char *format, ...)
{
	va_list args;
	va_start(args, format);
	pg_write_str_va(port, format, args);
	va_end(args);
}

void
pg_write_str_va(struct pg_port *port, const char *fmt, va_list args)
{
	assert(fmt != NULL && "avoid UB");
	struct pg_port_buff *buff = &port->write_buff;
	assert(buff_verify(buff));

	va_list args_copy;
	va_copy(args_copy, args);
	int to_write = vsnprintf(NULL, 0, fmt, args_copy);
	va_end(args_copy);
	/*
	 * According to the ISO standards of C99 and above vsprintf may return
	 * a negative value ** only ** if an encoding error occurred, which does
	 * not apply to our case.
	 */
	assert(to_write >= 0 && "vsprintf failed, this should never happen");

	if (buff->size - buff->used < (uint32_t)to_write)
		/* Plus 1 for null-termination. */
		buff_resize(buff, buff->used + to_write + 1);

	int written = vsprintf(buff->data + buff->used, fmt, args);
	assert(written >= 0 && "vsprintf failed, this should never happen");
	assert(buff_verify(buff) && "pg_write_str_va has broken the buff");
	/** Plus one to include a terminating byte. */
	buff->used += written + 1;
}

void
pg_flush(struct pg_port *port)
{
	struct pg_port_buff *buff = &port->write_buff;
	assert(buff_verify(buff));
	if (buff->pos != 0)
		coio_write(&port->io, buff->data, buff->pos);
	buff_gc(buff);
}

void
pg_write_len_str(struct pg_port *port, const char *fmt, ...)
{
	assert(fmt != NULL && "avoid UB");
	struct pg_port_buff *buff = &port->write_buff;
	assert(buff_verify(buff));

	va_list args;
	va_start(args, fmt);
	int to_write = vsnprintf(NULL, 0, fmt, args);
	va_end(args);
	/** write len field */
	pg_write_uint32(port, to_write);
	/*
	 * According to the ISO standards of C99 and above vsprintf may return
	 * a negative value ** only ** if an encoding error occurred, which does
	 * not apply to our case.
	 */
	assert(to_write >= 0 && "vsprintf failed, this should never happen");

	if (buff->size - buff->used < (uint32_t)to_write)
		buff_resize(buff, buff->used + to_write);

	va_start(args, fmt);
	int written = vsprintf(buff->data + buff->used, fmt, args);
	va_end(args);
	assert(written == to_write);
	assert(written >= 0 && "vsprintf failed, this should never happen");
	assert(buff_verify(buff) && "pg_write_str_va has broken the buff");
	buff->used += written;
}
