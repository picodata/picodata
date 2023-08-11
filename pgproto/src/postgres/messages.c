#include <msgpuck.h>

#include "messages.h"
#include "port.h"
#include "attributes.h"

void
send_ready_for_query(struct pg_port *port)
{
	pg_begin_msg(port, 'Z');
	/** 'I' - not in a transaction block */
	pg_write_byte(port, 'I');
	pg_end_msg(port);
	/** Notify the client that we are ready for queries. */
	pg_flush(port);
}

void
send_command_complete(struct pg_port *port, const char *tag,
		      bool display_row_count, size_t row_count)
{
	/** @note: see BuildQueryCompletionString
	 * 	   from src/backendtcop.cmdtag.c */

	pg_begin_msg(port, 'C');
	pg_write_bytes(port, tag, strlen(tag));
	/** Inserts are special. */
	if (strncasecmp(tag, "INSERT", 6) == 0)
		pg_write_bytes(port, " 0", 2);
	if (display_row_count)
		pg_write_str(port, " %zu", row_count);
	else
		pg_write_str(port, "\0");
	pg_end_msg(port);
}

void
send_row_description_message(struct pg_port *port,
			     const struct row_description *row_desc)
{
	pg_begin_msg(port, 'T');
	pg_write_uint16(port, row_desc->natts);
	/**
	 * ** From postgres sources **
	 * resorigtbl/resorigcol identify the source of the column, if it is a
 	 * simple reference to a column of a base table (or view).  If it is not
 	 * a simple reference, these fields are zeroes.
	 */
	uint32_t resorigtbl = 0;
	uint32_t resorigcol = 0;
	const struct pg_attribute *atts = row_desc->atts;
	for (uint16_t i = 0; i < row_desc->natts; ++i) {
		pg_write_str(port, "%.*s", atts[i].name_len, atts[i].name);
		pg_write_uint32(port, resorigtbl);
		pg_write_uint16(port, resorigcol);
		pg_write_uint32(port, atts[i].type_oid);
		pg_write_uint16(port, atts[i].type_len);
		pg_write_uint32(port, atts[i].typemod);
		pg_write_uint16(port, atts[i].format);
	}
	pg_end_msg(port);
}

void
send_data_row(struct pg_port *port, const char **data,
	      const struct row_description *row_desc,
	      uint16_t format)
{
	(void)format;
	pg_begin_msg(port, 'D');
	pg_write_uint16(port, row_desc->natts);
	const struct pg_attribute *atts = row_desc->atts;
	/**
	 * All queries except explain return rows as arrays,
	 * explain returns strings, so there is no need for decoding.
	 */
	if (mp_typeof(**data) == MP_ARRAY) {
		uint32_t row_size = mp_decode_array(data);
		assert(row_size == row_desc->natts);
	} else {
		assert(mp_typeof(**data) == MP_STR);
	}
	for (uint16_t i = 0; i < row_desc->natts; ++i)
		atts[i].write(&atts[i], port, data);
	pg_end_msg(port);
}

uint32_t
send_data_rows(struct pg_port *port, const char **data,
	       const struct row_description *row_desc)
{
	assert(mp_typeof(**data) == MP_ARRAY);
	uint32_t row_count = mp_decode_array(data);
	for (uint32_t i = 0; i < row_count; ++i)
		send_data_row(port, data, row_desc, row_desc->atts[i].format);
	return row_count;
}

void
send_parameter_status(struct pg_port *port,
		      const char *name,
		      const char *value)
{
	pg_begin_msg(port, 'S');
	pg_write_str(port, name);
	pg_write_str(port, value);
	pg_end_msg(port);
}
