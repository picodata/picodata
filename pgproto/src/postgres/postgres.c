#include <module.h>
#include <msgpuck.h>
#include <inttypes.h>
#include <strings.h>
#include <ctype.h>

#include "postgres.h"
#include "report.h"
#include "port.h"
#include "startup.h"
#include "auth.h"
#include "attributes.h"
#include "tarantool/trivia/util.h"

/**
 * Format of ReadyForQuery message.
 * ReadyForQuery informs the frontend that it can safely send a new command.
*/
struct ready_for_query_message {
	/** Type byte, equals 'Z' */
	uint8_t type;
	/** Packet length without the type byte, including len field itself. */
	uint32_t len;
	/**
	 * Current backend transaction status indicator.
	 * Possible values are 'I' if idle (not in a transaction block);
	 * 'T' if in a transaction block; or 'E' if in a failed transaction
	 * block (queries will be rejected until block is ended).
	 */
	uint8_t transaction_status;
};

/**
 * Send ReadyForQuery message.
 * ReadyForQuery informs the frontend that it can safely send a new command.
 */
static void
send_ready_for_query(struct pg_port *port)
{
	pg_begin_msg(port, 'Z');
	/** 'I' - not in a transaction block */
	pg_write_byte(port, 'I');
	pg_end_msg(port);
	/** Notify the client that we are ready for queries. */
	pg_flush(port);
}

/** Query message format. */
struct query_message {
	/** Type byte. Equals 'Q' */
	uint8_t type;
	/** Message length, including the len field. */
	uint32_t len;
	/** The query string. */
	const char *query;
};

#define COMPARE_AND_RETURN_IF_EQUALS(query, tag)	\
	if (strncasecmp(query, tag, strlen(tag)) == 0)	\
		return tag

/**
 * Get a command tag that should be sent in CommandComplete message.
 * It returns the exact command tag only if the tag must be sent with row count.
 */
static const char *
get_command_tag(const char *query, bool *display_row_count)
{
	/* skip leading spaces */
	while (isspace(*query) && *query)
		query++;

	/**
	 * tagname is only considered in the folowing cases
	 * and these are actually the only cases we need to send row count
	 */
	*display_row_count = true;
	COMPARE_AND_RETURN_IF_EQUALS(query, "SELECT");
	COMPARE_AND_RETURN_IF_EQUALS(query, "DELETE");
	COMPARE_AND_RETURN_IF_EQUALS(query, "UPDATE");
	COMPARE_AND_RETURN_IF_EQUALS(query, "INSERT");
	COMPARE_AND_RETURN_IF_EQUALS(query, "FETCH");
	COMPARE_AND_RETURN_IF_EQUALS(query, "MERGE");
	COMPARE_AND_RETURN_IF_EQUALS(query, "MOVE");
	COMPARE_AND_RETURN_IF_EQUALS(query, "COPY");

	*display_row_count = false;
	return "DONE";
}

#undef COMPARE_AND_RETURN_IF_EQUALS

/**
 * Send CommandComplete message to the frontend.
 * CommandComplete informs the frontend that the sent query has been
 * completed successfully.
 */
static void
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

/** Picodata's sql worker. */
extern int
dispatch_query(struct box_function_ctx *f_ctx,
	       const char *args, const char *args_end);

/**
 * Call stored C routine dispatch_query and
 * get the response in msgpuck format allocated on box region.
 */
static const char *
dispatch_query_wrapped(const char *query, size_t query_len)
{
	const char *tracer = "global";
	uint32_t arg_size = mp_sizeof_array(5) + mp_sizeof_str(query_len) +
			    3 * mp_sizeof_nil() +
			    mp_sizeof_str(strlen(tracer));

	char *args = box_region_alloc(arg_size);
	char *args_end = args;
	args_end = mp_encode_array(args_end, 5);
	args_end = mp_encode_str(args_end, query, query_len);
	args_end = mp_encode_nil(args_end);
	args_end = mp_encode_nil(args_end);
	args_end = mp_encode_nil(args_end);
	args_end = mp_encode_str(args_end, tracer, strlen(tracer));
	struct port out;
	port_c_create(&out);
	struct box_function_ctx ctx = { &out };
	int rc = dispatch_query(&ctx, args, args_end);
	if (rc != 0)
		return NULL;
	uint32_t response_size;
	const char *response = port_get_msgpack(&out, &response_size);
	port_destroy(&out);
	return response;
}

/**
 * Row description message.
 * Describes the format of subsequent RowData messages.
 */
struct row_description {
	/** Number of attributes. */
	uint32_t natts;
	/** Attribute descriptions. */
	struct pg_attribute *atts;
};

/**
 * Get row description from the metadata.
 * Format is not mentioned in metadata so the caller must choose it him self.
 */
static int
parse_metadata(const char **data,
	       struct row_description *row_desc, uint16_t format)
{
	uint32_t natts = mp_decode_array(data);
	if (natts >= (uint16_t)-1) {
		pg_debug("too many attributes: %"PRIu32, natts);
		return -1;
	}
	row_desc->natts = (uint16_t)natts;
	row_desc->atts = box_region_alloc(sizeof(*row_desc->atts) * natts);
	const char *str;
	uint32_t len;
	for (uint32_t i = 0; i < row_desc->natts; ++i) {
		assert(mp_typeof(**data) == MP_MAP);
		uint32_t map_size = mp_decode_map(data);
		assert(map_size == 2);
		str = mp_decode_str(data, &len);
		assert(len == 4 && strncmp(str, "name", 4) == 0);
		uint32_t name_len;
		const char *name = mp_decode_str(data, &name_len);
		str = mp_decode_str(data, &len);
		assert(len == 4 && strncmp(str, "type", 4) == 0);

		const char *type = mp_decode_str(data, &len);
		struct pg_attribute *att = &row_desc->atts[i];
		if (strncmp(type, "integer", len) == 0) {
			pg_attribute_int8(att, name, name_len, format,
					  TYPEMOD_DEFAULT);
		} else if (strncmp(type, "string", len) == 0) {
			pg_attribute_text(att, name, name_len, format,
					  TYPEMOD_DEFAULT);
		} else if (strncmp(type, "boolean", len) == 0) {
			pg_attribute_bool(att, name, name_len, format,
					  TYPEMOD_DEFAULT);
		} else if (strncmp(type, "double", len) == 0) {
			pg_attribute_float8(att, name, name_len, format,
					    TYPEMOD_DEFAULT);
		} else if (strncmp(type, "any", len) == 0) {
			pg_attribute_unknown(att, name, name_len, format,
					     TYPEMOD_DEFAULT);
		} else {
			/**
			 * Unsigned type is not supported by postgres.
			 * Decimal type is supported in picodata and can be
			 * matched to NUMERIC type in postgres but it is not
			 * trivial to work with it compared to the other types.
			 */
			pg_error(NULL, ERRCODE_INTERNAL_ERROR,
				 "unknown type \'%.*s\'", len, type);
			return -1;
		}
	}
	return 0;
}

/**
 * Send a RowDescription message that
 * describes the format of subsequent RowData messages.
 */
static void
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

/**
 * Read a msgpuck data row and sent it to the frontend according
 * to the row description.
 */
static void
send_data_row(struct pg_port *port, const char **data,
	      const struct row_description *row_desc,
	      uint16_t format)
{
	pg_begin_msg(port, 'D');
	pg_write_uint16(port, row_desc->natts);
	const struct pg_attribute *atts = row_desc->atts;
	assert(mp_typeof(**data) == MP_ARRAY);
	uint32_t row_size = mp_decode_array(data);
	assert(row_size == row_desc->natts);
	for (uint16_t i = 0; i < row_desc->natts; ++i)
		atts[i].write(&atts[i], port, data);
	pg_end_msg(port);
}

/**
 * Read all the msgpuck data rows and sent them to the frontend according
 * to the row description.
 */
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

static uint32_t
process_query_response(struct pg_port *port, const char **response)
{
	size_t row_count = 0;
	const char **data = response;
	assert(mp_typeof(**data) == MP_ARRAY);
	uint32_t size = mp_decode_array(data);
	assert(size == 1);
	assert(mp_typeof(**data) == MP_ARRAY);
	size = mp_decode_array(data);
	assert(mp_typeof(**data) == MP_MAP);
	size = mp_decode_map(data);
	uint32_t len;
	const char *str;
	if (size == 2) {
		/**
		 * { name:val, ... } - map, [a, b, ...] - array
		 *
		 * { "metadata": [{ "name":"col", "type":"integer" }, ... }],
		 *	"rows": [[row1], ... ] }
		 */
		assert(mp_typeof(**data) == MP_STR);
		str = mp_decode_str(data, &len);
		assert(strncmp(str, "metadata", strlen("metadata")) == 0);

		/** Simple query response is always in text format. */
		struct row_description row_desc;
		if (parse_metadata(data, &row_desc, TEXT_FORMAT) != 0) {
			pg_error(port, ERRCODE_INTERNAL_ERROR,
				 "can't parse attributes description");
			return -1;
		}
		send_row_description_message(port, &row_desc);
		assert(mp_typeof(**data) == MP_STR);
		str = mp_decode_str(data, &len);
		assert(strncmp(str, "rows", strlen("rows")) == 0);
		row_count = send_data_rows(port, data, &row_desc);
	} else if (size == 1) {
		/* { "row_count": n } */
		assert(mp_typeof(**data) == MP_STR);
		const char *str = mp_decode_str(data, &len);
		assert(strncmp(str, "row_count", strlen("row_count")) == 0);
		row_count = mp_decode_uint(data);
	}
	return row_count;
}

static int
process_simple_query_impl(struct pg_port *port)
{
	size_t query_len;
	const char *query = pg_read_cstr(port, &query_len);
	if (query == NULL) {
		pg_error(port, ERRCODE_INTERNAL_ERROR,
			 "failed to read a query message");
		return -1;
	}

	pg_debug("processing query \'%s\'", query);
	const char *response = dispatch_query_wrapped(query, query_len);

	if (response == NULL) {
		pg_error(port, ERRCODE_INTERNAL_ERROR,
			 "failed to execute query \'%s\': %s",
			 query, box_error_message(box_error_last()));
		/**
		 * Got client or memory error.
		 * The error code should be considered to decide what to return.
		 * In case of sql error 0 should be returned and pg_error should
		 * report the code, otherwise -1 should be returned.
		 * @todo: Handle it smarter.
		 */
		return 0;
	}

	bool display_row_count;
	const char *command_tag = get_command_tag(query, &display_row_count);
	size_t row_count = process_query_response(port, &response);
	send_command_complete(port, command_tag, display_row_count, row_count);
	return 0;
}

static int
process_simple_query(struct pg_port *port)
{
	size_t region_svp = box_region_used();
	int rc = process_simple_query_impl(port);
	box_region_truncate(region_svp);
	return rc;
}

static int
start_query_cycle(struct pg_port *port)
{
	while (true) {
		send_ready_for_query(port);

		uint8_t msg_type;
		pg_read_uint8(port, &msg_type);
		if (port->status == PG_EOF) {
			pg_error(NULL, ERRCODE_CONNECTION_DOES_NOT_EXIST,
				 "unexpected EOF on client connection");
			return -1;
		} else if (port->status == PG_ERR) {
			/* Error has already been logged. */
			return -1;
		}

		switch (msg_type) {
		case 'Q': /* Query */
			if (process_simple_query(port) != 0)
				return -1;
			break;
		case 'X': /* Terminate */
			pg_debug("got Terminate message");
			return 0;
		default:
			pg_error(port, ERRCODE_FEATURE_NOT_SUPPORTED,
				 "\'%c\' message type is not supported",
				 msg_type);
			return -1;
		}
		pg_read_gc(port);
	}
}

static void
pg_send_parameter_status(struct pg_port *port,
			 const char *name, const char *value)
{
	pg_begin_msg(port, 'S');
	pg_write_str(port, name);
	pg_write_str(port, value);
	pg_end_msg(port);
}

static void
pg_set_fiber_name(const char *name, size_t name_len) {
	const char prefix[] = "pg.";
	const size_t prefix_len = sizeof(prefix) - 1;
	const size_t pg_name_len = prefix_len + name_len;
	char *pg_name = xmalloc(prefix_len + name_len);
	memcpy(pg_name, prefix, prefix_len);
	memcpy(pg_name + prefix_len, name, name_len),
	fiber_set_name_n(fiber_self(), pg_name, pg_name_len);
	free(pg_name);
}

int
postgres_main(struct iostream *iostream)
{
	struct pg_port port;
	pg_port_create(&port, iostream);

	int ret = 0;
	if (pg_process_startup_message(&port) != 0) {
		ret = -1;
		goto close_connection;
	}

	pg_set_fiber_name(port.user, strlen(port.user));

	if (pg_authenticate(&port) != 0) {
		ret = -1;
		goto close_connection;
	}

	pg_send_parameter_status(&port, "client_encoding", "UTF8");

	if (start_query_cycle(&port)  != 0) {
		ret = -1;
		goto close_connection;
	}

close_connection:
	if (fiber_is_cancelled())
		pg_notice(&port,
			  "server is stopping and closing all connections");
	pg_info("disconnected");
	pg_port_close(&port);
	return ret;
}
