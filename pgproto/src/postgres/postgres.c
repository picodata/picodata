#include <module.h>
#include <msgpuck.h>
#include <inttypes.h>
#include <strings.h>
#include <ctype.h>

#include "postgres.h"
#include "messages.h"
#include "report.h"
#include "port.h"
#include "startup.h"
#include "auth.h"
#include "attributes.h"
#include "tarantool/trivia/util.h"

/**
 * Get a command tag that should be sent in CommandComplete message.
 * It returns the exact command tag only if the tag must be sent with row count.
 */
static const char *
get_command_tag(const char *query, bool *display_row_count)
{
	/** skip leading spaces */
	while (isspace(*query) && *query)
		query++;

	/**
	 * tagname is only considered in the folowing cases
	 * and these are actually the only cases we need to send row count
	 */
	static const char *tags[] = {
		"SELECT", "DELETE", "UPDATE", "INSERT",
		"FETCH", "MERGE", "MOVE", "COPY",
	};
	for (size_t i = 0; i < lengthof(tags); ++i) {
		if (strncasecmp(query, tags[i], strlen(tags[i])) == 0) {
			*display_row_count = true;
			return tags[i];
		}
	}

	*display_row_count = false;
	return "DONE";
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

	/**
	 * Wait for the commit.
	 * @todo: fix me
	 */
	fiber_sleep(0.01);

	const char *response = NULL;
	uint32_t response_size;
	if (rc == 0)
		response = port_get_msgpack(&out, &response_size);
	port_destroy(&out);
	return response;
}

/**
 * Parse and send query response.
 * Returns -1 in case of error,
 */
static int64_t
process_query_response(struct pg_port *port, const char **response)
{
	size_t row_count = 0;
	const char **data = response;
	assert(mp_typeof(**data) == MP_ARRAY);
	uint32_t size = mp_decode_array(data);
	assert(size == 1);
	assert(mp_typeof(**data) == MP_ARRAY);
	size = mp_decode_array(data);
	if (mp_typeof(**data) == MP_ARRAY) {
		/** Explain query */
		struct row_description row_desc;
		row_description_explain(&row_desc);
		send_row_description_message(port, &row_desc);
		return send_data_rows(port, data, &row_desc);
	}
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

/**
 * Process a pending simple query message.
 * Allocates on box region.
 * Returns 0 if the query cycle can be continued, -1 otherwise.
 */
static int
process_simple_query_impl(struct pg_port *port)
{
	size_t query_len;
	const char *query = pg_read_cstr(port, &query_len);
	if (query == NULL) {
		pg_error(port, ERRCODE_INTERNAL_ERROR,
			 "failed to read a query message");
		/**
		 * We can't restore the message borders,
		 * so the cycle should be stopped.
		 */
		return -1;
	}

	say_debug("processing query \'%s\'", query);
	const char *response = dispatch_query_wrapped(query, query_len);

	if (response == NULL) {
		pg_error(port, ERRCODE_INTERNAL_ERROR,
			 "failed to execute query \'%s\': %s",
			 query, box_error_message(box_error_last()));
		/**
		 * The error was properly handled,
		 * so we can continue the query cycle.
		 */
		return 0;
	}

	bool display_row_count;
	const char *command_tag = get_command_tag(query, &display_row_count);
	int64_t row_count = process_query_response(port, &response);
	/** Send CommandComplete only if no error happened. */
	if (row_count != -1)
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
			say_error("unexpected EOF on client connection");
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
			say_debug("got Terminate message");
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

	if ((ret = pg_process_startup_message(&port)) != 0)
		goto cleanup;

	pg_set_fiber_name(port.user, strlen(port.user));

	if ((ret = pg_authenticate(&port)) != 0)
		goto cleanup;

	send_parameter_status(&port, "client_encoding", "UTF8");
	send_parameter_status(&port, "server_version", "15.0");

	if ((ret = start_query_cycle(&port)) != 0)
		goto cleanup;

cleanup:
	if (fiber_is_cancelled())
		pg_notice(&port, "shutting down");

	say_info("disconnected");
	pg_port_close(&port);
	return ret;
}
