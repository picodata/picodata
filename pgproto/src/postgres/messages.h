#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

struct pg_port;
struct row_description;

/**
 * Send ReadyForQuery message.
 * ReadyForQuery informs the frontend that it can safely send a new command.
 */
void
send_ready_for_query(struct pg_port *port);

/**
 * Send CommandComplete message to the frontend.
 * CommandComplete informs the frontend that the sent query has been
 * completed successfully.
 */
void
send_command_complete(struct pg_port *port, const char *tag,
		      bool display_row_count, size_t row_count);

/**
 * Send a RowDescription message that
 * describes the format of subsequent RowData messages.
 */
void
send_row_description_message(struct pg_port *port,
			     const struct row_description *row_desc);

/**
 * Read a msgpuck data row and sent it to the frontend according
 * to the row description.
 */
void
send_data_row(struct pg_port *port, const char **data,
	      const struct row_description *row_desc,
	      uint16_t format);

/**
 * Read all the msgpuck data rows and sent them to the frontend according
 * to the row description.
 */
uint32_t
send_data_rows(struct pg_port *port, const char **data,
	       const struct row_description *row_desc);

/**
 * Send ParameterStatus message to the frontend.
 */
void
send_parameter_status(struct pg_port *port,
		      const char *name,
		      const char *value);
