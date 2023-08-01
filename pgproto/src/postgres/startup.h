#pragma once

struct pg_port;

/**
 * Process the pending startup message on the port.
 *
 * After connecting to the server, client sends a strtup message that defines
 * parameters such as user name and etc. that must be processed by this routine.
 *
 * @retval 0 on success.
 * @retval -1 on error, error message is written to the server log and sent
 * 	   to the client.
 */
int
pg_process_startup_message(struct pg_port *port);
