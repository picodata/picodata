#pragma once

struct pg_port;

/**
 * Authenticate the postgres client after processing a startup message.
 *
 * @retval 0 on success.
 * @retval -1 on error, error message is written to the server log and sent
 *         to the client.
 */
int
pg_authenticate(struct pg_port *port);
