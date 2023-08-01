#pragma once

struct iostream;

/**
 * Start postgres protocol backend message flow. The whole session, beginning
 * with startup message exchange and ending with termination, is done by a
 * single call of this routine.
 *
 * @param iostream connection to the postgres frontend, it will be moved so
 *                 it should not be used after the call.
 *
 * @retval 0 on success.
 * @retval -1 on error, error message is written to the server log and sent
 *         to the client.
 */
int
postgres_main(struct iostream *iostream);
