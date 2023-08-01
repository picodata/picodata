
#include <stdarg.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <sys/queue.h>

#include <module.h>
#include "msgpuck.h"
#include "tarantool/sio.h"
#include "tarantool/diag.h"
#include "tarantool/evio.h"
#include "tarantool/trivia/util.h"
#include "postgres/postgres.h"

enum {
	SERVER_TIMEOUT_INFINITY = 3600 * 24 * 365 * 10
};

/** Entry of a doubly linked list containing fibers */
struct fiber_list_entry {
	struct fiber *fiber;
	LIST_ENTRY(fiber_list_entry) entries;
};

/** Head of a doubly linked list containing fibers. */
LIST_HEAD(fiber_list, fiber_list_entry);

/** Insert entry to the head of the list. Takes o(1) time */
static void
fiber_list_insert_head(struct fiber_list *list, struct fiber_list_entry *entry)
{
	LIST_INSERT_HEAD(list, entry, entries);
}

/** Remove entry from the list. Takes o(1) time */
static void
fiber_list_remove(struct fiber_list_entry *entry)
{
	LIST_REMOVE(entry, entries);
}

/** Initialize fiber list. */
static void
fiber_list_init(struct fiber_list *list_head)
{
	LIST_INIT(list_head);
}

/** Get the first entry from the list. */
static struct fiber_list_entry *
fiber_list_first(struct fiber_list *list)
{
	return LIST_FIRST(list);
}

/** Check if the list has no entries. */
static bool
fiber_list_empty(struct fiber_list *list)
{
	return LIST_EMPTY(list);
}

/**
 * cord_on_yield is declared but not defined in the tarantool's core
 * so it must be defined by core users.
 */
void
cord_on_yield() {}

struct server {
	/** Server socket. */
	int socket;
	/** Fiber on which the accept loop runs. */
	struct fiber *fiber;

	/**
	 * Running clients.
	 * There is a need in stopping all the running clients before
	 * unmapping the library. Waking up a client running in the unmapped
	 * address space leads to SEGV_MAPPER.
	 * The list is manged by client_worker.
	 */
	struct fiber_list clients;
};

/** Server instance. */
static struct server server;

/** Create new server socket */
static int
server_socket_new(const char *host, const char *service)
{
	struct addrinfo hints;
	memset(&hints, 0, sizeof(struct addrinfo));
	/* Allow IPv4 or IPv6 */
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	/* Loop-back address if host is not specified */
	hints.ai_flags = AI_PASSIVE;
	struct addrinfo *ai;
	const double delay1s = 1.;
	int rc = coio_getaddrinfo(host, service, &hints, &ai, delay1s);
	if (rc != 0)
		return -1;

	for (; ai != NULL; ai = ai->ai_next) {
		struct sockaddr *addr = ai->ai_addr;
		const socklen_t addr_len = ai->ai_addrlen;

		int server_socket = sio_socket(addr->sa_family, SOCK_STREAM, 0);
		if (server_socket < 0)
			continue;

		if (evio_setsockopt_server(server_socket, addr->sa_family,
					   SOCK_STREAM) != 0)
			goto cleanup_and_try_again;

		if (sio_bind(server_socket, addr, addr_len) != 0)
			goto cleanup_and_try_again;

		if (sio_listen(server_socket) != 0)
			goto cleanup_and_try_again;

		return server_socket;

		cleanup_and_try_again:
			close(server_socket);
	}

	return diag_set(IllegalParams,
			"Can't create a server at the specified address: "
			"%s:%s", host, service);
}

/**
 * Client worker.
 * Takes 2 arguments:
 * 1st: int           client_socket
 * 2nd: struct fiber *client_fiber
 */
static int
client_worker(va_list args)
{
	int client_socket = va_arg(args, int);
	struct fiber *client_fiber = va_arg(args, struct fiber *);

	/**
	 * Insert the fiber to the list so that we can find it in a client
	 * list and cancel while stopping the server and unloading the library.
	 */
	struct fiber_list_entry *entry = xcalloc(1, sizeof(*entry));
	entry->fiber = client_fiber;
	fiber_list_insert_head(&server.clients, entry);

	struct iostream io;
	plain_iostream_create(&io, client_socket);
	postgres_main(&io);

	/** Fiber is finished so it can be removed from the list. */
	fiber_list_remove(entry);
	free(entry);
	return 0;
}

static int
server_start_client_worker(int client_socket)
{
	say_info("client[%d]: connected", client_socket);

	struct fiber *client_fiber = fiber_new("client", client_worker);
	if (client_fiber == NULL)
		return -1;

	fiber_set_joinable(client_fiber, true);
	fiber_start(client_fiber, client_socket, client_fiber);
	return 0;
}

static int
server_wouldblock(int err)
{
	return sio_wouldblock(err);
}

/**
 * Yield control to other fibers until there are no pending clients
 * in the backlog.
 * Return 1 if there are pending connections and 0 if waiting was interrupted.
 */
static int
server_wait_for_connection()
{
	int events = coio_wait(server.socket, COIO_READ,
			       SERVER_TIMEOUT_INFINITY);
	return (events & COIO_READ) != 0;
}

static int
server_accept_and_setopt()
{
	struct sockaddr_storage client_addr;
	socklen_t addr_len = sizeof(struct sockaddr_storage);
	struct sockaddr *client_addr_ptr = (struct sockaddr *)&client_addr;
	int client_socket = sio_accept(server.socket, client_addr_ptr,
				       &addr_len);

	if (client_socket < 0)
		return -1;

	if (evio_setsockopt_client(client_socket, client_addr.ss_family,
				   SOCK_STREAM) != 0) {
		close(client_socket);
		return -1;
	}

	return client_socket;
}

/**
 * Accept a pending client and run its worker.
 * If serving completed successfully 0 is returned, otherwise -1.
 */
static int
server_serve_connection()
{
	int client_socket = server_accept_and_setopt();
	if (client_socket >= 0) {
		if (server_start_client_worker(client_socket) == 0) {
			return 0;
		} else {
			close(client_socket);
			return -1;
		}
	}
	return -1;
}

#ifdef __linux__
/**
 * Check if the error is one of the network errors.
 */
static int
server_network_error(int err)
{
	return err == ENETDOWN   || err == EPROTO || err == ENOPROTOOPT  ||
	       err == EHOSTDOWN  || err == ENONET || err == EHOSTUNREACH ||
	       err == EOPNOTSUPP || err == ENETUNREACH;
}
#endif /* __linux__ */

/** Check whether the error should be treated as EAGAIN. */
static int
server_should_try_again(int err)
{
#ifdef __linux__
	/* Take a look at the Error handling section
	   in the accept's manual page. */
	return server_wouldblock(err) || server_network_error(err);
#else
	return server_wouldblock(err);
#endif
}

/** Server accept loop. */
static int
server_worker(va_list args)
{
	say_info("server has been started");
	while (! fiber_is_cancelled()) {
		if (server_wait_for_connection()) {
			if (server_serve_connection() != 0 &&
			    ! server_should_try_again(errno)) {
			    	say_info("server was stopped due to error: %s",
					 box_error_message(box_error_last()));
				return -1;
			}
		}
	}
	say_info("server was stopped");
	return 0;
}

static int
server_init(const char *host, const char *service)
{
	server.socket = server_socket_new(host, service);
	if (server.socket < 0)
		return -1;
	server.fiber = fiber_new("server", server_worker);
	if (server.fiber == NULL) {
		close(server.socket);
		return -1;
	}
	fiber_set_joinable(server.fiber, true);
	fiber_list_init(&server.clients);
	return 0;
}

static void
server_start_accept_loop()
{
	fiber_start(server.fiber);
}

int
server_start(box_function_ctx_t *ctx,
	    const char *args, const char *args_end)
{
	(void)ctx;
	(void)args_end;
	const char *usage = "server_start(host = <str>, service = <str>)";

	uint32_t args_count = mp_decode_array(&args);
	if (args_count != 2)
		goto illegal_params;

	if (mp_typeof(*args) != MP_STR)
		goto illegal_params;
	uint32_t host_len = 0;
	const char *host = mp_decode_str(&args, &host_len);

	if (mp_typeof(*args) != MP_STR)
		goto illegal_params;
	uint32_t service_len = 0;
	const char *service = mp_decode_str(&args, &service_len);

	/* make host and service null-terminated */
	host = xstrndup(host, host_len);
	service = xstrndup(service, service_len);
	int rc = server_init(host, service);
	free((void *)host);
	free((void *)service);
	if (rc != 0)
		return -1;

	server_start_accept_loop();
	return 0;

illegal_params:
	return diag_set(IllegalParams, "Usage: %s", usage);
}

static int
server_stop_accept_loop()
{
	fiber_cancel(server.fiber);
	fiber_wakeup(server.fiber);
	return fiber_join(server.fiber);
}

/**
 * Cancel all client fibers and wait for them to finish.
 * See the comment to server::clients for details.
 */
static void
server_terminate_clients()
{
	struct fiber_list_entry *entry;
	struct fiber *fiber;
	while(! fiber_list_empty(&server.clients)) {
		entry = fiber_list_first(&server.clients);
		fiber = entry->fiber;
		/* List entry is removed when fiber is finished */
		fiber_cancel(fiber);
		fiber_join(fiber);
	}
}

static int
server_free()
{
	int server_socket = server.socket;
	memset(&server, 0, sizeof(server));
	return coio_close(server_socket);
}

int
server_stop(box_function_ctx_t *ctx,
	    const char *args, const char *args_end)
{
	(void)ctx;
	(void)args_end;
	const char *usage = "server_stop()";
	uint32_t arg_count = mp_decode_array(&args);
	if (arg_count != 0)
		goto illegal_params;

	server_stop_accept_loop();
	server_terminate_clients();
	assert(fiber_list_empty(&server.clients));
	server_free();
	return 0;

illegal_params:
	return diag_set(IllegalParams, "Usage: %s", usage);
}
