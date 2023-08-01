/*
 * Copyright 2010-2016, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include <module.h>
#include "diag.h"
#include "trivia/util.h"
#include "sio.h"

#include <limits.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/uio.h>
#include <stdio.h>
#include <limits.h>
#include <netinet/in.h> /* TCP_NODELAY */
#include <netinet/tcp.h> /* TCP_NODELAY */
#include <arpa/inet.h>
#include <netdb.h>

#ifndef NI_MAXHOST
#define NI_MAXHOST 1025
#endif

/** Get a string representation of a socket option name,
 * for logging.
 */
static const char *
sio_option_name(int option)
{
#define CASE_OPTION(opt) case opt: return #opt
	switch (option) {
	CASE_OPTION(SO_KEEPALIVE);
	CASE_OPTION(SO_LINGER);
	CASE_OPTION(SO_ERROR);
	CASE_OPTION(SO_REUSEADDR);
	CASE_OPTION(TCP_NODELAY);
#ifdef __linux__
	CASE_OPTION(TCP_KEEPCNT);
	CASE_OPTION(TCP_KEEPINTVL);
#endif
	default:
		return "undefined";
	}
#undef CASE_OPTION
}

int
sio_setsockopt(int fd, int level, int optname,
	       const void *optval, socklen_t optlen)
{
	int rc = setsockopt(fd, level, optname, optval, optlen);
	if (rc) {
		diag_set_SocketError(fd, "setsockopt(%s)",
				     sio_option_name(optname));
	}
	return rc;
}

int
sio_getsockopt(int fd, int level, int optname,
	       void *optval, socklen_t *optlen)
{
	int rc = getsockopt(fd, level, optname, optval, optlen);
	if (rc) {
		diag_set_SocketError(fd, "getsockopt(%s)",
				     sio_option_name(optname));
	}
	return rc;
}

static int
sio_addr_snprintf(char *buf, size_t size, const struct sockaddr *addr,
		  socklen_t addrlen)
{
	int res;
	if (addr->sa_family == AF_UNIX) {
		struct sockaddr_un *u = (struct sockaddr_un *)addr;
		if (addrlen >= sizeof(*u))
			res = snprintf(buf, size, "unix/:%s", u->sun_path);
		else
			res = snprintf(buf, size, "unix/:(socket)");
	} else {
		char host[NI_MAXHOST], serv[NI_MAXSERV];
		int flags = NI_NUMERICHOST | NI_NUMERICSERV;
		if (getnameinfo(addr, addrlen, host, sizeof(host), serv,
				sizeof(serv), flags) != 0)
			res = snprintf(buf, size, "(host):(port)");
		else if (addr->sa_family == AF_INET)
			res = snprintf(buf, size, "%s:%s", host, serv);
		else
			res = snprintf(buf, size, "[%s]:%s", host, serv);
	}
	assert(res + 1 < SERVICE_NAME_MAXLEN);
	assert(res >= 0);
	return res;
}

/**
 * Safely print a socket description to the given buffer, with correct overflow
 * checks and all.
 */
int
sio_socketname_to_buffer(int fd, char *buf, int size)
{
	int n = 0;
	(void)n;
	SNPRINT(n, snprintf, buf, size, "fd %d", fd);
	if (fd < 0)
		return 0;
	struct sockaddr_storage addr;
	socklen_t addrlen = sizeof(addr);
	struct sockaddr *base_addr = (struct sockaddr *)&addr;
	int rc = getsockname(fd, base_addr, &addrlen);
	if (rc == 0) {
		SNPRINT(n, snprintf, buf, size, ", aka ");
		SNPRINT(n, sio_addr_snprintf, buf, size, base_addr, addrlen);
	}
	addrlen = sizeof(addr);
	rc = getpeername(fd, (struct sockaddr *) &addr, &addrlen);
	if (rc == 0) {
		SNPRINT(n, snprintf, buf, size, ", peer of ");
		SNPRINT(n, sio_addr_snprintf, buf, size, base_addr, addrlen);
	}
	return 0;
}

int
sio_getfl(int fd)
{
	int flags = fcntl(fd, F_GETFL, 0);
	if (flags < 0)
		diag_set_SocketError(fd, "fcntl(..., F_GETFL, ...)");
	return flags;
}

int
sio_setfl(int fd, int flag, int on)
{
	int flags = sio_getfl(fd);
	if (flags < 0)
		return flags;
	flags = fcntl(fd, F_SETFL, on ? flags | flag : flags & ~flag);
	if (flags < 0)
		diag_set_SocketError(fd, "fcntl(..., F_SETFL, ...)");
	return flags;
}

/** Try to automatically configure a listen backlog.
 * On Linux, use the system setting, which defaults
 * to 128. This way a system administrator can tune
 * the backlog as needed. On other systems, use SOMAXCONN.
 */
static int
sio_listen_backlog()
{
#ifdef __linux__
	FILE *proc = fopen("/proc/sys/net/core/somaxconn", "r");
	if (proc) {
		int backlog;
		int rc = fscanf(proc, "%d", &backlog);
		fclose(proc);
		if (rc == 1)
			return backlog;
	}
#endif /* __linux__ */
	return SOMAXCONN;
}

int
sio_socket(int domain, int type, int protocol)
{
	/* AF_UNIX can't use tcp protocol */
	if (domain == AF_UNIX)
		protocol = 0;
	int fd = socket(domain, type, protocol);
	if (fd < 0)
		diag_set_SocketError(fd, "socket");
	return fd;
}

int
sio_bind(int fd, const struct sockaddr *addr, socklen_t addrlen)
{
	int rc = bind(fd, addr, addrlen);
	if (rc < 0)
		diag_set_SocketError(fd, "bind");
	return rc;
}

int
sio_listen(int fd)
{
	int rc = listen(fd, sio_listen_backlog());
	if (rc < 0)
		diag_set_SocketError(fd, "listen");
	return rc;
}

int
sio_accept(int fd, struct sockaddr *addr, socklen_t *addrlen)
{
	/* Accept a connection. */
	int newfd = accept(fd, addr, addrlen);
	if (newfd < 0 && !sio_wouldblock(errno))
		diag_set_SocketError(fd, "accept");
	return newfd;
}
