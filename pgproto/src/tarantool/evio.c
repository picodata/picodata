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

#include "evio.h"
#include "sio.h"
#include <stdio.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

static int
evio_setsockopt_keepalive(int fd)
{
	int on = 1;
	/*
	 * SO_KEEPALIVE to ensure connections don't hang
	 * around for too long when a link goes away.
	 */
	if (sio_setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE,
		       &on, sizeof(on)))
		return -1;
#ifdef __linux__
	/*
	 * On Linux, we are able to fine-tune keepalive
	 * intervals. Set smaller defaults, since the system-wide
	 * defaults are in days.
	 */
	int keepcnt = 5;
	if (sio_setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &keepcnt,
		       sizeof(int)))
		return -1;
	int keepidle = 30;

	if (sio_setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &keepidle,
		       sizeof(int)))
		return -1;

	int keepintvl = 60;
	if (sio_setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &keepintvl,
		       sizeof(int)))
		return -1;
#endif
	return 0;
}

/** Set common client socket options. */
int
evio_setsockopt_client(int fd, int family, int type)
{
	int on = 1;
	/* In case this throws, the socket is not leaked. */
	if (sio_setfl(fd, O_NONBLOCK, on))
		return -1;
	if (type == SOCK_STREAM && family != AF_UNIX) {
		/*
		 * SO_KEEPALIVE to ensure connections don't hang
		 * around for too long when a link goes away.
		 */
		if (evio_setsockopt_keepalive(fd) != 0)
			return -1;
		/*
		 * Lower latency is more important than higher
		 * bandwidth, and we usually write entire
		 * request/response in a single syscall.
		 */
		if (sio_setsockopt(fd, IPPROTO_TCP, TCP_NODELAY,
				   &on, sizeof(on)))
			return -1;
	}
	return 0;
}

int
evio_setsockopt_server(int fd, int family, int type)
{
	int on = 1;
	/* In case this throws, the socket is not leaked. */
	if (sio_setfl(fd, O_NONBLOCK, on))
		return -1;
	/* Allow reuse local addresses. */
	if (sio_setsockopt(fd, SOL_SOCKET, SO_REUSEADDR,
		       &on, sizeof(on)))
		return -1;

#ifndef TARANTOOL_WSL1_WORKAROUND_ENABLED
	/* Send all buffered messages on socket before take
	 * control out from close(2) or shutdown(2). */
	struct linger linger = { 0, 0 };

	if (sio_setsockopt(fd, SOL_SOCKET, SO_LINGER,
		       &linger, sizeof(linger)))
		return -1;
#endif
	if (type == SOCK_STREAM && family != AF_UNIX &&
	    evio_setsockopt_keepalive(fd) != 0)
		return -1;
	return 0;
}
