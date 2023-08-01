#pragma once
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
/**
 * A thin wrapper around BSD sockets. Sets the diagnostics
 * area with a nicely formatted message for most errors (some
 * intermittent errors such as EWOULDBLOCK, EINTR, EINPROGRESS,
 * EAGAIN are an exception to this). The API is following
 * suite of BSD socket API: most functinos -1 on error, 0 or a
 * valid file descriptor on success. Exceptions to this rule, once
 * again, are marked explicitly.
 */
#include <stdbool.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <fcntl.h>
#include <errno.h>

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

enum {
	/**
	 * - Unix socket path is 108 bytes max;
	 * - IP(v4, v6) max string len is 45;
	 *
	 * Max result is rounded up just in case the numbers are a bit different
	 * on various platforms.
	 */
	SERVICE_NAME_MAXLEN = 200,
};

/** Get socket flags. */
int sio_getfl(int fd);

/** Set socket flags. */
int sio_setfl(int fd, int flag, int on);

/**
 * Check if an errno, returned from a sio function, means a
 * non-critical error: EAGAIN, EWOULDBLOCK, EINTR.
 */
static inline bool
sio_wouldblock(int err)
{
	return err == EAGAIN || err == EWOULDBLOCK || err == EINTR;
}

/** Create a TCP or AF_UNIX socket. */
int
sio_socket(int domain, int type, int protocol);

/** Set an option on a socket. */
int
sio_setsockopt(int fd, int level, int optname,
	       const void *optval, socklen_t optlen);

/** Get a socket option value. */
int
sio_getsockopt(int fd, int level, int optname,
	       void *optval, socklen_t *optlen);

/**
 * Bind a socket to the given address.
 */
int
sio_bind(int fd, const struct sockaddr *addr, socklen_t addrlen);

/**
 * Mark a socket as accepting connections.
 */
int
sio_listen(int fd);

/**
 * Safely print a socket description to the given buffer, with correct overflow
 * checks and all.
 */
int
sio_socketname_to_buffer(int fd, char *buf, int size);

/**
 * Accept a client connection on a server socket. The
 * diagnostics is not set for inprogress errors (@sa
 * sio_wouldblock())
 */
int sio_accept(int fd, struct sockaddr *addr, socklen_t *addrlen);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */
