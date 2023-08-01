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

#include <stdlib.h>
#include <stdio.h>
#include <limits.h>

#include <module.h>

#ifndef IOV_MAX
#define IOV_MAX 1024
#endif

/**
 * Helper macro to handle easily snprintf() result
 */
#define SNPRINT(_total, _fun, _buf, _size, ...) do {				\
	int written =_fun(_buf, _size, ##__VA_ARGS__);				\
	if (written < 0)							\
		return -1;							\
	_total += written;							\
	if (written < _size) {							\
		_buf += written, _size -= written;				\
	} else {								\
		_buf = NULL, _size = 0;						\
	}									\
} while(0)

#define nelem(x)     (sizeof((x))/sizeof((x)[0]))
#define field_sizeof(compound_type, field) sizeof(((compound_type *)NULL)->field)
#ifndef lengthof
#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))
#endif

/**
 * An x* variant of a memory allocation function calls the original function
 * and panics if it fails (i.e. it should never return NULL).
 */
#define xalloc_impl(size, func, args...)					\
	({									\
		void *ret = func(args);						\
		if (unlikely(ret == NULL)) {					\
			fprintf(stderr, "Can't allocate %zu bytes at %s:%d",	\
				(size_t)(size), __FILE__, __LINE__);		\
			exit(EXIT_FAILURE);					\
		}								\
		ret;								\
	})

#define xmalloc(size)		xalloc_impl((size), malloc, (size))
#define xcalloc(n, size)	xalloc_impl((n) * (size), calloc, (n), (size))
#define xrealloc(ptr, size)	xalloc_impl((size), realloc, (ptr), (size))
#define xstrdup(s)		xalloc_impl(strlen((s)) + 1, strdup, (s))
#define xstrndup(s, n)		xalloc_impl((n) + 1, strndup, (s), (n))

#ifndef NDEBUG
#define TRASH(ptr) memset(ptr, '#', sizeof(*ptr))
#else
#define TRASH(ptr) (void) (ptr)
#endif

/**
 * If control flow reaches the point of the unreachable(), the program is
 * undefined. It is useful in situations where the compiler cannot deduce
 * the unreachability of the code.
 */
#if __has_builtin(__builtin_unreachable) || defined(__GNUC__)
#  define unreachable() (assert(0), __builtin_unreachable())
#else
#  define unreachable() (assert(0))
#endif
