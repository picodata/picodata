/*
This file is used by picodata's build.rs to generate bindings to tarantool's VDBE subsystem.
The generated bindings are included by ffi.rs that is stored alongside this file.
*/

#if defined(__GNUC__) && !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#endif

#include "bind.h"
#include "sql/sqlInt.h"
#include "sql/vdbe.h"
#include "sql/vdbeInt.h"
