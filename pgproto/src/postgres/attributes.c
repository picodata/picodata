/**
 * PostgreSQL Database Management System
 * (formerly known as Postgres, then as Postgres95)
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 1994, The Regents of the University of California
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */
#include <msgpuck.h>
#include <inttypes.h>

#include "attributes.h"
#include "port.h"
#include "report.h"

enum {
	TYPEMOD_DEFAULT = -1,
};

static void
write_unknown(const struct pg_attribute *this,
	      struct pg_port *port, const char **data)
{
	uint16_t format = this->format;
	assert(format == TEXT_FORMAT || format == BINARY_FORMAT);
	int type = mp_typeof(**data);
	assert(type == MP_NIL);
	mp_decode_nil(data);
	if (format == TEXT_FORMAT) {
		pg_write_len_str(port, "NULL");
	} else {
		assert(false && "binary format is not supported");
	}
}

static void
pg_attribute_unknown(struct pg_attribute *att,
		     const char *name, uint32_t name_len,
		     uint16_t format, uint32_t typemod)
{
	att->name = name;
	att->name_len = name_len;
	att->type_oid = 705;
	att->type_len = -2;
	att->typemod = typemod;
	att->format = format;
	att->write = write_unknown;
}

static void
write_int8(const struct pg_attribute *this,
	   struct pg_port *port, const char **data)
{
	uint16_t format = this->format;
	if (mp_typeof(**data) == MP_NIL)
		return write_unknown(this, port, data);
	assert(format == TEXT_FORMAT || format == BINARY_FORMAT);
	int type = mp_typeof(**data);
	assert(type == MP_UINT || type == MP_INT);
	if (format == TEXT_FORMAT) {
		if (type == MP_INT) {
			int64_t val = mp_decode_int(data);
			pg_write_len_str(port, "%"PRId64, val);
		} else if (type == MP_UINT) {
			uint64_t val = mp_decode_uint(data);
			pg_write_len_str(port, "%"PRIu64, val);
		}
	} else {
		assert(false && "binary format is not supported");
	}
}

static void
pg_attribute_int8(struct pg_attribute *att,
		  const char *name, uint32_t name_len,
		  uint16_t format, uint32_t typemod)
{
	att->name = name;
	att->name_len = name_len;
	att->type_oid = 20;
	att->type_len = 8;
	att->typemod = typemod;
	att->format = format;
	att->write = write_int8;
}

static void
write_text(const struct pg_attribute *this,
	   struct pg_port *port, const char **data)
{
	uint16_t format = this->format;
	if (mp_typeof(**data) == MP_NIL)
		return write_unknown(this, port, data);
	assert(format == TEXT_FORMAT || format == BINARY_FORMAT);
	int type = mp_typeof(**data);
	assert(type == MP_STR);
	uint32_t len;
	const char *str = mp_decode_str(data, &len);
	if (format == TEXT_FORMAT) {
		pg_write_len_str(port, "%.*s", len, str);
	} else {
		assert(false && "binary format is not supported");
	}
}

static void
pg_attribute_text(struct pg_attribute *att, const char *name, uint32_t name_len,
		  uint16_t format, uint32_t typemod)
{
	att->name = name;
	att->name_len = name_len;
	att->type_oid = 25;
	att->type_len = -1;
	att->typemod = typemod;
	att->format = format;
	att->write = write_text;
}

static void
write_bool(const struct pg_attribute *this,
	   struct pg_port *port, const char **data)
{
	uint16_t format = this->format;
	if (mp_typeof(**data) == MP_NIL)
		return write_unknown(this, port, data);
	assert(format == TEXT_FORMAT || format == BINARY_FORMAT);
	int type = mp_typeof(**data);
	assert(type == MP_BOOL);
	bool val = mp_decode_bool(data);
	if (format == TEXT_FORMAT) {
		pg_write_len_str(port, val ? "t" : "f");
	} else {
		assert(false && "binary format is not supported");
	}
}

static void
pg_attribute_bool(struct pg_attribute *att,
		  const char *name, uint32_t name_len,
		  uint16_t format, uint32_t typemod)
{
	att->name = name;
	att->name_len = name_len;
	att->type_oid = 16;
	att->type_len = 1;
	att->typemod = typemod;
	att->format = format;
	att->write = write_bool;
}

static void
write_float8(const struct pg_attribute *this,
	     struct pg_port *port, const char **data)
{
	uint16_t format = this->format;
	if (mp_typeof(**data) == MP_NIL)
		return write_unknown(this, port, data);
	assert(format == TEXT_FORMAT || format == BINARY_FORMAT);
	int type = mp_typeof(**data);
	assert(type == MP_FLOAT || type == MP_DOUBLE);
	double val = type == MP_DOUBLE ? mp_decode_double(data) :
					 mp_decode_float(data);
	if (format == TEXT_FORMAT) {
		pg_write_len_str(port, "%lf" , val);
	} else {
		assert(false && "binary format is not supported");
	}
}

static void
pg_attribute_float8(struct pg_attribute *att,
		    const char *name, uint32_t name_len,
		    uint16_t format, uint32_t typemod)
{
	att->name = name;
	att->name_len = name_len;
	att->type_oid = 701;
	att->type_len = 8;
	att->typemod = typemod;
	att->format = format;
	att->write = write_float8;
}

/**
 * Get row description from the metadata.
 * Format is not mentioned in metadata so the caller must choose it him self.
 */
int
parse_metadata(const char **data,
	       struct row_description *row_desc,
	       uint16_t format)
{
	uint32_t natts = mp_decode_array(data);
	if (natts >= (uint16_t)-1) {
		pg_debug("too many attributes: %"PRIu32, natts);
		return -1;
	}
	row_desc->natts = (uint16_t)natts;
	row_desc->atts = box_region_alloc(sizeof(*row_desc->atts) * natts);
	const char *str;
	uint32_t len;
	for (uint32_t i = 0; i < row_desc->natts; ++i) {
		assert(mp_typeof(**data) == MP_MAP);
		uint32_t map_size = mp_decode_map(data);
		assert(map_size == 2);
		str = mp_decode_str(data, &len);
		assert(len == 4 && strncmp(str, "name", 4) == 0);
		uint32_t name_len;
		const char *name = mp_decode_str(data, &name_len);
		str = mp_decode_str(data, &len);
		assert(len == 4 && strncmp(str, "type", 4) == 0);

		const char *type = mp_decode_str(data, &len);
		struct pg_attribute *att = &row_desc->atts[i];
		if (strncmp(type, "integer", len) == 0) {
			pg_attribute_int8(att, name, name_len, format,
					  TYPEMOD_DEFAULT);
		} else if (strncmp(type, "string", len) == 0) {
			pg_attribute_text(att, name, name_len, format,
					  TYPEMOD_DEFAULT);
		} else if (strncmp(type, "boolean", len) == 0) {
			pg_attribute_bool(att, name, name_len, format,
					  TYPEMOD_DEFAULT);
		} else if (strncmp(type, "double", len) == 0) {
			pg_attribute_float8(att, name, name_len, format,
					    TYPEMOD_DEFAULT);
		} else if (strncmp(type, "any", len) == 0) {
			pg_attribute_unknown(att, name, name_len, format,
					     TYPEMOD_DEFAULT);
		} else {
			/**
			 * Unsigned type is not supported by postgres.
			 * Decimal type is supported in picodata and can be
			 * matched to NUMERIC type in postgres but it is not
			 * trivial to work with it compared to the other types.
			 */
			pg_error(NULL, ERRCODE_INTERNAL_ERROR,
				 "unknown type \'%.*s\'", len, type);
			return -1;
		}
	}
	return 0;
}
