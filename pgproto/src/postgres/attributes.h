#pragma once

#include <stdint.h>

enum {
	TEXT_FORMAT = 0,
	BINARY_FORMAT = 1,
};

struct pg_port;

/** Definition of an attribute. */
struct pg_attribute {
	/** Column name. */
	const char *name;
	/** Len of a name. */
	uint32_t name_len;
	/** OID that defines the data type of this attribute. */
	uint32_t type_oid;
	/** Len of the data type. */
	uint16_t type_len;
	/**
	 * typmod records type-specific data supplied at table creation time
	 * (for example, the max length of a varchar field).  The
	 * value will generally be -1 for types that do not need typmod.
	 */
	uint32_t typemod;
	/** Format is used to send the attribute. */
	uint16_t format;

	/**
	 * Decode an attribute from msgpuck and write it to the port.
	 * It is used for forming a DataRow message.
	 */
	void (*write)(const struct pg_attribute *this,
		      struct pg_port *port, const char **data);
};

enum {
	TYPEMOD_DEFAULT = -1,
};

/** Initialize attribute for INT8 type. */
void
pg_attribute_int8(struct pg_attribute *att,
		  const char *name, uint32_t name_len,
		  uint16_t format, uint32_t typemod);

/** Initialize attribute for TEXT type. */
void
pg_attribute_text(struct pg_attribute *att, const char *name, uint32_t name_len,
		  uint16_t format, uint32_t typemod);

/** Initialize attribute for BOOL type. */
void
pg_attribute_bool(struct pg_attribute *att,
		  const char *name, uint32_t name_len,
		  uint16_t format, uint32_t typemod);

/** Initialize attribute for FLOAT8 type. */
void
pg_attribute_float8(struct pg_attribute *att,
		    const char *name, uint32_t name_len,
		    uint16_t format, uint32_t typemod);

/** Initialize attribute for UNKNOWN type. */
void
pg_attribute_unknown(struct pg_attribute *att,
		     const char *name, uint32_t name_len,
		     uint16_t format, uint32_t typemod);
