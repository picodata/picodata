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

/**
 * Row description message.
 * Describes the format of subsequent RowData messages.
 */
struct row_description {
	/** Number of attributes. */
	uint32_t natts;
	/** Attribute descriptions. */
	struct pg_attribute *atts;
};

/**
 * Get a row description from the metadata.
 * Format is not mentioned in metadata so the caller must choose it him self.
 * After the call metadata is consumed and the data points to the begining of
 * the rows array.
 * Allocates on box region.
 */
int
parse_metadata(const char **data,
	       struct row_description *row_desc,
	       uint16_t format);

/**
 * Get a row description for an explain query.
 * Allocates on box region.
 */
void
row_description_explain(struct row_description *row_desc);
