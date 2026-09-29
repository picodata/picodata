//! Serialization logic for audit entries

use crate::traft::LogicalClockInstant;
use abi_stable::std_types::RStr;
use serde::ser::SerializeMap as _;

/// Information that is always added by the audit logger
struct AuditEntryMeta<'a> {
    id: LogicalClockInstant,
    time: chrono::DateTime<chrono::Local>,
    message: &'a str,
}

/// KVs that are provided by the `record_impl` caller
struct AuditUserKV<'a> {
    keys: &'a [RStr<'a>],
    values: &'a [RStr<'a>],
}

/// Full audit log entry, just before serialization to JSON.
pub struct AuditEntry<'a> {
    meta: AuditEntryMeta<'a>,
    user: AuditUserKV<'a>,
}

impl<'a> AuditEntry<'a> {
    pub fn new(
        id: LogicalClockInstant,
        time: chrono::DateTime<chrono::Local>,
        message: &'a str,
        keys: &'a [RStr<'a>],
        values: &'a [RStr<'a>],
    ) -> Self {
        Self {
            meta: AuditEntryMeta { id, time, message },
            user: AuditUserKV { keys, values },
        }
    }
}

impl serde::Serialize for AuditEntry<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        let mut map = serializer.serialize_map(None)?;

        // first write the meta KVs
        map.serialize_entry("id", &self.meta.id.to_string())?;
        map.serialize_entry(
            "time",
            &self.meta.time.format("%FT%H:%M:%S%.3f%z").to_string(),
        )?;
        map.serialize_entry("message", &self.meta.message)?;

        // now write the user KVs
        for (key, value) in std::iter::zip(self.user.keys, self.user.values) {
            let key = key.as_str();
            let value = value.as_str();

            if matches!(key, "id" | "time" | "message") {
                // don't let user-provided KVs accidentally overwrite the meta information.
                continue;
            }

            map.serialize_entry(key, value)?;
        }

        map.end()
    }
}
