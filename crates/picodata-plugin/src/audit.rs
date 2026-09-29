//! Writing to the instance audit log.

use crate::internal::ffi::{pico_ffi_audit_is_enabled, pico_ffi_audit_record};
use abi_stable::std_types::{RSlice, RStr};

tarantool::define_str_enum! {
    /// Type-safe entry severity for use in [`crate::audit!`].
    /// Severity levels and their usage are defined in the RFC.
    pub enum Severity {
        Low = "low",
        Medium = "medium",
        High = "high",
    }
}

/// Write an event to picodata audit log.
///
/// Note that you likely do not want to use this directly. Instead, define your own plugin-specific
/// `audit!` macro with [`crate::define_audit!`], so that you wouldn't have to repeat yourself
/// specifying `subsystem: "plugin_name"` every time.
///
/// NOTE: arms starting with the `@` token are NOT part of the public interface.
///
/// # Parameters
///
/// - `subsystem` - subsystem which generated the event. For plugins, this should be the plugin name
/// - `message` - human-readable message describing what has happened. It is formatted using `format!(...)`
/// - `title` - machine-readable event name
/// - `severity` - one of `High` / `Medium` / `Low`
///
/// You can also add any amount of auxiliary fields, providing machine-readable context to the event.
/// Values for those fields should be coercible to a `&str`. If they re not, you may prepend them
/// with `%` or `?`, which will format them using `Display` or `Debug` correspondingly.
///
/// # Usage guidance
///
/// Only emit the event where the change becomes durable, not where it is proposed. For picodata,
/// this should be done in raft-apply path.
///
/// If you fail to follow this, and, for example, write the event on starting a compare-and-swap,
/// and then lose it, the audit log would contain a record of something that never happened.
///
/// # Example usage
///
/// ```no_run
/// let initiator = "shaver";
/// let name = 1;
///
/// picodata_plugin::audit! {
///     subsystem: "shavery",
///     message: "shaved yak `{name}`",
///     title: "shave_yak",
///     severity: High,
///     // auxiliary fields
///     name: %name,
///     initiator: initiator,
/// }
/// ```
///
/// It prints the following to the audit log:
///
/// ```json
/// {"id":"1.0.15","time":"2026-09-29T13:28:02.792+0300","message":"shaved yak `1`","subsystem":"shavery","title":"shave_yak","severity":"high","name":"1","initiator":"shaver"}
/// ```
#[macro_export]
macro_rules! audit(
    // Parse keys into a static slice and values into a local variable.
    // Both are converted to `FfiSafeStr`s, because we will pass them to picodata host via FFI.
    // Collecting them straight to ffi safe str at the call site allows us to skip conversion,
    // which would require allocating a vec to hold them.
    (@parse_kv { $keys_id:ident $values_ref:ident } { $($keys:tt)* } { $($value_stor:tt)* } { $($value_refs:tt)* }; {}) => {
        static $keys_id: &[$crate::macro_support::abi_stable::std_types::RStr<'static>] = &[$($keys)*];
        $($value_stor)*
        let $values_ref: &[$crate::macro_support::abi_stable::std_types::RStr<'_>] = &[
            $($value_refs)*
        ];
    };
    (@parse_kv { $($idents:ident)* } { $($keys:tt)* } { $($value_stor:tt)* } { $($value_refs:tt)* }; { $key:ident : ?$value:expr, $($rest:tt)* }) => {
        // debug-formatted value
        $crate::audit!(@parse_kv
            { $($idents)* }
            { $($keys)* $crate::macro_support::abi_stable::std_types::RStr::from_str(stringify!($key)), }
            { $($value_stor)* let _value = $crate::macro_support::smol_str::format_smolstr!("{:?}", $value); }
            { $($value_refs)* $crate::macro_support::abi_stable::std_types::RStr::from_str(_value.as_str()), }; { $($rest)* })
    };
    (@parse_kv { $($idents:ident)* } { $($keys:tt)* } { $($value_stor:tt)* } { $($value_refs:tt)* }; { $key:ident : %$value:expr, $($rest:tt)* }) => {
        // display-formatted value
        $crate::audit!(@parse_kv
            { $($idents)* }
            { $($keys)* $crate::macro_support::abi_stable::std_types::RStr::from_str(stringify!($key)), }
            { $($value_stor)* let _value = $crate::macro_support::smol_str::format_smolstr!("{}", $value); }
            { $($value_refs)* $crate::macro_support::abi_stable::std_types::RStr::from_str(_value.as_str()), }; { $($rest)* })
    };
    (@parse_kv { $($idents:ident)* } { $($keys:tt)* } { $($value_stor:tt)* } { $($value_refs:tt)* }; { $key:ident : $value:expr, $($rest:tt)* }) => {
        // value is a string already
        $crate::audit!(@parse_kv
            { $($idents)* }
            { $($keys)* $crate::macro_support::abi_stable::std_types::RStr::from_str(stringify!($key)), }
            { $($value_stor)* let ref _value = $value; }
            { $($value_refs)* $crate::macro_support::abi_stable::std_types::RStr::from_str(_value), }; { $($rest)* })
    };
    (@parse_kv { $($idents:ident)* } { $($kvs:tt)* }) => {
        $crate::audit!(@parse_kv { $($idents)* } { } { } { }; { $($kvs)* })
    };

    (@with_functions
        // These parameters allow picodata to provide its own functions to skip calling the FFI functions.
        // This is desirable because FFI functions go through GOT, adding overhead.
        {
            $is_enabled:path,
            $record_impl:path
        }
        {
            subsystem: $subsystem:expr,
            message: $message:expr,
            title: $title:expr,
            severity: $severity:ident,
            $($aux_fields:tt)*
        }
    ) => {
         if $is_enabled() {
            $crate::audit!(
                @parse_kv { KEYS values } {
                    subsystem: $subsystem,
                    title: $title,
                    severity: $crate::audit::Severity::$severity.as_str(),
                    $($aux_fields)*
                }
            );

            let message = format!($message);

            // SAFETY:
            // - KEYS are always sound to dereference, since they are `'static`
            // - values are sound to dereference for the duration of function execution
            unsafe { $record_impl(&message, KEYS, values) };
        }
    };

    (
        subsystem: $subsystem:expr,
        message: $message:expr,
        title: $title:expr,
        severity: $severity:ident,
        $($aux_fields:tt)*
    ) => {
        $crate::audit!(
            @with_functions {
                $crate::audit::is_enabled,
                $crate::audit::record_impl
            }
            {
                subsystem: $subsystem,
                message: $message,
                title: $title,
                severity: $severity,
                $($aux_fields)*
            }
        );
    };
);

/// Define an `audit!` macro that will emit audit events attributed to the specified `subsystem`
///
/// # Example usage
///
/// ```no_run
/// use picodata_plugin::define_audit;
///
/// // defines `audit!` macro
/// define_audit!("shavery");
///
/// let initiator = "shaver";
/// let name = 1;
///
/// audit! {
///     message: "shaved yak `{name}`",
///     title: "shave_yak",
///     severity: High,
///     name: %name,
///     initiator: initiator,
/// }
///
/// // The above is equivalent to:
/// picodata_plugin::audit! {
///     subsystem: "shavery",
///     message: "shaved yak `{name}`",
///     title: "shave_yak",
///     severity: High,
///     name: %name,
///     initiator: initiator,
/// }
/// ```
#[macro_export]
macro_rules! define_audit {
    // $dollar is a hack to escape $ until $$ is stabilized: https://github.com/rust-lang/rust/issues/83527
    ($subsystem:expr, $dollar:tt) => {
        #[doc = "[`picodata_plugin::audit!`] macro, specialized for subsystem `"]
        #[doc = stringify!($subsystem)]
        #[doc = "`.\n\nSee [`picodata_plugin::audit!`] and [`picodata_plugin::define_audit!`] for more information.\n"]
        macro_rules! audit {
            (
                message: $dollar message:expr,
                title: $dollar title:expr,
                severity: $dollar severity:ident,
                $dollar ($dollar aux_fields:tt)*
            ) => {
                $crate::audit! {
                    subsystem: $subsystem,
                    message: $dollar message,
                    title: $dollar title,
                    severity: $dollar severity,
                    $dollar ($dollar aux_fields)*
                }
            };
        }
    };
    ($subsystem:expr) => {
        $crate::define_audit!($subsystem, $);
    };
}

/// Check if picodata host has audit log enabled.
pub fn is_enabled() -> bool {
    unsafe { pico_ffi_audit_is_enabled() != 0 }
}

/// An implementation detail of [`crate::audit!`], which hands off the record to picodata via FFI.
#[doc(hidden)]
pub fn record_impl(message: &str, keys: &[RStr<'_>], values: &[RStr<'_>]) -> bool {
    let message = RStr::from_str(message);
    let keys = RSlice::from_slice(keys);
    let values = RSlice::from_slice(values);
    unsafe { pico_ffi_audit_record(message, keys, values) != 0 }
}
