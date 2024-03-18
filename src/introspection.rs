//! This module contains items intended to help with runtime introspection of
//! rust types. Currently it's mainly used for the [`PicodataConfig`] struct
//! to simplify the management of configuration parameters and relatetd stuff.
//!
//! The main functionality is implemented via the [`Introspection`] trait and
//! the corresponding derive macro. This trait currently facilitates the ability
//! to set and get fields of the struct via a path known at runtime. Also it
//! supports some basic struct field information.
//!
//! [`PicodataConfig`]: crate::config::PicodataConfig
use crate::traft::error::Error;
pub use pico_proc_macro::Introspection;

pub trait Introspection {
    /// Information about the struct fields of `Self`.
    const FIELD_INFOS: &'static [FieldInfo];

    /// Assign field of `self` described by `path` to value parsed from a given
    /// `yaml` expression.
    ///
    /// When using the `#[derive(Introspection)]` derive macro the implementation
    /// uses the `serde_yaml` to decode values from yaml. This may change in the
    /// future.
    ///
    /// # Examples:
    /// ```
    /// use picodata::introspection::Introspection;
    ///
    /// #[derive(Introspection, Default)]
    /// #[introspection(crate = picodata)]
    /// struct MyStruct {
    ///     number: i32,
    ///     text: String,
    ///     #[introspection(nested)]
    ///     nested: NestedStruct,
    /// }
    ///
    /// #[derive(Introspection, Default)]
    /// #[introspection(crate = picodata)]
    /// struct NestedStruct {
    ///     sub_field: f32,
    /// }
    ///
    /// let mut s = MyStruct::default();
    /// s.set_field_from_yaml("number", "420").unwrap();
    /// s.set_field_from_yaml("text", "hello world").unwrap();
    /// s.set_field_from_yaml("nested.sub_field", "3.14").unwrap();
    /// ```
    // TODO: maybe remove in favour of set_field_from_rmpv
    fn set_field_from_yaml(&mut self, path: &str, yaml: &str) -> Result<(), IntrospectionError>;

    /// Assign field of `self` described by `path` to value converted from the
    /// provided `value`.
    ///
    /// When using the `#[derive(Introspection)]` derive macro the implementation
    /// uses the `rmpv::ext::deserialize_from` to convert to the required type.
    /// This may change in the future.
    ///
    /// # Examples:
    /// ```
    /// use picodata::introspection::Introspection;
    /// use rmpv::Value;
    ///
    /// #[derive(Introspection, Default)]
    /// #[introspection(crate = picodata)]
    /// struct MyStruct {
    ///     number: i32,
    ///     text: String,
    ///     #[introspection(nested)]
    ///     nested: NestedStruct,
    /// }
    ///
    /// #[derive(Introspection, Default)]
    /// #[introspection(crate = picodata)]
    /// struct NestedStruct {
    ///     sub_field: f32,
    /// }
    ///
    /// let mut s = MyStruct::default();
    /// s.set_field_from_rmpv("number", &Value::from(420)).unwrap();
    /// s.set_field_from_rmpv("text", &Value::from("hello world")).unwrap();
    /// s.set_field_from_rmpv("nested.sub_field", &Value::F32(3.14)).unwrap();
    /// ```
    fn set_field_from_rmpv(
        &mut self,
        path: &str,
        value: &rmpv::Value,
    ) -> Result<(), IntrospectionError>;

    /// Get field of `self` described by `path` as a generic msgpack value in
    /// form of [`rmpv::Value`].
    ///
    /// When using the `#[derive(Introspection)]` derive macro the implementation
    /// converts the value to msgpack using [`to_rmpv_value`].
    ///
    /// In the future we may want to get values as some other enums (maybe
    /// serde_yaml::Value, or our custom one), but for now we've chosen rmpv
    /// because we're using this to convert values to msgpack.
    ///
    /// # Examples:
    /// ```
    /// use picodata::introspection::Introspection;
    /// use rmpv::Value;
    ///
    /// #[derive(Introspection, Default)]
    /// #[introspection(crate = picodata)]
    /// struct MyStruct {
    ///     number: i32,
    ///     text: String,
    ///     #[introspection(nested)]
    ///     nested: NestedStruct,
    /// }
    ///
    /// #[derive(Introspection, Default)]
    /// #[introspection(crate = picodata)]
    /// struct NestedStruct {
    ///     sub_field: f64,
    /// }
    ///
    /// let mut s = MyStruct {
    ///     number: 13,
    ///     text: "hello".into(),
    ///     nested: NestedStruct {
    ///         sub_field: 2.71,
    ///     },
    /// };
    ///
    /// assert_eq!(s.get_field_as_rmpv("number").unwrap(), Value::from(13));
    /// assert_eq!(s.get_field_as_rmpv("text").unwrap(), Value::from("hello"));
    /// assert_eq!(s.get_field_as_rmpv("nested.sub_field").unwrap(), Value::from(2.71));
    /// ```
    fn get_field_as_rmpv(&self, path: &str) -> Result<rmpv::Value, IntrospectionError>;

    /// Get a value which was specified via the `#[introspection(config_default = expr)]`
    /// attribute converted to a [`rmpv::Value`].
    ///
    /// Returns `Ok(None)` if `config_default` attribute wasn't provided for
    /// given field.
    ///
    /// Note that the value is not type checked, instead it is immediately
    /// converted to a rmpv::Value, so it's the user's responsibility to make
    /// sure the resulting value has the appropriate type. This however makes it
    /// a bit simpler to specify values for nested types like `Option<String>`,
    /// etc.
    ///
    /// The `expr` in `#[introspection(config_default = expr)]` may contain
    /// references to `self`, which allows for default values of some parameters
    /// to be dependent on values of other parameters
    /// (see [`InstanceConfig::advertise_address`] for example).
    ///
    /// Also note that this function doesn't do any special checks to see if the
    /// values are set or not in the actual struct. See what happens in
    /// [`PicodataConfig::set_defaults_explicitly`] for a real-life example.
    ///
    /// When using the `#[derive(Introspection)]` derive macro the implementation
    /// converts the value to msgpack using `rmp_serde` and then decodes that
    /// msgpack into a `rmpv::Value`. This is sub-optimal with respect to performance,
    /// but for our use cases this is fine.
    ///
    /// # Examples:
    /// ```
    /// use picodata::introspection::Introspection;
    /// use rmpv::Value;
    ///
    /// #[derive(Introspection, Default)]
    /// #[introspection(crate = picodata)]
    /// struct MyStruct {
    ///     #[introspection(config_default = 69105)]
    ///     number: i32,
    ///     #[introspection(config_default = format!("there's {} leaves in the pile", self.number))]
    ///     text: String,
    ///     doesnt_have_default: bool,
    /// }
    ///
    /// let mut s = MyStruct {
    ///     number: 2,
    ///     ..Default::default()
    /// };
    ///
    /// assert_eq!(s.get_field_default_value_as_rmpv("number").unwrap(), Some(Value::from(69105)));
    ///
    /// // Note here the actual value of `s.number` is used, not the config_default one. --------V
    /// assert_eq!(s.get_field_default_value_as_rmpv("text").unwrap(), Some(Value::from("there's 2 leaves in the pile")));
    ///
    /// assert_eq!(s.get_field_default_value_as_rmpv("doesnt_have_default").unwrap(), None);
    /// ```
    ///
    /// [`InstanceConfig::advertise_address`]: crate::config::InstanceConfig::advertise_address
    /// [`PicodataConfig::set_defaults_explicitly`]: crate::config::PicodataConfig::set_defaults_explicitly
    fn get_field_default_value_as_rmpv(
        &self,
        path: &str,
    ) -> Result<Option<rmpv::Value>, IntrospectionError>;
}

/// Information about a single struct field. This is the type which is stored
/// in [`Introspection::FIELD_INFOS`].
///
/// Currently this info is used when reporting errors about wrong field names
/// and for introspecting structs using for example [`leaf_field_paths`].
#[derive(PartialEq, Eq, Hash)]
pub struct FieldInfo {
    pub name: &'static str,
    pub nested_fields: &'static [FieldInfo],
}

impl std::fmt::Debug for FieldInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.name)?;
        if !self.nested_fields.is_empty() {
            write!(f, ": {:?}", self.nested_fields)?;
        }
        Ok(())
    }
}

/// Returns an array of period-separated (e.g. `field.sub_field`) paths
/// to struct fields including subfields of structs.
///
/// This is basically just a helper method for recursively walking over
/// the values from [`Introspection::FIELD_INFOS`] array.
///
/// # Examples:
/// ```
/// use picodata::introspection::{leaf_field_paths, Introspection};
///
/// #[derive(Introspection, Default)]
/// #[introspection(crate = picodata)]
/// struct MyStruct {
///     number: i32,
///     text: String,
///     #[introspection(nested)]
///     nested: NestedStruct,
/// }
///
/// #[derive(Introspection, Default)]
/// #[introspection(crate = picodata)]
/// struct NestedStruct {
///     sub_field: f32,
/// }
///
/// assert_eq!(
///     leaf_field_paths::<MyStruct>(),
///     &["number".to_owned(), "text".to_owned(), "nested.sub_field".to_owned()]
/// );
/// ```
pub fn leaf_field_paths<T>() -> Vec<String>
where
    T: Introspection,
{
    // TODO: do the awful ugly rust thing to cache this value
    let mut res = Vec::new();
    recursive_helper("", T::FIELD_INFOS, &mut res);
    return res;

    fn recursive_helper(prefix: &str, nested_fields: &[FieldInfo], res: &mut Vec<String>) {
        for field in nested_fields {
            let name = field.name;
            if field.nested_fields.is_empty() {
                res.push(format!("{prefix}{name}"));
            } else {
                recursive_helper(&format!("{prefix}{name}."), field.nested_fields, res);
            }
        }
    }
}

/// A public reimport for use in the derive macro.
pub use rmpv::Value as RmpvValue;

/// Converts a [`rmpv::Value`] `value` to a generic serde deserializable type.
/// This function is just needed to be called from the derived
/// [`Introspection::set_field_from_rmpv`] implementations.
#[inline(always)]
pub fn from_rmpv_value<T>(value: &rmpv::Value) -> Result<T, Error>
where
    T: for<'de> serde::Deserialize<'de>,
{
    rmpv::ext::deserialize_from(value.as_ref()).map_err(Error::other)
}

/// Converts a generic serde serializable value to [`rmpv::Value`]. This
/// function is just needed to be called from the derived
/// [`Introspection::get_field_as_rmpv`] implementations.
#[inline(always)]
pub fn to_rmpv_value<T>(v: &T) -> Result<rmpv::Value, Error>
where
    T: serde::Serialize,
{
    crate::to_rmpv_named::to_rmpv_named(v).map_err(Error::other)
}

#[derive(Debug, thiserror::Error)]
pub enum IntrospectionError {
    #[error("{}", Self::no_such_field_error_message(.parent, .field, .expected))]
    NoSuchField {
        parent: String,
        field: String,
        expected: &'static [FieldInfo],
    },

    #[error("incorrect value for field '{field}': {error}")]
    ConvertToFieldError {
        field: String,
        error: Box<dyn std::error::Error>,
    },

    #[error("{expected} '{path}'")]
    InvalidPath {
        path: String,
        expected: &'static str,
    },

    #[error("field '{field}' has no nested sub-fields")]
    NotNestable { field: String },

    #[error("field '{field}' cannot be assigned directly, must choose a sub-field (for example '{field}.{example}')")]
    AssignToNested {
        field: String,
        example: &'static str,
    },

    #[error("failed converting '{field}' to a msgpack value: {details}")]
    ToRmpvValue { field: String, details: Error },
}

impl IntrospectionError {
    fn no_such_field_error_message(parent: &str, field: &str, expected: &[FieldInfo]) -> String {
        let mut res = String::with_capacity(128);
        if !parent.is_empty() {
            _ = write!(&mut res, "{parent}: ");
        }
        use std::fmt::Write;
        _ = write!(&mut res, "unknown field `{field}`");

        let mut fields = expected.iter();
        if let Some(first) = fields.next() {
            _ = write!(&mut res, ", expected one of `{}`", first.name);
            for next in fields {
                _ = write!(&mut res, ", `{}`", next.name);
            }
        } else {
            _ = write!(&mut res, ", there are no fields at all");
        }

        res
    }

    pub fn with_prepended_prefix(mut self, prefix: &str) -> Self {
        match &mut self {
            Self::NotNestable { field } => {
                *field = format!("{prefix}.{field}");
            }
            Self::InvalidPath { path, .. } => {
                *path = format!("{prefix}.{path}");
            }
            Self::NoSuchField { parent, .. } => {
                if parent.is_empty() {
                    *parent = prefix.into();
                } else {
                    *parent = format!("{prefix}.{parent}");
                }
            }
            Self::ConvertToFieldError { field, .. } => {
                *field = format!("{prefix}.{field}");
            }
            Self::AssignToNested { field, .. } => {
                *field = format!("{prefix}.{field}");
            }
            Self::ToRmpvValue { field, .. } => {
                *field = format!("{prefix}.{field}");
            }
        }
        self
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use pretty_assertions::assert_eq;

    #[derive(Default, Debug, Introspection, PartialEq)]
    struct S {
        x: i32,
        #[introspection(config_default = self.x as f32 * 0.5)]
        y: f32,
        #[introspection(config_default = "this is a &str but it still works")]
        s: String,
        #[introspection(config_default = &["this", "also", "works"])]
        v: Vec<String>,
        #[introspection(nested)]
        r#struct: Nested,

        #[introspection(ignore)]
        ignored: serde_yaml::Value,
    }

    #[derive(Default, Debug, Introspection, PartialEq)]
    struct Nested {
        #[introspection(config_default = "nested of course works")]
        a: String,
        #[introspection(config_default = format!("{}, but type safety is missing unfortunately", self.a))]
        b: i64,
        #[introspection(nested)]
        empty: Empty,
    }

    #[derive(Default, Debug, Introspection, PartialEq)]
    struct Empty {}

    #[test]
    fn set_field_from_yaml() {
        let mut s = S::default();

        //
        // Check `set_field_from_yaml` error cases
        //
        let e = s.set_field_from_yaml("a", "foo").unwrap_err();
        assert_eq!(
            e.to_string(),
            "unknown field `a`, expected one of `x`, `y`, `s`, `v`, `struct`"
        );

        let e = s.set_field_from_yaml(".x", "foo").unwrap_err();
        assert_eq!(e.to_string(), "expected a field name before '.x'");

        let e = s.set_field_from_yaml("&-*%?!", "foo").unwrap_err();
        assert_eq!(
            e.to_string(),
            "unknown field `&-*%?!`, expected one of `x`, `y`, `s`, `v`, `struct`"
        );

        let e = s.set_field_from_yaml("x.foo.bar", "foo").unwrap_err();
        assert_eq!(e.to_string(), "field 'x' has no nested sub-fields");

        let e = s.set_field_from_yaml("struct", "foo").unwrap_err();
        assert_eq!(e.to_string(), "field 'struct' cannot be assigned directly, must choose a sub-field (for example 'struct.a')");

        let e = s.set_field_from_yaml("struct.empty", "foo").unwrap_err();
        assert_eq!(e.to_string(), "field 'struct.empty' cannot be assigned directly, must choose a sub-field (for example 'struct.empty.<actually there's no fields in this struct :(>')");

        let e = s.set_field_from_yaml("struct.foo", "foo").unwrap_err();
        assert_eq!(
            e.to_string(),
            "struct: unknown field `foo`, expected one of `a`, `b`, `empty`"
        );

        let e = s.set_field_from_yaml("struct.a.bar", "foo").unwrap_err();
        assert_eq!(e.to_string(), "field 'struct.a' has no nested sub-fields");

        let e = s.set_field_from_yaml("struct.a..", "foo").unwrap_err();
        assert_eq!(e.to_string(), "expected a field name after 'struct.a.'");

        let e = s.set_field_from_yaml("x.", "foo").unwrap_err();
        assert_eq!(e.to_string(), "expected a field name after 'x.'");

        let e = s.set_field_from_yaml("x", "foo").unwrap_err();
        assert_eq!(
            e.to_string(),
            "incorrect value for field 'x': invalid type: string \"foo\", expected i32"
        );

        let e = s.set_field_from_yaml("x", "'420'").unwrap_err();
        assert_eq!(
            e.to_string(),
            "incorrect value for field 'x': invalid type: string \"420\", expected i32"
        );

        let e = s.set_field_from_yaml("x", "'420'").unwrap_err();
        assert_eq!(
            e.to_string(),
            "incorrect value for field 'x': invalid type: string \"420\", expected i32"
        );

        let e = s.set_field_from_yaml("ignored", "foo").unwrap_err();
        assert_eq!(
            e.to_string(),
            "unknown field `ignored`, expected one of `x`, `y`, `s`, `v`, `struct`"
        );
        assert_eq!(s.ignored, serde_yaml::Value::default());

        let e = s
            .set_field_from_yaml("struct.empty.foo", "bar")
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "struct.empty: unknown field `foo`, there are no fields at all"
        );

        //
        // Check `set_field_from_yaml` success cases
        //
        s.set_field_from_yaml("v", "[1, 2, 3]").unwrap();
        assert_eq!(&s.v, &["1", "2", "3"]);
        s.set_field_from_yaml("v", "['foo', \"bar\", baz]").unwrap();
        assert_eq!(&s.v, &["foo", "bar", "baz"]);

        s.set_field_from_yaml("x", "420").unwrap();
        assert_eq!(s.x, 420);

        s.set_field_from_yaml("y", "13").unwrap();
        assert_eq!(s.y, 13.0);
        s.set_field_from_yaml("y", "13.37").unwrap();
        assert_eq!(s.y, 13.37);

        s.set_field_from_yaml("s", "13.37").unwrap();
        assert_eq!(s.s, "13.37");
        s.set_field_from_yaml("s", "foo bar").unwrap();
        assert_eq!(s.s, "foo bar");
        s.set_field_from_yaml("s", "'foo bar'").unwrap();
        assert_eq!(s.s, "foo bar");

        s.set_field_from_yaml("  struct  .  a  ", "aaaa").unwrap();
        assert_eq!(s.r#struct.a, "aaaa");
        s.set_field_from_yaml("struct.b", "  0xbbbb  ").unwrap();
        assert_eq!(s.r#struct.b, 0xbbbb);
    }

    #[test]
    fn set_field_from_rmpv() {
        let mut s = S::default();

        //
        // Error cases
        //
        let e = s
            .set_field_from_rmpv("x", &rmpv::Value::Boolean(true))
            .unwrap_err();
        assert_eq!(e.to_string(), "incorrect value for field 'x': error while decoding value: invalid type: boolean `true`, expected i32");

        let e = s
            .set_field_from_rmpv("struct.a", &rmpv::Value::Nil)
            .unwrap_err();
        assert_eq!(e.to_string(), "incorrect value for field 'struct.a': error while decoding value: invalid type: unit value, expected a string");

        let e = s
            .set_field_from_rmpv("struct", &rmpv::Value::Nil)
            .unwrap_err();
        assert_eq!(e.to_string(), "field 'struct' cannot be assigned directly, must choose a sub-field (for example 'struct.a')");

        // Other error cases are mostly the same as in case of `set_field_from_yaml`

        //
        // Success cases
        //
        s.set_field_from_rmpv("x", &rmpv::Value::from(123)).unwrap();
        s.set_field_from_rmpv("y", &rmpv::Value::F32(3.21)).unwrap();
        s.set_field_from_rmpv("s", &rmpv::Value::from("ccc"))
            .unwrap();
        s.set_field_from_rmpv(
            "v",
            &rmpv::Value::Array(vec![rmpv::Value::from("Vv"), rmpv::Value::from("vV")]),
        )
        .unwrap();

        s.set_field_from_rmpv("struct.a", &("AaAa".into())).unwrap();
        s.set_field_from_rmpv("struct.b", &(0xccc.into())).unwrap();
        assert_eq!(
            s,
            S {
                x: 123,
                y: 3.21,
                s: "ccc".into(),
                v: vec!["Vv".into(), "vV".into()],
                r#struct: Nested {
                    a: "AaAa".into(),
                    b: 0xccc,
                    empty: Empty {},
                },
                ignored: Default::default(),
            },
        );
    }

    #[test]
    fn get_field_as_rmpv() {
        let s = S {
            x: 111,
            y: 2.22,
            s: "sssss".into(),
            v: vec!["v".into(), "vv".into(), "vvv".into()],
            r#struct: Nested {
                a: "aaaaaa".into(),
                b: 0xbbbbbb,
                empty: Empty {},
            },
            ignored: serde_yaml::Value::default(),
        };

        //
        // Check `get_field_as_rmpv` error cases
        //
        let e = s.get_field_as_rmpv("a").unwrap_err();
        assert_eq!(
            e.to_string(),
            "unknown field `a`, expected one of `x`, `y`, `s`, `v`, `struct`"
        );

        let e = s.get_field_as_rmpv(".x").unwrap_err();
        assert_eq!(e.to_string(), "expected a field name before '.x'");

        let e = s.get_field_as_rmpv("&-*%?!").unwrap_err();
        assert_eq!(
            e.to_string(),
            "unknown field `&-*%?!`, expected one of `x`, `y`, `s`, `v`, `struct`"
        );

        let e = s.get_field_as_rmpv("x.foo.bar").unwrap_err();
        assert_eq!(e.to_string(), "field 'x' has no nested sub-fields");

        let e = s.get_field_as_rmpv("struct.foo").unwrap_err();
        assert_eq!(
            e.to_string(),
            "struct: unknown field `foo`, expected one of `a`, `b`, `empty`"
        );

        let e = s.get_field_as_rmpv("struct.a.bar").unwrap_err();
        assert_eq!(e.to_string(), "field 'struct.a' has no nested sub-fields");

        let e = s.get_field_as_rmpv("struct.a..").unwrap_err();
        assert_eq!(e.to_string(), "expected a field name after 'struct.a.'");

        let e = s.get_field_as_rmpv("x.").unwrap_err();
        assert_eq!(e.to_string(), "expected a field name after 'x.'");

        let e = s.get_field_as_rmpv("ignored").unwrap_err();
        assert_eq!(
            e.to_string(),
            "unknown field `ignored`, expected one of `x`, `y`, `s`, `v`, `struct`"
        );
        assert_eq!(s.ignored, serde_yaml::Value::default());

        let e = s.get_field_as_rmpv("struct.empty.foo").unwrap_err();
        assert_eq!(
            e.to_string(),
            "struct.empty: unknown field `foo`, there are no fields at all"
        );

        //
        // Check `get_field_as_rmpv` success cases
        //
        assert_eq!(s.get_field_as_rmpv("x").unwrap(), rmpv::Value::from(111));
        assert_eq!(s.get_field_as_rmpv("y").unwrap(), rmpv::Value::F32(2.22));
        assert_eq!(
            s.get_field_as_rmpv("s").unwrap(),
            rmpv::Value::from("sssss")
        );
        assert_eq!(
            s.get_field_as_rmpv("v").unwrap(),
            rmpv::Value::Array(vec![
                rmpv::Value::from("v"),
                rmpv::Value::from("vv"),
                rmpv::Value::from("vvv"),
            ])
        );

        assert_eq!(
            s.get_field_as_rmpv("struct.a").unwrap(),
            rmpv::Value::from("aaaaaa")
        );
        assert_eq!(
            s.get_field_as_rmpv("struct.b").unwrap(),
            rmpv::Value::from(0xbbbbbb)
        );
        assert_eq!(
            s.get_field_as_rmpv("struct.empty").unwrap(),
            rmpv::Value::Map(vec![])
        );

        // We can also get the entire `struct` sub-field if we wanted:
        assert_eq!(
            s.get_field_as_rmpv("struct").unwrap(),
            rmpv::Value::Map(vec![
                (rmpv::Value::from("a"), rmpv::Value::from("aaaaaa")),
                (rmpv::Value::from("b"), rmpv::Value::from(0xbbbbbb)),
                (rmpv::Value::from("empty"), rmpv::Value::Map(vec![])),
            ])
        );
    }

    #[test]
    fn nested_field_names() {
        assert_eq!(
            S::FIELD_INFOS,
            &[
                FieldInfo {
                    name: "x",
                    nested_fields: &[]
                },
                FieldInfo {
                    name: "y",
                    nested_fields: &[]
                },
                FieldInfo {
                    name: "s",
                    nested_fields: &[]
                },
                FieldInfo {
                    name: "v",
                    nested_fields: &[]
                },
                FieldInfo {
                    name: "struct",
                    nested_fields: &[
                        FieldInfo {
                            name: "a",
                            nested_fields: &[]
                        },
                        FieldInfo {
                            name: "b",
                            nested_fields: &[]
                        },
                        FieldInfo {
                            name: "empty",
                            nested_fields: &[]
                        },
                    ]
                },
            ],
        );

        assert_eq!(
            leaf_field_paths::<S>(),
            &["x", "y", "s", "v", "struct.a", "struct.b", "struct.empty"]
        );

        assert_eq!(leaf_field_paths::<Nested>(), &["a", "b", "empty"]);
    }

    #[test]
    fn get_field_default_value_as_rmpv() {
        let s = S {
            x: 999,
            y: 8.88,
            s: "S".into(),
            v: vec!["V0".into(), "V1".into(), "V2".into()],
            r#struct: Nested {
                a: "self.a which was set explicitly".into(),
                b: 0xb,
                empty: Empty {},
            },
            ignored: serde_yaml::Value::default(),
        };

        //
        // Error cases are mostly covered in tests above
        //
        let e = s
            .get_field_default_value_as_rmpv("struct.no_such_field")
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "struct: unknown field `no_such_field`, expected one of `a`, `b`, `empty`"
        );

        let e = s.get_field_default_value_as_rmpv("ignored").unwrap_err();
        assert_eq!(
            e.to_string(),
            "unknown field `ignored`, expected one of `x`, `y`, `s`, `v`, `struct`"
        );

        //
        // Success cases
        //
        assert_eq!(s.get_field_default_value_as_rmpv("x").unwrap(), None);
        // Remember, it's `self.x * 0.5`
        assert_eq!(
            s.get_field_default_value_as_rmpv("y").unwrap(),
            Some(rmpv::Value::F32(499.5))
        );

        assert_eq!(
            s.get_field_default_value_as_rmpv("s").unwrap(),
            Some(rmpv::Value::from("this is a &str but it still works"))
        );

        assert_eq!(
            s.get_field_default_value_as_rmpv("v").unwrap(),
            Some(rmpv::Value::Array(vec![
                rmpv::Value::from("this"),
                rmpv::Value::from("also"),
                rmpv::Value::from("works"),
            ]))
        );

        assert_eq!(
            s.get_field_default_value_as_rmpv("struct.a").unwrap(),
            Some(rmpv::Value::from("nested of course works"))
        );

        assert_eq!(
            s.get_field_default_value_as_rmpv("struct.b").unwrap(),
            Some(rmpv::Value::from(
                "self.a which was set explicitly, but type safety is missing unfortunately"
            ))
        );

        assert_eq!(
            s.get_field_default_value_as_rmpv("struct.empty").unwrap(),
            None
        );

        // For some reason we even allow getting the default for the whole subsection.
        #[rustfmt::skip]
        assert_eq!(
            s.get_field_default_value_as_rmpv("struct").unwrap(),
            Some(rmpv::Value::Map(vec![
                (rmpv::Value::from("a"), rmpv::Value::from("nested of course works")),
                (rmpv::Value::from("b"), rmpv::Value::from("self.a which was set explicitly, but type safety is missing unfortunately")),
            ])),
        );
    }
}
