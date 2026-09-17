use std::borrow::Cow;

use tarantool::space::{FieldType, TypedArray};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the column type `{}` has no equivalent in picodata's DDL", .field_type.as_str())]
pub(super) struct UnsupportedType {
    field_type: FieldType,
}

pub(super) fn render_sql_type(field_type: FieldType) -> Result<Cow<'static, str>, UnsupportedType> {
    match field_type {
        FieldType::Array(element) => get_element_type(element)
            .and_then(render_scalar_type)
            .map(|element| Cow::Owned(format!("{element}[]"))),
        scalar => render_scalar_type(scalar).map(Cow::Borrowed),
    }
    .ok_or(UnsupportedType { field_type })
}

fn render_scalar_type(field_type: FieldType) -> Option<&'static str> {
    match field_type {
        FieldType::Unsigned => Some("UNSIGNED"),
        FieldType::Integer => Some("INT"),
        FieldType::Double => Some("DOUBLE"),
        // `DECIMAL(p, s)` and `VARCHAR(n)` are stored without their parameters.
        FieldType::Decimal => Some("DECIMAL"),
        FieldType::Boolean => Some("BOOL"),
        FieldType::String => Some("TEXT"),
        FieldType::Datetime => Some("DATETIME"),
        FieldType::Uuid => Some("UUID"),
        // `JSON` is stored as `map`.
        FieldType::Map => Some("JSON"),
        // Not expressible in `ColumnDefType` of the grammar.
        _ => None,
    }
}

fn get_element_type(element: TypedArray) -> Option<FieldType> {
    match element {
        TypedArray::Boolean => Some(FieldType::Boolean),
        TypedArray::Datetime => Some(FieldType::Datetime),
        TypedArray::Decimal => Some(FieldType::Decimal),
        TypedArray::Double => Some(FieldType::Double),
        TypedArray::Integer => Some(FieldType::Integer),
        TypedArray::String => Some(FieldType::String),
        TypedArray::Uuid => Some(FieldType::Uuid),
        TypedArray::Map => Some(FieldType::Map),
        // The grammar always requires an element type.
        TypedArray::Any => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn scalar_types_map_the_way_the_ddl_spells_them() {
        let cases = [
            (FieldType::Unsigned, "UNSIGNED"),
            (FieldType::Integer, "INT"),
            (FieldType::Double, "DOUBLE"),
            (FieldType::Decimal, "DECIMAL"),
            (FieldType::Boolean, "BOOL"),
            (FieldType::String, "TEXT"),
            (FieldType::Datetime, "DATETIME"),
            (FieldType::Uuid, "UUID"),
            (FieldType::Map, "JSON"),
        ];

        for (field_type, expected) in cases {
            assert_eq!(render_sql_type(field_type).as_deref(), Ok(expected));
        }
    }

    #[test]
    fn arrays_keep_their_element_type() {
        let cases = [
            (TypedArray::String, "TEXT[]"),
            (TypedArray::Integer, "INT[]"),
            (TypedArray::Uuid, "UUID[]"),
            (TypedArray::Double, "DOUBLE[]"),
            (TypedArray::Boolean, "BOOL[]"),
            (TypedArray::Datetime, "DATETIME[]"),
            (TypedArray::Decimal, "DECIMAL[]"),
            (TypedArray::Map, "JSON[]"),
        ];

        for (element, expected) in cases {
            assert_eq!(
                render_sql_type(FieldType::Array(element)).as_deref(),
                Ok(expected)
            );
        }
    }

    #[test]
    fn types_without_a_ddl_spelling_are_refused() {
        for field_type in [
            FieldType::Any,
            FieldType::Number,
            FieldType::Scalar,
            FieldType::Varbinary,
            FieldType::Interval,
            FieldType::Array(TypedArray::Any),
        ] {
            let error = render_sql_type(field_type).expect_err("there is no DDL spelling");
            assert_eq!(error.field_type, field_type);
        }
    }
}
