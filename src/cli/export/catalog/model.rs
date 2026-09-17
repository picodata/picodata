//! The catalog that the exporter reads back via pgproto.
//!
//! The json that arrives does not fit picodata's catalog types, which
//! are defined in the `schema.rs` file (`Distribution`, `TableOption`,
//! `IndexOption`). A row is kept as msgpack, and pgproto turns that msgpack
//! into json as it is. As a result, the JSON has the msgpack structure,
//! while these types read a different structure using serde.
//! One structure cannot satisfy both requirements:
//!
//! * `Decimal` is an extension of msgpack, and pgproto converts it to a string;
//!   `schema::IndexOption`, however, expects a packed pair and throws an error in this case;
//! * `_pico_table` is encoded using a derived msgpack format that wraps the value in a
//!   single-element array (`{“unlogged”: [true]}`), whereas `schema::TableOption` reads `{“unlogged”: true}`;
//! * `Distribution` is serialized as `{“kind”: ...}` for the Raft log,
//!   rather than as `{“ShardedImplicitly”: [..]}`.
//!
//! Teaching the catalog types to read both formats would change how the Raft log itself is decoded,
//! so the exporter maintains its own `Raw*` mirrors. Their correspondence to the originals is ensured
//! by the `assert_*_shape` assertions listed below, which halt compilation when a variant is added,
//! as well as by tests that encode the catalog type and decode the result here.

use serde::Deserialize;
use smol_str::SmolStr;
use tarantool::index::{Part, RtreeIndexDistanceType};
use tarantool::space::Field;

use crate::config::ReplicationMode;
use crate::schema::ShardingFn;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawTier {
    pub name: String,
    pub replication_factor: i64,
    pub bucket_count: i64,
    pub replication_mode: ReplicationMode,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RawTable {
    pub id: i64,
    pub name: String,
    pub distribution: RawDistribution,
    pub format: Vec<Field>,
    pub engine: String,
    pub description: String,
    pub options: Vec<RawTableOption>,
}

/// A row of `_pico_index`; the primary key is the one with `id = 0`.
#[derive(Debug, Clone, PartialEq)]
pub struct RawIndex {
    pub table_id: i64,
    pub id: i64,
    pub name: String,
    /// Uses text instead of an enum to handle new index types.
    pub ty: String,
    pub options: Vec<RawIndexOption>,
    pub parts: Vec<Part<String>>,
}

////////////////////////////////////////////////////////////////////////////////
// Distribution
////////////////////////////////////////////////////////////////////////////////

/// Represents [`crate::schema::Distribution`] in the form in which pgproto sends it:
/// a msgpack tuple converted to JSON, `{“ShardedImplicitly”: [...]}`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub enum RawDistribution {
    Global,
    ShardedImplicitly(Vec<SmolStr>, ShardingFn, SmolStr),
    ShardedByField(SmolStr, SmolStr),
}

/// Ensures we cover every Distribution variant in RawDistribution
#[expect(dead_code)]
fn assert_distribution_shape(original: crate::schema::Distribution) {
    use crate::schema::Distribution;
    let _: RawDistribution = match original {
        Distribution::Global => RawDistribution::Global,
        Distribution::ShardedImplicitly {
            sharding_key,
            sharding_fn,
            tier,
        } => RawDistribution::ShardedImplicitly(sharding_key, sharding_fn, tier),
        Distribution::ShardedByField { field, tier } => {
            RawDistribution::ShardedByField(field, tier)
        }
    };
}

////////////////////////////////////////////////////////////////////////////////
// Table options
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawTableOption {
    Unlogged(#[serde(deserialize_with = "unwrap_one_element_tuple")] bool),
    PkContainsBucketId(#[serde(deserialize_with = "unwrap_one_element_tuple")] bool),
    Synchronous(#[serde(deserialize_with = "unwrap_one_element_tuple")] bool),
}

/// `_pico_table` is written by the `msgpack::Encode` derive, which wraps even a
/// single-field variant in an array (`{"unlogged": [true]}`); `_pico_index` is
/// written by rmp-serde, which does not (`{"unique": true}`). This unwraps the first.
fn unwrap_one_element_tuple<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    <(T,)>::deserialize(deserializer).map(|(value,)| value)
}

/// Ensures we cover every TableOption variant in RawTableOption
#[expect(dead_code)]
fn assert_table_option_shape(original: crate::schema::TableOption) {
    use crate::schema::TableOption;
    let _: RawTableOption = match original {
        TableOption::Unlogged(enabled) => RawTableOption::Unlogged(enabled),
        TableOption::PkContainsBucketId(enabled) => RawTableOption::PkContainsBucketId(enabled),
        TableOption::Synchronous(enabled) => RawTableOption::Synchronous(enabled),
    };
}

////////////////////////////////////////////////////////////////////////////////
// Index options
////////////////////////////////////////////////////////////////////////////////

/// Unlike [`RawTableOption`], `_pico_index` is encoded with rmp-serde, so the
/// value is not wrapped in an array: `{"unique": true}`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawIndexOption {
    #[serde(rename = "bloom_fpr")]
    BloomFalsePositiveRate(DecimalLiteral),
    Dimension(u8),
    Distance(RtreeIndexDistanceType),
    Hint(bool),
    PageSize(u32),
    RangeSize(u32),
    RunCountPerLevel(u32),
    RunSizeRatio(DecimalLiteral),
    CompressionLevel(i8),
    Unique(bool),
}

impl RawIndexOption {
    /// The option name and its value, ready for use in a `WITH (...)` clause.
    pub fn as_pair(&self) -> (&'static str, &dyn std::fmt::Display) {
        match self {
            Self::BloomFalsePositiveRate(value) => ("bloom_fpr", value),
            Self::Dimension(value) => ("dimension", value),
            Self::Distance(value) => ("distance", value),
            Self::Hint(value) => ("hint", value),
            Self::PageSize(value) => ("page_size", value),
            Self::RangeSize(value) => ("range_size", value),
            Self::RunCountPerLevel(value) => ("run_count_per_level", value),
            Self::RunSizeRatio(value) => ("run_size_ratio", value),
            Self::CompressionLevel(value) => ("compression_level", value),
            Self::Unique(value) => ("unique", value),
        }
    }

    pub fn is_vinyl(&self) -> bool {
        matches!(
            self,
            Self::BloomFalsePositiveRate(_)
                | Self::PageSize(_)
                | Self::RangeSize(_)
                | Self::RunCountPerLevel(_)
                | Self::RunSizeRatio(_)
                | Self::CompressionLevel(_)
        )
    }
}

/// Ensures we cover every IndexOption variant in RawIndexOption
#[expect(dead_code)]
fn assert_index_option_shape(original: crate::schema::IndexOption) {
    use crate::schema::IndexOption;
    let _: RawIndexOption = match original {
        IndexOption::BloomFalsePositiveRate(rate) => {
            RawIndexOption::BloomFalsePositiveRate(rate.to_string().into())
        }
        IndexOption::Dimension(dimension) => RawIndexOption::Dimension(dimension),
        IndexOption::Distance(distance) => RawIndexOption::Distance(distance),
        IndexOption::Hint(hint) => RawIndexOption::Hint(hint),
        IndexOption::PageSize(size) => RawIndexOption::PageSize(size),
        IndexOption::RangeSize(size) => RawIndexOption::RangeSize(size),
        IndexOption::RunCountPerLevel(count) => RawIndexOption::RunCountPerLevel(count),
        IndexOption::RunSizeRatio(ratio) => RawIndexOption::RunSizeRatio(ratio.to_string().into()),
        IndexOption::CompressionLevel(level) => RawIndexOption::CompressionLevel(level),
        IndexOption::Unique(unique) => RawIndexOption::Unique(unique),
    };
}

////////////////////////////////////////////////////////////////////////////////
// Decimal
////////////////////////////////////////////////////////////////////////////////

/// TODO: pgproto is going to send a number instead of a string. See: <https://git.picodata.io/core/picodata/-/work_items/3245>
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct DecimalLiteral(String);

impl From<String> for DecimalLiteral {
    fn from(literal: String) -> Self {
        Self(literal)
    }
}

impl From<&str> for DecimalLiteral {
    fn from(literal: &str) -> Self {
        Self(literal.to_owned())
    }
}

/// A decimal keeps no trailing zeros and the grammar's `Decimal` rejects it.
impl std::fmt::Display for DecimalLiteral {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)?;
        if !self.0.contains('.') {
            formatter.write_str(".0")?;
        }
        Ok(())
    }
}

/// TODO: share this with pgproto instead of repeating its path around the conversion.
fn convert_like_pgproto(msgpack: &[u8]) -> serde_json::Value {
    let value = rmpv::decode::read_value(&mut &msgpack[..]).expect("valid msgpack");
    serde_json::to_value(crate::util::MpValueAsJson(&value)).expect("pgproto renders it as json")
}

/// How `_pico_index` columns are encoded: rmp-serde.
fn encode_via_rmp_serde<T: tarantool::tuple::Encode>(value: &T) -> serde_json::Value {
    let mut buffer = Vec::new();
    value.encode(&mut buffer).expect("encodes");
    convert_like_pgproto(&buffer)
}

/// Tests that need a real [`tarantool::decimal::Decimal`] from tarantool.
mod decimal_tests {
    use super::*;
    use crate::schema::IndexOption;
    use tarantool::decimal::Decimal;

    #[::tarantool::test]
    fn the_decimal_index_options_agree_with_schema_rs() {
        let rate: Decimal = "0.05".parse().expect("a decimal literal");
        let cases = [
            (
                IndexOption::BloomFalsePositiveRate(rate),
                RawIndexOption::BloomFalsePositiveRate("0.05".into()),
            ),
            (
                IndexOption::RunSizeRatio(rate),
                RawIndexOption::RunSizeRatio("0.05".into()),
            ),
        ];

        for (original, raw) in cases {
            let (name, value) = raw.as_pair();
            assert_eq!(name, original.type_name());
            assert_eq!(raw.is_vinyl(), original.is_vinyl(), "{name}");
            assert_eq!(value.to_string(), rate.to_string(), "{name}");

            let pgproto_json = encode_via_rmp_serde(&vec![original]);
            let decoded: Vec<RawIndexOption> = serde_json::from_value(pgproto_json.clone())
                .unwrap_or_else(|error| panic!("{pgproto_json} should decode: {error}"));
            assert_eq!(decoded, vec![raw]);
        }
    }

    #[::tarantool::test]
    fn a_decimal_without_a_point_gets_one() {
        let ratio: Decimal = f64::try_into(2.0).expect("a decimal");
        assert_eq!(ratio.to_string(), "2");

        let pgproto_json = encode_via_rmp_serde(&vec![IndexOption::RunSizeRatio(ratio)]);
        let decoded: Vec<RawIndexOption> = serde_json::from_value(pgproto_json.clone())
            .unwrap_or_else(|error| panic!("{pgproto_json} should decode: {error}"));
        assert_eq!(decoded, vec![RawIndexOption::RunSizeRatio("2".into())]);
        assert_eq!(decoded[0].as_pair().1.to_string(), "2.0");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Distribution, IndexOption, TableOption};
    use pretty_assertions::assert_eq;
    use serde_json::json;

    /// How `_pico_table` columns are encoded.
    fn encode_via_msgpack_derive<T: tarantool::msgpack::Encode>(value: &T) -> serde_json::Value {
        convert_like_pgproto(&tarantool::msgpack::encode(value))
    }

    /// Encodes the catalog type in the same way as its own table does,
    /// specifies what json from pgproto passes on, and decodes exactly this JSON.
    fn round_trip<C, R>(catalog_value: &C, pgproto_json: serde_json::Value, expected: R)
    where
        C: tarantool::msgpack::Encode,
        R: serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        assert_eq!(encode_via_msgpack_derive(catalog_value), pgproto_json);
        let decoded: R = serde_json::from_value(pgproto_json.clone())
            .unwrap_or_else(|error| panic!("{pgproto_json} should decode: {error}"));
        assert_eq!(decoded, expected);
    }

    #[test]
    fn a_distribution_goes_from_pico_table_to_us() {
        round_trip(
            &Distribution::Global,
            json!({"Global": null}),
            RawDistribution::Global,
        );
        round_trip(
            &Distribution::ShardedImplicitly {
                sharding_key: vec!["id".into(), "name".into()],
                sharding_fn: ShardingFn::Crc32,
                tier: "storage".into(),
            },
            json!({"ShardedImplicitly": [["id", "name"], "crc32", "storage"]}),
            RawDistribution::ShardedImplicitly(
                vec!["id".into(), "name".into()],
                ShardingFn::Crc32,
                "storage".into(),
            ),
        );
        round_trip(
            &Distribution::ShardedByField {
                field: "bucket_id".into(),
                tier: "default".into(),
            },
            json!({"ShardedByField": ["bucket_id", "default"]}),
            RawDistribution::ShardedByField("bucket_id".into(), "default".into()),
        );
    }

    /// `_pico_table` options are wrapped a value in a one-element array.
    #[test]
    fn a_table_option_goes_from_pico_table_to_us() {
        round_trip(
            &vec![
                TableOption::Unlogged(true),
                TableOption::PkContainsBucketId(true),
                TableOption::Synchronous(false),
            ],
            json!([
                {"unlogged": [true]},
                {"pk_contains_bucket_id": [true]},
                {"synchronous": [false]},
            ]),
            vec![
                RawTableOption::Unlogged(true),
                RawTableOption::PkContainsBucketId(true),
                RawTableOption::Synchronous(false),
            ],
        );
    }

    /// `_pico_index` is encoded with rmp-serde, which does not wrap.
    #[test]
    fn an_index_option_goes_from_pico_index_to_us() {
        let pgproto_json = json!([{"unique": true}, {"page_size": 1024}]);
        assert_eq!(
            encode_via_rmp_serde(&vec![
                IndexOption::Unique(true),
                IndexOption::PageSize(1024)
            ]),
            pgproto_json
        );

        let decoded: Vec<RawIndexOption> = serde_json::from_value(pgproto_json.clone())
            .unwrap_or_else(|error| {
                panic!("{pgproto_json} should decode: {error}");
            });
        assert_eq!(
            decoded,
            vec![RawIndexOption::Unique(true), RawIndexOption::PageSize(1024)]
        );
        assert!(!decoded[0].is_vinyl());
        assert!(decoded[1].is_vinyl());
    }

    #[test]
    fn index_option_names_and_kinds_agree_with_schema_rs() {
        let cases = [
            (IndexOption::Dimension(2), RawIndexOption::Dimension(2)),
            (
                IndexOption::Distance(RtreeIndexDistanceType::Manhattan),
                RawIndexOption::Distance(RtreeIndexDistanceType::Manhattan),
            ),
            (IndexOption::Hint(true), RawIndexOption::Hint(true)),
            (IndexOption::PageSize(1024), RawIndexOption::PageSize(1024)),
            (
                IndexOption::RangeSize(1024),
                RawIndexOption::RangeSize(1024),
            ),
            (
                IndexOption::RunCountPerLevel(2),
                RawIndexOption::RunCountPerLevel(2),
            ),
            (
                IndexOption::CompressionLevel(3),
                RawIndexOption::CompressionLevel(3),
            ),
            (IndexOption::Unique(true), RawIndexOption::Unique(true)),
        ];

        for (original, raw) in cases {
            let (name, value) = raw.as_pair();
            assert_eq!(name, original.type_name());
            assert_eq!(raw.is_vinyl(), original.is_vinyl(), "{name}");

            let value = serde_json::from_str(&value.to_string())
                .unwrap_or_else(|_| json!(value.to_string()));
            let decoded: RawIndexOption =
                serde_json::from_value(json!({ name: value })).expect("decodes");
            assert_eq!(decoded, raw);
        }
    }

    #[test]
    fn a_decimal_arrives_as_a_string_holding_its_literal() {
        let decoded: Vec<RawIndexOption> = serde_json::from_value(json!([
            {"bloom_fpr": "0.001"},
            {"run_size_ratio": "12345.6789"},
        ]))
        .expect("decodes");

        assert_eq!(
            decoded,
            vec![
                RawIndexOption::BloomFalsePositiveRate("0.001".into()),
                RawIndexOption::RunSizeRatio("12345.6789".into()),
            ]
        );
    }

    #[test]
    fn a_whole_decimal_is_written_with_a_point() {
        let decoded: Vec<RawIndexOption> =
            serde_json::from_value(json!([{"run_size_ratio": "2"}, {"bloom_fpr": "100"}]))
                .expect("decodes");

        assert_eq!(
            decoded,
            vec![
                RawIndexOption::RunSizeRatio("2".into()),
                RawIndexOption::BloomFalsePositiveRate("100".into()),
            ]
        );
        let written: Vec<String> = decoded
            .iter()
            .map(|option| option.as_pair().1.to_string())
            .collect();
        assert_eq!(written, ["2.0", "100.0"]);
    }

    #[test]
    fn a_decimal_that_is_not_a_string_is_rejected() {
        serde_json::from_value::<RawIndexOption>(json!({"bloom_fpr": [1, [2, 28]]}))
            .expect_err("only a literal is accepted");
    }

    #[test]
    fn format_columns_are_read_into_the_tarantool_type() {
        let decoded: Vec<Field> = serde_json::from_value(json!([
            {"name": "id", "field_type": "integer", "is_nullable": false},
            {"name": "tags", "field_type": "text[]", "is_nullable": true},
            {"name": "payload", "field_type": "map", "is_nullable": true},
        ]))
        .expect("decodes");

        use tarantool::space::{FieldType, TypedArray};
        assert_eq!(decoded[0].field_type, FieldType::Integer);
        assert!(!decoded[0].is_nullable);
        assert_eq!(decoded[1].field_type, FieldType::Array(TypedArray::String));
        assert_eq!(decoded[2].field_type, FieldType::Map);
    }

    #[test]
    fn index_parts_are_read_from_arrays_of_varying_length() {
        // The sixth element only shows up for descending parts.
        let decoded: Vec<Part<String>> = serde_json::from_value(json!([
            ["a", "integer", null, false, null, "desc"],
            ["b", "integer", null, false, null],
        ]))
        .expect("decodes");

        use tarantool::index::SortOrder;
        assert_eq!(decoded[0].field, "a");
        assert_eq!(decoded[0].sort_order, Some(SortOrder::Desc));
        assert_eq!(decoded[1].field, "b");
        assert_eq!(decoded[1].sort_order, None);
    }
}
