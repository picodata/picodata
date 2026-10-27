//! The catalog that the exporter reads back via pgproto.
//!
//! The json that arrives does not fit picodata's catalog types, which
//! are defined in the `schema.rs` file (`Distribution`, `TableOption`,
//! `IndexOption`). A row is kept as msgpack, and pgproto turns that msgpack
//! into json as it is. As a result, the JSON has the msgpack structure,
//! while these types read a different structure using serde.
//! One structure cannot satisfy both requirements:
//!
//! * `Decimal` is an extension of msgpack, and pgproto converts it to a json
//!   number; `schema::IndexOption`, however, expects `Decimal`'s serde form
//!   and fails to deserialize a number;
//! * `_pico_table` is encoded using a derived msgpack format that wraps the value in a
//!   single-element array (`{“unlogged”: [true]}`), whereas `schema::TableOption` reads `{“unlogged”: true}`;
//! * `Distribution` is serialized as `{“kind”: ...}` for the Raft log,
//!   rather than as `{“ShardedImplicitly”: [..]}`.
//!
//! Teaching the catalog types to read both formats would change how the Raft log itself is decoded,
//! so the exporter maintains its own `Raw*` mirrors. Their correspondence to the originals is ensured
//! by the `assert_*_shape` assertions listed below, which halt compilation when a variant is added,
//! as well as by tests that encode the catalog type and decode the result here.

use rust_decimal::Decimal;
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
    #[serde(rename = "bloom_fpr", deserialize_with = "decimal_from_json")]
    BloomFalsePositiveRate(Decimal),
    Dimension(u8),
    Distance(RtreeIndexDistanceType),
    Hint(bool),
    PageSize(u32),
    RangeSize(u32),
    RunCountPerLevel(u32),
    #[serde(deserialize_with = "decimal_from_json")]
    RunSizeRatio(Decimal),
    CompressionLevel(i8),
    CompressionDict(bool),
    Unique(bool),
}

impl RawIndexOption {
    /// The option name and its value, ready for use in a `WITH (...)` clause.
    pub fn as_pair(&self) -> (&'static str, String) {
        match self {
            Self::BloomFalsePositiveRate(value) => ("bloom_fpr", decimal_literal(value)),
            Self::Dimension(value) => ("dimension", value.to_string()),
            Self::Distance(value) => ("distance", value.to_string()),
            Self::Hint(value) => ("hint", value.to_string()),
            Self::PageSize(value) => ("page_size", value.to_string()),
            Self::RangeSize(value) => ("range_size", value.to_string()),
            Self::RunCountPerLevel(value) => ("run_count_per_level", value.to_string()),
            Self::RunSizeRatio(value) => ("run_size_ratio", decimal_literal(value)),
            Self::CompressionLevel(value) => ("compression_level", value.to_string()),
            Self::CompressionDict(value) => ("compression_dict", value.to_string()),
            Self::Unique(value) => ("unique", value.to_string()),
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
                | Self::CompressionDict(_)
        )
    }
}

/// Ensures we cover every IndexOption variant in RawIndexOption
#[expect(dead_code)]
fn assert_index_option_shape(original: crate::schema::IndexOption) {
    use crate::schema::IndexOption;
    let _: RawIndexOption = match original {
        IndexOption::BloomFalsePositiveRate(rate) => {
            RawIndexOption::BloomFalsePositiveRate(rate.to_string().parse().unwrap())
        }
        IndexOption::Dimension(dimension) => RawIndexOption::Dimension(dimension),
        IndexOption::Distance(distance) => RawIndexOption::Distance(distance),
        IndexOption::Hint(hint) => RawIndexOption::Hint(hint),
        IndexOption::PageSize(size) => RawIndexOption::PageSize(size),
        IndexOption::RangeSize(size) => RawIndexOption::RangeSize(size),
        IndexOption::RunCountPerLevel(count) => RawIndexOption::RunCountPerLevel(count),
        IndexOption::RunSizeRatio(ratio) => {
            RawIndexOption::RunSizeRatio(ratio.to_string().parse().unwrap())
        }
        IndexOption::CompressionLevel(level) => RawIndexOption::CompressionLevel(level),
        IndexOption::CompressionDict(enabled) => RawIndexOption::CompressionDict(enabled),
        IndexOption::Unique(unique) => RawIndexOption::Unique(unique),
    };
}

////////////////////////////////////////////////////////////////////////////////
// Decimal
////////////////////////////////////////////////////////////////////////////////

/// Reads a decimal option value that pgproto sent as a json number.
///
/// The number text is handed over through `serde_json::value::RawValue`:
/// `serde_json::Value` would parse it into an `f64` and lose digits beyond
/// float precision. [`Decimal`] then parses it, which validates the value.
///
/// Note: a value wider than [`Decimal`]'s 28 significant digits fails to
/// decode, though tarantool itself holds up to 38. Real option values are far
/// shorter.
fn decimal_from_json<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde_json::value::RawValue;

    let json = Box::<RawValue>::deserialize(deserializer)?;
    Decimal::from_str_exact(json.get()).map_err(serde::de::Error::custom)
}

/// The grammar's decimal literal rejects a value without a point, so `2`
/// comes out as `2.0`.
fn decimal_literal(value: &Decimal) -> String {
    if value.scale() == 0 {
        format!("{value:.1}")
    } else {
        value.to_string()
    }
}

/// TODO: share this with pgproto instead of repeating its path around the conversion.
///
/// Returns json text rather than a `serde_json::Value`: the latter would parse
/// a decimal number into an `f64` and lose digits beyond float precision.
fn convert_like_pgproto(msgpack: &[u8]) -> String {
    let value = rmpv::decode::read_value(&mut &msgpack[..]).expect("valid msgpack");
    serde_json::to_string(&crate::util::MpValueAsJson(&value)).expect("pgproto renders it as json")
}

/// How `_pico_index` columns are encoded: rmp-serde.
fn encode_via_rmp_serde<T: tarantool::tuple::Encode>(value: &T) -> String {
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
                RawIndexOption::BloomFalsePositiveRate("0.05".parse().unwrap()),
            ),
            (
                IndexOption::RunSizeRatio(rate),
                RawIndexOption::RunSizeRatio("0.05".parse().unwrap()),
            ),
        ];

        for (original, raw) in cases {
            let (name, value) = raw.as_pair();
            assert_eq!(name, original.type_name());
            assert_eq!(raw.is_vinyl(), original.is_vinyl(), "{name}");
            assert_eq!(value, rate.to_string(), "{name}");

            let pgproto_json = encode_via_rmp_serde(&vec![original]);
            let decoded: Vec<RawIndexOption> = serde_json::from_str(&pgproto_json)
                .unwrap_or_else(|error| panic!("{pgproto_json} should decode: {error}"));
            assert_eq!(decoded, vec![raw]);
        }
    }

    #[::tarantool::test]
    fn a_decimal_without_a_point_gets_one() {
        let ratio: Decimal = f64::try_into(2.0).expect("a decimal");
        assert_eq!(ratio.to_string(), "2");

        let pgproto_json = encode_via_rmp_serde(&vec![IndexOption::RunSizeRatio(ratio)]);
        let decoded: Vec<RawIndexOption> = serde_json::from_str(&pgproto_json)
            .unwrap_or_else(|error| panic!("{pgproto_json} should decode: {error}"));
        assert_eq!(
            decoded,
            vec![RawIndexOption::RunSizeRatio("2".parse().unwrap())]
        );
        assert_eq!(decoded[0].as_pair().1, "2.0");
    }

    #[::tarantool::test]
    fn every_decimal_digit_survives_decoding() {
        // All 28 significant digits rust_decimal holds. A wider value, though
        // legal in the catalog, fails to decode, see `decimal_from_json`.
        let literal = "12345678901234567890.12345678";
        let ratio: Decimal = literal.parse().expect("a decimal literal");

        let pgproto_json = encode_via_rmp_serde(&vec![IndexOption::RunSizeRatio(ratio)]);
        assert!(pgproto_json.contains(literal), "{pgproto_json}");

        let decoded: Vec<RawIndexOption> = serde_json::from_str(&pgproto_json)
            .unwrap_or_else(|error| panic!("{pgproto_json} should decode: {error}"));
        assert_eq!(
            decoded,
            vec![RawIndexOption::RunSizeRatio(literal.parse().unwrap())]
        );
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
        let json = convert_like_pgproto(&tarantool::msgpack::encode(value));
        serde_json::from_str(&json).expect("pgproto renders valid json")
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
    fn an_exponent_form_number_is_rejected() {
        // The decimal parser rejects the exponent form, as does the SQL
        // grammar for a decimal literal. Pgproto never sends one: tarantool
        // prints decimals positionally.
        assert!(serde_json::from_str::<RawIndexOption>(r#"{"run_size_ratio":1e5}"#).is_err());
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
        let pgproto_json = json!([{"unique": true}, {"page_size": 1024}]).to_string();
        assert_eq!(
            encode_via_rmp_serde(&vec![
                IndexOption::Unique(true),
                IndexOption::PageSize(1024)
            ]),
            pgproto_json
        );

        let decoded: Vec<RawIndexOption> =
            serde_json::from_str(&pgproto_json).unwrap_or_else(|error| {
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
            (
                IndexOption::CompressionDict(true),
                RawIndexOption::CompressionDict(true),
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
    fn a_decimal_arrives_as_a_number_holding_its_literal() {
        let decoded: Vec<RawIndexOption> =
            serde_json::from_str(r#"[{"bloom_fpr":0.001},{"run_size_ratio":12345.67890}]"#)
                .expect("decodes");

        assert_eq!(
            decoded,
            vec![
                RawIndexOption::BloomFalsePositiveRate("0.001".parse().unwrap()),
                RawIndexOption::RunSizeRatio("12345.67890".parse().unwrap()),
            ]
        );
    }

    #[test]
    fn a_whole_decimal_is_written_with_a_point() {
        let decoded: Vec<RawIndexOption> =
            serde_json::from_str(r#"[{"run_size_ratio":2},{"bloom_fpr":100}]"#).expect("decodes");

        assert_eq!(
            decoded,
            vec![
                RawIndexOption::RunSizeRatio("2".parse().unwrap()),
                RawIndexOption::BloomFalsePositiveRate("100".parse().unwrap()),
            ]
        );
        let written: Vec<String> = decoded
            .iter()
            .map(|option| option.as_pair().1.to_string())
            .collect();
        assert_eq!(written, ["2.0", "100.0"]);
    }

    #[test]
    fn a_decimal_that_is_not_a_number_is_rejected() {
        for json in [r#"{"bloom_fpr":"0.001"}"#, r#"{"bloom_fpr":true}"#] {
            serde_json::from_str::<RawIndexOption>(json).expect_err("only a number is accepted");
        }
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
