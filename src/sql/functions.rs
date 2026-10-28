//! Scalar SQL functions implemented as stored procedures.
//!
//! The SQL names of the functions are in [`FUNCTION_NAME_MAPPINGS`]. Their
//! signatures for the planner are in `get_builtin_functions` of
//! `sql-executor` and in the type system of `sql-frontend`. The functions
//! are stable: they depend only on their arguments.
//!
//! [`FUNCTION_NAME_MAPPINGS`]: sql::frontend::sql::FUNCTION_NAME_MAPPINGS

use crate::traft::error::Error;
use sql::explain::utils::format_query;
use sql::ir::value::Value;
use tarantool::decimal::Decimal;
use tarantool::tuple::Decode;

////////////////////////////////////////////////////////////////////////////////
// arguments
////////////////////////////////////////////////////////////////////////////////

/// The arguments of a function with one argument, as tarantool SQL passes
/// them to a stored procedure.
pub struct Arg<T>(T);

impl<'de, T: tarantool::msgpack::Decode<'de>> Decode<'de> for Arg<T> {
    fn decode(data: &'de [u8]) -> tarantool::Result<Self> {
        let (value,): (T,) = tarantool::msgpack::decode(data)?;
        Ok(Arg(value))
    }
}

/// A number argument of [`proc_size_pretty`] and [`proc_num_pretty`].
#[derive(Debug, PartialEq)]
enum Number {
    /// The text of the whole part of the number, as `-123`.
    Whole(String),
    /// The text of a decimal below the limit of the smallest unit.
    /// It is printed as it is, as PostgreSQL prints a small `numeric`.
    Small(String),
}

/// Converts `value` to a [`Number`]. A decimal is [`Number::Small`] when its
/// absolute value is below `small_limit`, and its whole part otherwise. `None`
/// is SQL `NULL`.
fn number_arg(value: Value, function: &str, small_limit: i64) -> Result<Option<Number>, Error> {
    let number = match value {
        Value::Null => return Ok(None),
        Value::Integer(n) => Number::Whole(n.to_string()),
        Value::Decimal(d) => {
            if d.abs() < Decimal::from(small_limit) {
                Number::Small(d.to_string())
            } else {
                Number::Whole(whole_part(&d.to_string()))
            }
        }
        other => {
            return Err(Error::other(format!(
                "{function}: expected a number, got {other}"
            )))
        }
    };
    Ok(Some(number))
}

/// The whole part of the decimal number `text`, as `-123` for `-123.9` or
/// `1000` for `1E+3`. The fraction is dropped, so the part is truncated toward
/// zero, as in PostgreSQL.
fn whole_part(text: &str) -> String {
    let (sign, unsigned) = text.split_at(usize::from(text.starts_with('-')));
    let (mantissa, exp) = unsigned
        .split_once(['e', 'E'])
        .map_or((unsigned, 0), |(mantissa, exp)| {
            (mantissa, exp.parse().expect("the exponent is a number"))
        });
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = format!("{whole}{fraction}");
    // The number of digits before the decimal point.
    let point = whole.len() as i64 + exp;
    let whole = if point <= 0 {
        String::new()
    } else if point as usize >= digits.len() {
        format!("{digits}{}", "0".repeat(point as usize - digits.len()))
    } else {
        digits[..point as usize].to_owned()
    };
    match whole.trim_start_matches('0') {
        "" => "0".to_owned(),
        whole => format!("{sign}{whole}"),
    }
}

/// Divides the whole number `digits` by `divisor`. Gives the quotient without
/// zeros at the start and the remainder.
fn divide_digits(digits: &str, divisor: u64) -> (String, u64) {
    let mut quotient = String::with_capacity(digits.len());
    let mut remainder = 0_u64;
    for digit in digits.bytes() {
        let current = remainder * 10 + u64::from(digit - b'0');
        let q = current / divisor;
        if !quotient.is_empty() || q != 0 {
            quotient.push(char::from(b'0' + q as u8));
        }
        remainder = current % divisor;
    }
    if quotient.is_empty() {
        quotient.push('0');
    }
    (quotient, remainder)
}

/// Adds 1 to the whole number `digits`.
fn increment_digits(digits: &str) -> String {
    let mut digits = digits.as_bytes().to_vec();
    for digit in digits.iter_mut().rev() {
        if *digit == b'9' {
            *digit = b'0';
        } else {
            *digit += 1;
            return String::from_utf8(digits).expect("the digits are ASCII");
        }
    }
    digits.insert(0, b'1');
    String::from_utf8(digits).expect("the digits are ASCII")
}

////////////////////////////////////////////////////////////////////////////////
// pico_size_pretty
////////////////////////////////////////////////////////////////////////////////

/// A unit of [`size_pretty`], as in `pg_size_pretty` of PostgreSQL.
struct SizeUnit {
    name: &'static str,
    /// The unit is used when the absolute size in this unit is below the
    /// limit. In a unit with rounding, the size is still doubled here.
    limit: u128,
    /// Round the size to the nearest whole, halves away from zero.
    round: bool,
    /// The unit is `2^bits` bytes.
    bits: u32,
}

const SIZE_UNITS: [SizeUnit; 6] = [
    SizeUnit {
        name: "bytes",
        limit: 10 * 1024,
        round: false,
        bits: 0,
    },
    SizeUnit {
        name: "kB",
        limit: 20 * 1024 - 1,
        round: true,
        bits: 10,
    },
    SizeUnit {
        name: "MB",
        limit: 20 * 1024 - 1,
        round: true,
        bits: 20,
    },
    SizeUnit {
        name: "GB",
        limit: 20 * 1024 - 1,
        round: true,
        bits: 30,
    },
    SizeUnit {
        name: "TB",
        limit: 20 * 1024 - 1,
        round: true,
        bits: 40,
    },
    SizeUnit {
        name: "PB",
        limit: 20 * 1024 - 1,
        round: true,
        bits: 50,
    },
];

/// Formats the size in bytes `size`, the text of a whole number, as
/// [`size_pretty_i128`] does.
fn size_pretty(size: &str) -> String {
    if let Ok(size) = size.parse() {
        return size_pretty_i128(size);
    }
    // The size is larger than an `i128`, so it is in the last unit. The
    // divisions into the last unit truncate, so together they are one
    // division by `2^49`. The rounding then halves the result: this is a
    // division by `2^50`, with 1 more if the remainder is at least `2^49`.
    let (sign, digits) = size.split_at(usize::from(size.starts_with('-')));
    let last = SIZE_UNITS.last().expect("there are units");
    let (quotient, remainder) = divide_digits(digits, 1 << last.bits);
    let quotient = if remainder >= 1 << (last.bits - 1) {
        increment_digits(&quotient)
    } else {
        quotient
    };
    format!("{sign}{quotient} {}", last.name)
}

/// Formats a size in bytes with the largest unit that keeps the number
/// at least 10. The algorithm is the one of `pg_size_pretty`, so the results
/// are the same. Each division truncates, and the division into a unit with
/// rounding keeps one more bit, which the rounding then drops.
fn size_pretty_i128(mut size: i128) -> String {
    for (i, unit) in SIZE_UNITS.iter().enumerate() {
        match SIZE_UNITS.get(i + 1) {
            Some(next) if size.unsigned_abs() >= unit.limit => {
                let bits = next.bits - unit.bits - u32::from(next.round) + u32::from(unit.round);
                size /= 1 << bits;
            }
            _ => {
                if unit.round {
                    size = half_rounded(size);
                }
                return format!("{size} {}", unit.name);
            }
        }
    }
    unreachable!("the last unit has no next unit")
}

/// `x / 2`, rounded to the nearest whole, halves away from zero.
fn half_rounded(x: i128) -> i128 {
    (x + if x < 0 { -1 } else { 1 }) / 2
}

/// `pico_size_pretty(size)`: the size in bytes as text with a unit, as
/// `pg_size_pretty` of PostgreSQL makes it.
#[tarantool::proc(packed_args)]
pub fn proc_size_pretty(Arg(size): Arg<Value>) -> Result<Option<String>, Error> {
    let Some(size) = number_arg(size, "pico_size_pretty", SIZE_UNITS[0].limit as i64)? else {
        return Ok(None);
    };
    Ok(Some(match size {
        Number::Whole(size) => size_pretty(&size),
        Number::Small(size) => format!("{size} {}", SIZE_UNITS[0].name),
    }))
}

////////////////////////////////////////////////////////////////////////////////
// pico_num_pretty
////////////////////////////////////////////////////////////////////////////////

/// The suffixes of [`num_pretty`]: each one is 1000 times the previous.
const NUM_SUFFIXES: &[u8] = b"kMGTPEZY";

/// A number below this limit is printed as it is.
const NUM_LIMIT: i64 = 1000;

/// Formats the number `n`, the text of a whole number, as `llvm-cov` formats
/// counts: the three first digits, with a decimal point after the whole part
/// in the suffix. The other digits are dropped, not rounded. Above the last
/// suffix, the whole part gets longer.
fn num_pretty(n: &str) -> String {
    let (sign, digits) = n.split_at(usize::from(n.starts_with('-')));
    if digits.len() <= 3 {
        return n.to_owned();
    }
    let group = ((digits.len() - 1) / 3).min(NUM_SUFFIXES.len());
    let suffix = char::from(NUM_SUFFIXES[group - 1]);
    let whole_len = digits.len() - 3 * group;
    if whole_len >= 3 {
        format!("{sign}{}{suffix}", &digits[..whole_len])
    } else {
        format!(
            "{sign}{}.{}{suffix}",
            &digits[..whole_len],
            &digits[whole_len..3]
        )
    }
}

/// `pico_num_pretty(n)`: the number as text with a suffix (`k` is 1000, `M`
/// is 1000000 and so on), as `llvm-cov` prints counts.
#[tarantool::proc(packed_args)]
pub fn proc_num_pretty(Arg(n): Arg<Value>) -> Result<Option<String>, Error> {
    let Some(n) = number_arg(n, "pico_num_pretty", NUM_LIMIT)? else {
        return Ok(None);
    };
    Ok(Some(match n {
        Number::Whole(n) => num_pretty(&n),
        Number::Small(n) => n,
    }))
}

////////////////////////////////////////////////////////////////////////////////
// pico_sql_pretty
////////////////////////////////////////////////////////////////////////////////

/// `pico_sql_pretty(query)`: the query formatted as `EXPLAIN (RAW, FMT)` shows
/// the queries of the plan.
#[tarantool::proc(packed_args)]
pub fn proc_sql_pretty(Arg(query): Arg<Option<String>>) -> Option<String> {
    query.map(|query| format_query(&query, &[] as &[String], true))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The results of `pg_size_pretty(bigint)` of PostgreSQL 18.
    #[test]
    fn size_pretty_is_pg_size_pretty() {
        let cases: &[(i128, &str)] = &[
            (0, "0 bytes"),
            (1, "1 bytes"),
            (-1, "-1 bytes"),
            (1023, "1023 bytes"),
            (1024, "1024 bytes"),
            (10239, "10239 bytes"),
            (10240, "10 kB"),
            (-10240, "-10 kB"),
            (10751, "10 kB"),
            (10752, "11 kB"),
            (10753, "11 kB"),
            (-10752, "-11 kB"),
            (-10753, "-11 kB"),
            (20479, "20 kB"),
            (20480, "20 kB"),
            (1048575, "1024 kB"),
            (10485247, "10239 kB"),
            (10485248, "10 MB"),
            (10485760, "10 MB"),
            (20970495, "20 MB"),
            (20971520, "20 MB"),
            (1073741824, "1024 MB"),
            (1099511627776, "1024 GB"),
            (1125899906842624, "1024 TB"),
            (1152921504606846976, "1024 PB"),
            (9223372036854775807, "8192 PB"),
            (-9223372036854775808, "-8192 PB"),
            // `pg_size_pretty(numeric)`.
            (123456789012345678901234567890, "109651655766237 PB"),
            (
                -99999999999999999999999999999999999999,
                "-88817841970012523233891 PB",
            ),
        ];
        for (size, expected) in cases {
            assert_eq!(size_pretty(&size.to_string()), *expected, "{size}");
        }
    }

    /// The results of `pg_size_pretty(numeric)` of PostgreSQL for sizes that
    /// are larger than an `i128`.
    #[test]
    fn size_pretty_above_i128() {
        let cases: &[(&str, &str)] = &[
            (
                "170141183460469231731687303715884105728",
                "151115727451828646838272 PB",
            ),
            (
                "-170141183460469231731687303715884105728",
                "-151115727451828646838272 PB",
            ),
            (
                "12345678901234567890123456789012345678901234567890",
                "10965165576623696965906390933487154 PB",
            ),
            // The remainder of the division by 2^50 is 2^49 and 2^49 - 1.
            (
                "1125899906842624000000000000000562949953421312",
                "1000000000000000000000000000001 PB",
            ),
            (
                "1125899906842624000000000000000562949953421311",
                "1000000000000000000000000000000 PB",
            ),
            (
                "-1125899906842624000000000000000562949953421312",
                "-1000000000000000000000000000001 PB",
            ),
            // The rounding carries to a new digit.
            (
                "1125899906842623999999999999999437050046578688",
                "1000000000000000000000000000000 PB",
            ),
        ];
        for (size, expected) in cases {
            assert_eq!(size_pretty(size), *expected, "{size}");
        }
    }

    /// The whole part of a decimal is truncated toward zero.
    #[test]
    fn whole_part_of_decimal() {
        let cases: &[(&str, &str)] = &[
            ("0", "0"),
            ("123", "123"),
            ("123.9", "123"),
            ("-123.9", "-123"),
            ("-0.5", "0"),
            ("0.000", "0"),
            ("1E+3", "1000"),
            ("1.5E+3", "1500"),
            ("-1.23456E+2", "-123"),
            ("1E-3", "0"),
            ("12345E-2", "123"),
            ("1E+40", "10000000000000000000000000000000000000000"),
        ];
        for (text, expected) in cases {
            assert_eq!(whole_part(text), *expected, "{text}");
        }
    }

    /// The counts of `llvm-cov show` of LLVM 23.
    #[test]
    fn num_pretty_is_llvm_cov() {
        let cases: &[(i128, &str)] = &[
            (0, "0"),
            (999, "999"),
            (1000, "1.00k"),
            (1234, "1.23k"),
            (9999, "9.99k"),
            (12345, "12.3k"),
            (99999, "99.9k"),
            (100000, "100k"),
            (123456, "123k"),
            (999999, "999k"),
            (1000000, "1.00M"),
            (1234567, "1.23M"),
            (12345678, "12.3M"),
            (123456789, "123M"),
            (1234567890, "1.23G"),
        ];
        for (n, expected) in cases {
            assert_eq!(num_pretty(&n.to_string()), *expected, "{n}");
        }
    }

    /// Negative numbers and numbers above the last suffix of `llvm-cov`.
    #[test]
    fn num_pretty_beyond_llvm_cov() {
        let cases: &[(i128, &str)] = &[
            (-1, "-1"),
            (-999, "-999"),
            (-1000, "-1.00k"),
            (-12345, "-12.3k"),
            (i64::MAX as i128, "9.22E"),
            (i64::MIN as i128, "-9.22E"),
            (10_i128.pow(24), "1.00Y"),
            (10_i128.pow(26) * 9, "900Y"),
            (10_i128.pow(27), "1000Y"),
            (i128::MAX, "170141183460469Y"),
        ];
        for (n, expected) in cases {
            assert_eq!(num_pretty(&n.to_string()), *expected, "{n}");
        }
        assert_eq!(
            num_pretty("-10000000000000000000000000000000000000000"),
            "-10000000000000000Y"
        );
    }
}
