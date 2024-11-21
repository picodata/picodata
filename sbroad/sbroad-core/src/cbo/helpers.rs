use crate::errors::{Entity, SbroadError};
use crate::ir::value::double::Double;
use itertools::enumerate;
use smol_str::{format_smolstr, SmolStr};
use tarantool::decimal;
use tarantool::decimal::Decimal;

// -------------------------Bucket fraction (STARTS)------------------------------------------------
// Calculation of fraction that value takes in a histogram bucket.
// [++++++++++++++++&-------------------------]
// `left_bound`   `value`                  `right_bound`
//
// We assume here that only two value type categories are available for scaling:
// 1.) Strings   (that are scaled using `scale_strings` function)
// 2.) Numerical (that are scaled being casted to `Decimal`s)
//
// `PostgreSQL` lines: `convert_to_scalar`, lines 4275-4461.

/// Helper structure that represents `String` byte sequence.
#[derive(Default, Debug, Clone)]
struct Bytes<'bytes> {
    inner: &'bytes [u8],
}

impl<'bytes> Bytes<'bytes> {
    /// Len of `inner` sequence.
    fn len(&self) -> usize {
        self.inner.len()
    }

    /// Min byte stored in `inner`.
    ///
    /// Returns None in case `inner` is empty.
    fn min(&self) -> Option<&u8> {
        self.inner.iter().min()
    }

    /// Max byte stored in `inner`.
    ///
    /// Returns None in case `inner` is empty.
    fn max(&self) -> Option<&u8> {
        self.inner.iter().max()
    }

    /// Helper function that converts `Bytes` into `f64` value from 0 to 1
    /// considering given range bounds.
    /// It works with `Bytes` subrange starting from the `from` index.
    ///
    /// It applies some kind of a reverse conversion algorithm.
    /// E.g. straight algorithm for usual ASCII string "cba" with the base of 255 (from 0 to 255)
    /// would traverse letters from right to left in a way like:
    /// 1.) "a" equals to 97. result += 97 * 255^1
    /// 2.) "b" equals to 98. result += 98 * 255^2
    /// 3.) "c" equals to 99. result += 99 * 255^3
    ///
    /// In this case we traverse the string in reverse order, every time decreasing the base and
    /// counting which part of the base range does our current byte takes
    /// (subtracting the value from the `low_bound`):
    /// 1.) "c" equals to 99, the working range is [97, 122] so result += (99 - 97) / base^1
    /// 2.) "b" equals to 98, the working range is [97, 122] so result += (98 - 97) / base^2
    /// 3.) "a" equals to 97, the working range is [97, 122] so result += (97 - 97) / base^3
    ///
    /// Check the largest case (where all bytes from the given byte sequence equal to the
    /// `high_bound` value) in order to show that the resulting value is less than 1
    /// (here `base` considered to be just `high_bound - low_bound`):
    /// 1.) `(high_bound - low_bound) / (base + 1)     = base / (base + 1)`
    /// 2.) `(high_bound - low_bound) / (base + 1) ^ 2 = base / (base + 1)^2`
    /// ...
    /// `result = (base / (base + 1)) * (1 + 1/(base+1) + 1/(base+1)^2 + ...)`
    /// considering the fact that `sum of 1 / ((base + 1)^j), j = 0 to infinity = (base + 1) / base`
    /// we get that `result <= 1`
    ///
    /// `PostgreSQL` lines: `convert_one_string_to_scalar`, lines 4564-4603.
    fn convert_bytes_to_decimal_from(
        &self,
        from: usize,
        low_bound: u8,
        high_bound: u8,
    ) -> Result<Decimal, SbroadError> {
        let base = Decimal::from(high_bound - low_bound + 1);
        let mut result = decimal!(0.0);

        for (index, byte) in enumerate(self.inner.iter().skip(from).take(12)) {
            // We check that given `byte` is in found bounds.
            // If somehow (in case we broaden our range from 0 to 127)
            // it's not, we made it equal to bound.
            let constrained_byte = byte.clamp(&low_bound, &high_bound);

            let power_base = i64::try_from(index + 1).map_err(|_| {
                SbroadError::Invalid(
                    Entity::Value,
                    Some(SmolStr::from("Unable to cast index to i64")),
                )
            })?;
            let power = Decimal::from(-power_base);
            let base_shift = Decimal::pow(base, power).ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Statistics,
                    Some(format_smolstr!(
                        "Unable to raise a Decimal {base} to chosen power {power}"
                    )),
                )
            })?;
            result += Decimal::from(constrained_byte - low_bound) * base_shift;
        }
        Ok(result)
    }
}

impl<'bytes> From<&'bytes String> for Bytes<'bytes> {
    fn from(s: &'bytes String) -> Self {
        Self {
            inner: s.as_bytes(),
        }
    }
}

/// Helper structure that represents several `String`s packed in a vector.
#[derive(Debug)]
struct VecOfBytes<'bytes_vec> {
    vec: Vec<&'bytes_vec Bytes<'bytes_vec>>,
}

impl<'bytes_vec> VecOfBytes<'bytes_vec> {
    fn new(bytes_vec: Vec<&'bytes_vec Bytes>) -> Self {
        VecOfBytes { vec: bytes_vec }
    }

    /// Find the shortest bytes sequence len in `vec`.
    fn min_len(&self) -> usize {
        self.vec
            .iter()
            .map(|x| x.len())
            .min()
            .map_or_else(|| 0, |l| l)
    }

    /// Find the smallest `u8` value among all the `Bytes`s stored in `vec`.
    /// Returns `None` in case every element of `vec` is empty.
    fn low_bound(&self) -> Option<u8> {
        self.vec
            .iter()
            .map(|x| x.min())
            .fold(None, |mut acc, x| {
                if let Some(min) = x {
                    if let Some(value) = acc {
                        if min < value {
                            acc = Some(min);
                        }
                    } else {
                        acc = Some(min);
                    }
                }
                acc
            })
            .copied()
    }

    /// Find the greatest `u8` value among all the `Bytes`s stored in `vec`.
    /// Returns `None` in case every element of `vec` is empty.
    fn high_bound(&self) -> Option<u8> {
        self.vec
            .iter()
            .map(|x| x.max())
            .fold(None, |mut acc, x| {
                if let Some(max) = x {
                    if let Some(value) = acc {
                        if max > value {
                            acc = Some(max);
                        }
                    } else {
                        acc = Some(max);
                    }
                }
                acc
            })
            .copied()
    }

    fn iter(&self) -> Iter {
        Iter {
            inner: self,
            min_len: self.min_len(),
            index: 0,
        }
    }

    /// Find the greatest common prefix among all `Bytes` stored in `vec`.
    fn common_bytes_len(&self) -> usize {
        self.iter().count()
    }
}

/// Helper structure that represents iterator over `VecOfBytes`.
/// Used in order to calculate longest common prefix of `String`s packed into `inner`.
struct Iter<'bytes_vec> {
    inner: &'bytes_vec VecOfBytes<'bytes_vec>,
    min_len: usize,
    index: usize,
}

impl<'bytes_vec> Iterator for Iter<'bytes_vec> {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.min_len {
            return None;
        }

        let mut byte_value: Option<u8> = None;
        for bytes in &self.inner.vec {
            match bytes.inner.get(self.index) {
                Some(byte) => {
                    if byte_value.is_none() {
                        byte_value = Some(*byte);
                    } else if byte_value != Some(*byte) {
                        return None;
                    }
                }
                None => return None,
            }
        }

        self.index += 1;

        byte_value
    }
}

/// Helper function used to broaden working space bounds.
/// We presume that in case [`low_bound`; `high_bound`] range cover some part of [`low_byte`; `high_byte`]
/// range, then it should include it fully.
fn broaden_bounds(low_bound: &mut u8, high_bound: &mut u8, low_byte: u8, high_byte: u8) {
    if *low_bound <= high_byte && *high_bound >= low_byte {
        if *low_bound > low_byte {
            *low_bound = low_byte;
        }
        if *high_bound < high_byte {
            *high_bound = high_byte;
        }
    }
}

/// All the function main logic presumes that we are working with ASCII characters,
/// 1-byte characters that are represented with `u8` values from 0 to 255.
/// We transform given strings into such ASCII arrays and convert them into `f64` from 0 to 1.
///
/// **Note**: calculation of scale is finished in `scale_values`.
///
/// `PostgreSQL` lines: `convert_string_to_scalar`, lines 4484-4561.
///
/// # Errors
/// - One of passed strings is empty.
/// - Conversion to Decimal failed.
#[allow(dead_code)]
pub fn scale_strings(
    value: &String,
    left_bound: &String,
    right_bound: &String,
) -> Result<(Decimal, Decimal, Decimal), SbroadError> {
    if value.is_empty() || left_bound.is_empty() || right_bound.is_empty() {
        return Err(SbroadError::Invalid(
            Entity::Value,
            Some(SmolStr::from("One of the passed strings is empty")),
        ));
    }

    let value_bytes = Bytes::from(value);
    let left_bound_bytes = Bytes::from(left_bound);
    let right_bound_bytes = Bytes::from(right_bound);
    let vec_of_byte_sequences =
        VecOfBytes::new(vec![&value_bytes, &left_bound_bytes, &right_bound_bytes]);

    // As soon as not all strings contain all ASCII characters from 0 to 255,
    // we want to reduce the range (base) that will be used for scaling.
    let low_bound = vec_of_byte_sequences.low_bound();
    let high_bound = vec_of_byte_sequences.high_bound();

    let (Some(mut low_bound), Some(mut high_bound)) = (low_bound, high_bound) else {
        return Err(SbroadError::Invalid(
            Entity::Value,
            Some(SmolStr::from("One of the passed strings is empty")),
        ));
    };

    broaden_bounds(&mut low_bound, &mut high_bound, b'0', b'9');
    broaden_bounds(&mut low_bound, &mut high_bound, b'A', b'Z');
    broaden_bounds(&mut low_bound, &mut high_bound, b'a', b'z');

    // In case our range is too small, we broaden it to the regular ASCII set.
    //
    // **Note**: In case our given values are smth like 230 (value), 235 and 240 (bounds)
    // the range will become [0; 127] and during `convert_bytes_to_decimal_from` all the values will
    // just be mapped to `high_bound` = 127. Nothing bad will happen.
    if high_bound - low_bound < 9 {
        low_bound = 0;
        high_bound = 127;
    }

    // We want to remove the longest common prefix from all of the input strings in order to
    // reduce the working bytes space.
    let greatest_common_prefix_length = vec_of_byte_sequences.common_bytes_len();

    // Convert strings with removed prefixes to `f64`.
    let value_decimal = value_bytes.convert_bytes_to_decimal_from(
        greatest_common_prefix_length,
        low_bound,
        high_bound,
    )?;
    let low_bound_decimal = left_bound_bytes.convert_bytes_to_decimal_from(
        greatest_common_prefix_length,
        low_bound,
        high_bound,
    )?;
    let high_bound_decimal = right_bound_bytes.convert_bytes_to_decimal_from(
        greatest_common_prefix_length,
        low_bound,
        high_bound,
    )?;

    Ok((value_decimal, low_bound_decimal, high_bound_decimal))
}

/// Helper function to convert `Double` into `f64` for selectivity estimation purposes
/// (see `boundaries_occupied_fraction` implementation for `Double`).
///
/// `PostgreSQL` lines: `numeric_float8_no_overflow`, lines 4459-4482.
#[must_use]
pub fn clamp_double(value: &Double) -> f64 {
    let inner_value = value.value;
    let is_special = |f: f64| f.is_nan() || f.is_infinite();
    if is_special(inner_value) {
        if inner_value == f64::INFINITY {
            f64::MAX
        } else if inner_value == f64::NEG_INFINITY {
            f64::MIN
        } else {
            f64::NAN
        }
    } else {
        inner_value
    }
}

/// Helper function for `Scalar` trait `find_boundaries_occupied_fraction` function
/// for finding f.
///
/// # Errors
/// - Boundaries are not located in the right order.
pub fn decimal_boundaries_occupied_fraction(
    value: Decimal,
    left_boundary: Decimal,
    right_boundary: Decimal,
) -> Result<Decimal, SbroadError> {
    // Check that converted values are located adequately.
    if right_boundary < left_boundary {
        Err(
            SbroadError::Invalid(
                Entity::Value,
                Some(format_smolstr!(
                    "Converted high_bound {right_boundary} must be greater than converted low_bound {left_boundary}")
                ),
            )
        )
    } else if value < left_boundary || value > right_boundary {
        Err(
            SbroadError::Invalid(
                Entity::Value,
                Some(format_smolstr!(
                    "Converted value {value} must lie between converted low_bound {left_boundary} and converted high_bound {right_boundary}")
                ),
            )
        )
    } else {
        Ok((value - left_boundary) / (right_boundary - left_boundary))
    }
}
// -------------------------Bucket fraction (ENDS)--------------------------------------------------

#[cfg(test)]
mod tests;
