use super::*;
use pretty_assertions::assert_eq;

#[test]
#[allow(clippy::excessive_precision)]
fn equivalence() {
    // Yes, this is correct (checked on Tarantool).
    assert_eq!(
        Double::from(0.999_999_999_999_999_7e-308_f64),
        Double::from(1e-308_f64)
    );
    assert_ne!(
        Double::from(0.999_999_999_999_999_6e-308_f64),
        Double::from(0.999_999_999_999_999_7e-308_f64)
    );
    // Yes, this is correct (checked on Tarantool).
    assert_eq!(
        Double::from(0.999_999_999_999_999_5e-308_f64),
        Double::from(0.999_999_999_999_999_6e-308_f64)
    );
    assert_eq!(
        Double::from(0.999_999_999_999_999_5e-308_f64),
        "0.9999999999999995e-308".parse::<Double>().unwrap()
    );
    assert_eq!(
        Double::from(9_223_372_036_854_775_807_f64),
        "9223372036854775807".parse::<Double>().unwrap()
    );
    assert_eq!(
        Double::from(18_446_744_073_709_551_615_u64),
        "18446744073709551615".parse::<Double>().unwrap()
    );
    assert_eq!(
        Double::from(f64::INFINITY),
        "inf".parse::<Double>().unwrap()
    );
    assert_eq!(
        Double::from(f64::INFINITY),
        "INF".parse::<Double>().unwrap()
    );
    assert_eq!(
        Double::from(f64::INFINITY),
        "+INF".parse::<Double>().unwrap()
    );
    assert_eq!(
        Double::from(f64::NEG_INFINITY),
        "-inf".parse::<Double>().unwrap()
    );
    assert_eq!(
        Double::from(f64::NEG_INFINITY),
        "-INF".parse::<Double>().unwrap()
    );
    assert_eq!(true, "hello".parse::<Double>().is_err());
}
