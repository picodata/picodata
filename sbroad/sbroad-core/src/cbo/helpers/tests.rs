use crate::cbo::histogram::Scalar;
use tarantool::decimal;
use tarantool::decimal::Decimal;

#[test]
fn values_scale() {
    let decimal_epsilon = Decimal::try_from(f32::EPSILON).unwrap();

    // Numerical
    assert!(
        decimal!(0.5)
            - decimal!(1)
                .boundaries_occupied_fraction(&decimal!(0), &decimal!(2))
                .unwrap()
            < decimal_epsilon
    );

    assert!(
        decimal!(0.1) - 1u64.boundaries_occupied_fraction(&0u64, &10u64).unwrap() < decimal_epsilon
    );

    assert!(
        decimal!(0.1) - 1i64.boundaries_occupied_fraction(&0i64, &10i64).unwrap() < decimal_epsilon
    );

    assert!(
        decimal!(0.0) - 1i64.boundaries_occupied_fraction(&1i64, &3i64).unwrap() < decimal_epsilon
    );

    assert!(
        decimal!(0.3333333333) - 1i64.boundaries_occupied_fraction(&0i64, &3i64).unwrap()
            < decimal_epsilon
    );

    // Strings
    // One of the passed strings is empty.
    assert_eq!(
        "invalid value: One of the passed strings is empty",
        String::from("")
            .boundaries_occupied_fraction(&String::from(""), &String::from(""))
            .unwrap_err()
            .to_string()
    );

    assert_eq!(
        "invalid value: One of the passed strings is empty",
        String::from("")
            .boundaries_occupied_fraction(&String::from("a"), &String::from("b"))
            .unwrap_err()
            .to_string()
    );

    // One-letter.
    //
    // Logic:
    // 1.) As soon as only lower case letters are used
    // 1.1) low_bound  =  'a' <-> 97
    // 1.2) high_bound =  'z' <-> 122
    // 2.) No chars are removed as common prefix.
    // 3.) Conversion process is made on assumption that base = 26
    // 3.1) value_f64       = (98 - 97) / 26 = 0.038461538461538464
    // 3.2) left_bound_f64  = (97 - 97) / 26 = 0.0
    // 3.3) right_bound_f64 = (99 - 97) / 26 = 0.07692307692307693
    // 4.) result = (0.038461538461538464) / (0.07692307692307693) = 0.5
    assert!(
        (decimal!(0.5)
            - String::from("b")
                .boundaries_occupied_fraction(&String::from("a"), &String::from("c"))
                .unwrap())
            < decimal_epsilon
    );

    // Two-letter.
    //
    // Logic:
    // <The same calculations as in the example above>.
    assert!(
        (decimal!(0.5)
            - String::from("ba")
                .boundaries_occupied_fraction(&String::from("aa"), &String::from("ca"))
                .unwrap())
            < decimal_epsilon
    );

    // Telephone-number.
    //
    // Logic:
    // 1.) As soon as only numbers are used
    // 1.1) low_bound  =  '0' <-> 48
    // 1.2) high_bound =  '9' <-> 57
    // 2.) First 4 common chars are removed as common prefix.
    // 3.) Conversion process is made on assumption that base = 10
    // 3.1) value_f64       = (49 - 48) / 10 + (53 - 48) / 100 = 0.15000000000000002
    // 3.2) left_bound_f64  = (49 - 48) / 10 = 0.10000000000000001
    // 3.3) right_bound_f64 = (50 - 48) / 10 = 0.20000000000000001
    // 4.) result = (0.05000000000000001) / (0.1) =~ 0.5
    assert!(
        (decimal!(0.5000000000000001)
            - String::from("891215") // "15" after prefix removal.
                .boundaries_occupied_fraction(
                    &String::from("891210"), // "10" after prefix removal.
                    &String::from("891220")  // "20" after prefix removal.
                )
                .unwrap())
            < decimal_epsilon
    );

    // ASCII chars that made the base equal to odd number.
    //
    // Logic:
    // 1.) As soon as only lower case letters are used
    // 1.1) low_bound  =  '/' <-> 47
    // 1.2) high_bound =  '9' <-> 57
    // 2.) No chars are removed as common prefix.
    // 3.) Conversion process is made on assumption that base = 11
    // 3.1) value_f64       = (52 - 47) / 11 = 0.4545454545454546
    // 3.2) left_bound_f64  = (47 - 47) / 11 = 0.0
    // 3.3) right_bound_f64 = (57 - 47) / 11 = 0.9090909090909092
    // 4.) result = (0.4545454545454546) / (0.9090909090909092) = 0.5
    assert!(
        (decimal!(0.5)
            - String::from("4")
                .boundaries_occupied_fraction(&String::from("/"), &String::from("9"))
                .unwrap())
            < decimal_epsilon
    );

    // Non-ASCII, russian chars.
    assert!(
        (decimal!(0.6938775510204102)
            - String::from("р")
                .boundaries_occupied_fraction(&String::from("а"), &String::from("я"))
                .unwrap())
            < decimal_epsilon
    );

    // Non-ASCII, russian char sequence.
    //
    // Logic:
    // <The same calculations as in the example above>.
    assert!(
        (decimal!(0.6938775510204102)
            - String::from("пикр")
                .boundaries_occupied_fraction(&String::from("пика"), &String::from("пикя"))
                .unwrap())
            < decimal_epsilon
    );

    // Seems okay that sequence of "ж" letters lies nearer to "а" sequence than to "я" sequence.
    assert!(
        (decimal!(0.041379310344835614)
            - String::from("жжж жжж жжж")
                .boundaries_occupied_fraction(
                    &String::from("ааа ааа ааа"),
                    &String::from("яяя яяя яяя")
                )
                .unwrap())
            < decimal_epsilon
    );

    // In reverse.
    // Seems okay that sequence of "э" letters lies nearer to "я" sequence than to "а" sequence.
    assert!(
        (decimal!(0.9862068965517115)
            - String::from("эээ эээ эээ")
                .boundaries_occupied_fraction(
                    &String::from("ааа ааа ааа"),
                    &String::from("яяя яяя яяя")
                )
                .unwrap())
            < decimal_epsilon
    );

    // Emoji test.
    // Seems okay that very happy face lies nearer to happy face than to angry face.
    assert!(
        (decimal!(0.1212121212121212)
            - String::from("😄")
                .boundaries_occupied_fraction(&String::from("😀"), &String::from("😡"))
                .unwrap())
            < decimal_epsilon
    );
}
