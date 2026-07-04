use super::*;
use pretty_assertions::assert_eq;

#[test]
fn test_bucket_id_by_str() {
    assert_eq!(str_to_bucket_id("100тесты", 30000), 17339);

    assert_eq!(
        str_to_bucket_id(
            "4TEST5501605647472000000100000000d9\
            2beee8-749f-4539-aa15-3d2941dbb0f1x32https://google.com",
            30000
        ),
        13815
    );

    assert_eq!(str_to_bucket_id("1123", 30000), 360);

    assert_eq!(str_to_bucket_id("daymonthyear", 30000), 3333);
}

#[test]
fn test_zero_bucket_id() {
    assert_eq!(str_to_bucket_id("18810", 30000), 1);
}
