use tarantool::test::TestCase;

#[tarantool::test]
pub fn just_works() {
    assert!(true);
}

#[tarantool::test(should_panic)]
pub fn should_panic() {
    assert!(false);
}

#[::linkme::distributed_slice]
static TEST_ATTR_SECTION: [TestCase] = [..];

#[tarantool::test(section = "crate::test_attr::TEST_ATTR_SECTION")]
pub fn with_custom_section() {
    let test_names = TEST_ATTR_SECTION.iter().collect::<Vec<_>>();
    assert_eq!(
        test_names,
        [&TestCase::new(
            "tarantool_module_test_runner::test_attr::with_custom_section",
            with_custom_section,
            false,
        )]
    )
}
