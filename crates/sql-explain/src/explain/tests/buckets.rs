use super::explain;
use crate::explain::buckets::{buckets_repr, BoundedBuckets};
use pretty_assertions::assert_eq;
use sql_ir::collection;
use sql_ir::ir::bucket::Buckets;

// Estimation.

#[test]
fn any_for_constant_query() {
    let sql = r#"explain (buckets) select 1"#;
    insta::assert_snapshot!(explain(sql), @"buckets = any");
}

#[test]
fn upper_bound_for_full_scan() {
    let sql = r#"explain (buckets) select e from t2"#;
    insta::assert_snapshot!(explain(sql), @"buckets <= [1-10000]");
}

#[test]
fn exact_for_inserted_row() {
    let sql = r#"explain (buckets) insert into t1 values ('1', 1)"#;
    insta::assert_snapshot!(explain(sql), @"buckets = [6691]");
}

#[test]
fn delete_all_is_upper_bound() {
    let sql = r#"explain (buckets) delete from t2"#;
    insta::assert_snapshot!(explain(sql), @"buckets <= [1-10000]");
}

// Rendering.

#[test]
fn repr_exact_set() {
    let bc = 3000;
    assert_eq!("[1-3000]", buckets_repr(&Buckets::All, bc, false));
    assert_eq!("any", buckets_repr(&Buckets::Any, bc, false));
    assert_eq!(
        "[1-3]",
        buckets_repr(&Buckets::new_filtered(collection!(1, 2, 3)), bc, false)
    );
    assert_eq!(
        "[1-3]",
        buckets_repr(&Buckets::new_filtered(collection!(3, 2, 1)), bc, false)
    );
    assert_eq!(
        "[1, 2]",
        buckets_repr(&Buckets::new_filtered(collection!(1, 2)), bc, false)
    );
    assert_eq!(
        "[1, 10, 11, 21-23]",
        buckets_repr(
            &Buckets::new_filtered(collection!(1, 10, 11, 23, 22, 21)),
            bc,
            false
        )
    );
    assert_eq!(
        "[]",
        buckets_repr(&Buckets::new_filtered(collection!()), bc, false)
    );
}

// FMT option.

#[test]
fn fmt_wraps_long_list() {
    let bc = 3000;
    assert_eq!("[1-3000]", buckets_repr(&Buckets::All, bc, true));
    assert_eq!("any", buckets_repr(&Buckets::Any, bc, true));
    assert_eq!(
        "[]",
        buckets_repr(&Buckets::new_filtered(collection!()), bc, true)
    );

    // Lists that fit into a single line are left as is.
    let eight = collection!(219, 626, 799, 1410, 1860, 1934, 1958, 2564);
    assert_eq!(
        "[219, 626, 799, 1410, 1860, 1934, 1958, 2564]",
        buckets_repr(&Buckets::new_filtered(eight), bc, true)
    );

    // Narrow ranges keep filling the line as long as they fit into it.
    let twelve = (0..12).map(|i| i * 2 + 1).collect();
    assert_eq!(
        "[1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23]",
        buckets_repr(&Buckets::new_filtered(twelve), bc, true)
    );

    // Longer lists are split so that each line fills the format width.
    let many = (1..=23).map(|i| i * 100).collect();
    insta::assert_snapshot!(
        buckets_repr(&Buckets::new_filtered(many), bc, true),
        @r"
    [
      100, 200, 300, 400, 500, 600, 700, 800, 900,
      1000, 1100, 1200, 1300, 1400, 1500, 1600,
      1700, 1800, 1900, 2000, 2100, 2200, 2300
    ]
    "
    );

    // A range is never split across lines.
    let ranges = (0..12)
        .flat_map(|i| [i * 10, i * 10 + 1, i * 10 + 2])
        .collect();
    insta::assert_snapshot!(
        buckets_repr(&Buckets::new_filtered(ranges), bc, true),
        @r"
    [
      0-2, 10-12, 20-22, 30-32, 40-42, 50-52,
      60-62, 70-72, 80-82, 90-92, 100-102,
      110-112
    ]
    "
    );
}

/// Wide ranges leave room for fewer items per line than plain ids do.
/// Modelled on a cluster with 30000 buckets.
#[test]
fn fmt_wrap_depends_on_item_width() {
    let bc = 30000;

    // Ten wide ranges make up 131 characters, so the list is wrapped.
    let ten_wide = (0..10)
        .flat_map(|i| {
            let base = 10000 + i * 100;
            [base, base + 1, base + 2]
        })
        .collect();
    insta::assert_snapshot!(
        buckets_repr(&Buckets::new_filtered(ten_wide), bc, true),
        @r"
    [
      10000-10002, 10100-10102, 10200-10202,
      10300-10302, 10400-10402, 10500-10502,
      10600-10602, 10700-10702, 10800-10802,
      10900-10902
    ]
    "
    );

    // Plain five-digit ids are narrow enough to pack more per line.
    let wide_ids = (0..23).map(|i| 10000 + i * 137).collect();
    insta::assert_snapshot!(
        buckets_repr(&Buckets::new_filtered(wide_ids), bc, true),
        @r"
    [
      10000, 10137, 10274, 10411, 10548, 10685,
      10822, 10959, 11096, 11233, 11370, 11507,
      11644, 11781, 11918, 12055, 12192, 12329,
      12466, 12603, 12740, 12877, 13014
    ]
    "
    );
}

/// `EXPLAIN (BUCKETS)` output for a set built out of many ranges. A range
/// is wider than a plain id, so fewer of them fit on a line.
#[test]
fn fmt_output_many_ranges() {
    let bc = 30000;

    // 50 ranges of three contiguous buckets each.
    let ranges = (0..50)
        .flat_map(|i| {
            let base = 1000 + i * 100;
            [base, base + 1, base + 2]
        })
        .collect();
    let buckets = Buckets::new_filtered(ranges);

    // Without FMT the whole list stays on one line, however long.
    insta::assert_snapshot!(
        BoundedBuckets::new(buckets.clone(), bc, false),
        @"buckets = [1000-1002, 1100-1102, 1200-1202, 1300-1302, 1400-1402, 1500-1502, 1600-1602, 1700-1702, 1800-1802, 1900-1902, 2000-2002, 2100-2102, 2200-2202, 2300-2302, 2400-2402, 2500-2502, 2600-2602, 2700-2702, 2800-2802, 2900-2902, 3000-3002, 3100-3102, 3200-3202, 3300-3302, 3400-3402, 3500-3502, 3600-3602, 3700-3702, 3800-3802, 3900-3902, 4000-4002, 4100-4102, 4200-4202, 4300-4302, 4400-4402, 4500-4502, 4600-4602, 4700-4702, 4800-4802, 4900-4902, 5000-5002, 5100-5102, 5200-5202, 5300-5302, 5400-5402, 5500-5502, 5600-5602, 5700-5702, 5800-5802, 5900-5902]"
    );

    // With FMT six nine-character ranges fit per line.
    insta::assert_snapshot!(
        BoundedBuckets::new(buckets, bc, true),
        @r"
    buckets = [
      1000-1002, 1100-1102, 1200-1202, 1300-1302,
      1400-1402, 1500-1502, 1600-1602, 1700-1702,
      1800-1802, 1900-1902, 2000-2002, 2100-2102,
      2200-2202, 2300-2302, 2400-2402, 2500-2502,
      2600-2602, 2700-2702, 2800-2802, 2900-2902,
      3000-3002, 3100-3102, 3200-3202, 3300-3302,
      3400-3402, 3500-3502, 3600-3602, 3700-3702,
      3800-3802, 3900-3902, 4000-4002, 4100-4102,
      4200-4202, 4300-4302, 4400-4402, 4500-4502,
      4600-4602, 4700-4702, 4800-4802, 4900-4902,
      5000-5002, 5100-5102, 5200-5202, 5300-5302,
      5400-5402, 5500-5502, 5600-5602, 5700-5702,
      5800-5802, 5900-5902
    ]
    "
    );
}

/// Singletons, adjacent pairs and ranges mixed together. Item widths differ
/// here, so the number packed onto a line varies with what happens to land
/// on it.
#[test]
fn fmt_output_mixed_widths() {
    let bc = 30000;

    let mixed = (0..24)
        .flat_map(|i| match i % 4 {
            0 => vec![100 + i],
            1 => vec![1000 + i * 100, 1001 + i * 100],
            2 => vec![10000 + i * 100, 10001 + i * 100, 10002 + i * 100],
            _ => vec![20000 + i * 7],
        })
        .collect();
    let buckets = Buckets::new_filtered(mixed);

    insta::assert_snapshot!(
        BoundedBuckets::new(buckets.clone(), bc, false),
        @"buckets = [100, 104, 108, 112, 116, 120, 1100, 1101, 1500, 1501, 1900, 1901, 2300, 2301, 2700, 2701, 3100, 3101, 10200-10202, 10600-10602, 11000-11002, 11400-11402, 11800-11802, 12200-12202, 20021, 20049, 20077, 20105, 20133, 20161]"
    );

    // Every line is packed up to the format width.
    insta::assert_snapshot!(
        BoundedBuckets::new(buckets, bc, true),
        @r"
    buckets = [
      100, 104, 108, 112, 116, 120, 1100, 1101,
      1500, 1501, 1900, 1901, 2300, 2301,
      2700, 2701, 3100, 3101, 10200-10202,
      10600-10602, 11000-11002, 11400-11402,
      11800-11802, 12200-12202, 20021, 20049,
      20077, 20105, 20133, 20161
    ]
    "
    );

    // An upper bound is a single range, so FMT leaves it alone.
    insta::assert_snapshot!(
        BoundedBuckets::new(Buckets::All, bc, true),
        @"buckets <= [1-30000]"
    );
}
