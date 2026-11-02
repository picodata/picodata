//! The schema `sql-ast-new-corpus`'s queries bind against: the anonymized corpus
//! tables and indexes, registered on the [`MockCatalog`] builder.
//!
//! The tables mirror the production ones the corpus was captured from, down to
//! the storage they live on — Vinyl, on the `default` tier — since a bound
//! query's tier decides how the planner routes it.

use sql_ir::ir::relation::SpaceEngine;
use sql_ir::ir::types::UnrestrictedType;

use crate::mock_catalog::MockCatalog;

/// A [`Metadata`](sql_ir::ir::metadata::Metadata) catalog with the obfuscated
/// corpus tables and indexes, and the builtin functions.
#[must_use]
pub fn corpus_catalog() -> MockCatalog {
    use UnrestrictedType::{Boolean, Datetime, Decimal, Integer, String};

    let mut catalog = MockCatalog::new().on_storage(SpaceEngine::Vinyl, "default");

    catalog.add_sharded(
        "a",
        &[
            ("cv", Integer, false),
            ("aw", Integer, false),
            ("cq", Integer, false),
            ("bk", Integer, false),
            ("bl", Integer, false),
            ("bm", Integer, false),
            ("a", Datetime, false),
            ("z", Integer, false),
            ("ay", String, false),
            ("bg", String, false),
            ("ah", String, false),
            ("f", Decimal, true),
            ("e", Decimal, true),
            ("g", Decimal, true),
        ],
        &["cv"],
        &[
            "cv", "aw", "cq", "bk", "bl", "bm", "a", "z", "ay", "bg", "ah",
        ],
    );

    catalog.add_sharded(
        "b",
        &[
            ("cv", Integer, false),
            ("aw", Integer, false),
            ("cq", Integer, false),
            ("bk", Integer, false),
            ("bl", Integer, false),
            ("bm", Integer, false),
            ("ay", String, false),
            ("bg", String, false),
            ("ah", String, false),
            ("f", Decimal, true),
            ("e", Decimal, true),
        ],
        &["cv"],
        &["cv", "aw", "cq", "bk", "bl", "bm", "ay", "bg", "ah"],
    );

    catalog.add_sharded(
        "c",
        &[
            ("cv", Integer, false),
            ("bm", Integer, false),
            ("ay", String, false),
            ("bg", String, false),
            ("ah", String, false),
        ],
        &["cv"],
        &["cv", "bm", "ay", "bg", "ah"],
    );

    catalog.add_sharded(
        "d",
        &[
            ("cv", Integer, false),
            ("cq", Integer, false),
            ("bk", Integer, false),
            ("bl", Integer, false),
            ("bm", Integer, false),
            ("a", Datetime, false),
            ("z", Integer, false),
            ("ay", String, false),
            ("f", Decimal, true),
            ("e", Decimal, true),
        ],
        &["cv"],
        &["cv", "cq", "bk", "bl", "bm", "a", "z", "ay"],
    );

    catalog.add_sharded(
        "e",
        &[
            ("cv", Integer, false),
            ("cq", Integer, false),
            ("bk", Integer, false),
            ("bl", Integer, false),
            ("bm", Integer, false),
            ("ay", String, false),
            ("f", Decimal, true),
            ("e", Decimal, true),
            ("g", Decimal, true),
        ],
        &["cv"],
        &["cv", "cq", "bk", "bl", "bm", "ay"],
    );

    catalog.add_global(
        "f",
        &[
            ("cw", Integer, false),
            ("z", Integer, true),
            ("cj", Integer, true),
        ],
        &["cw"],
    );

    catalog.add_global(
        "g",
        &[("cu", Integer, false), ("cw", Integer, true)],
        &["cu"],
    );

    catalog.add_global(
        "h",
        &[("cu", Integer, false), ("cq", Integer, true)],
        &["cu"],
    );

    catalog.add_global(
        "i",
        &[
            ("br", Integer, false),
            ("ax", String, true),
            ("cy", String, true),
            ("bc", String, true),
            ("cz", Boolean, true),
            ("ct", Boolean, true),
            ("ap", Boolean, true),
        ],
        &["br"],
    );

    catalog.add_global("j", &[("bg", String, false), ("bc", String, true)], &["bg"]);

    catalog.add_global(
        "k",
        &[
            ("bi", String, false),
            ("bj", Integer, true),
            ("bk", Integer, true),
            ("bl", Integer, true),
        ],
        &["bi"],
    );

    catalog.add_sharded(
        "l",
        &[
            ("ag", Integer, false),
            ("v", Integer, false),
            ("cw", Integer, false),
            ("cv", Integer, false),
            ("cu", Integer, false),
            ("a", Datetime, false),
            ("u", Datetime, true),
            ("d", Decimal, false),
            ("au", Integer, false),
            ("ah", String, true),
            ("ay", String, true),
            ("bm", Integer, true),
            ("bi", String, true),
            ("cs", Datetime, true),
            ("h", Integer, true),
            ("cm", Integer, true),
            ("cn", String, true),
            ("cl", Datetime, true),
            ("bg", String, true),
            ("co", Integer, true),
            ("w", Datetime, true),
            ("bb", Integer, true),
        ],
        &["cv"],
        &["ag"],
    );

    catalog.add_sharded(
        "m",
        &[
            ("ag", Integer, false),
            ("cv", Integer, false),
            ("cr", Integer, false),
            ("w", Datetime, false),
            ("z", Integer, true),
            ("cu", Integer, true),
            ("bj", Integer, true),
            ("bm", Integer, true),
            ("aw", Integer, true),
            ("aa", Decimal, true),
            ("ao", Boolean, true),
            ("bb", Integer, true),
            ("ay", String, true),
            ("bg", String, true),
            ("ah", String, true),
            ("cw", Integer, true),
            ("ac", Decimal, true),
            ("ae", String, true),
            ("ab", Datetime, true),
            ("ad", Integer, true),
            ("h", Integer, true),
        ],
        &["cv"],
        &["ag", "cr"],
    );

    catalog.add_sharded(
        "n",
        &[
            ("ag", Integer, false),
            ("v", Integer, true),
            ("cw", Integer, false),
            ("cv", Integer, false),
            ("cu", Integer, false),
            ("a", Datetime, false),
            ("u", Datetime, true),
            ("d", Decimal, false),
            ("au", Integer, true),
            ("ah", String, true),
            ("ay", String, true),
            ("bm", Integer, true),
            ("bi", String, true),
            ("cs", Datetime, true),
            ("h", Integer, true),
            ("cm", Integer, false),
            ("cn", String, true),
            ("cl", Datetime, true),
            ("bg", String, true),
            ("co", Integer, true),
            ("w", Datetime, true),
            ("i", Datetime, true),
            ("bb", Integer, true),
            ("bf", Integer, true),
        ],
        &["cv"],
        &["ag"],
    );

    catalog.add_sharded(
        "o",
        &[
            ("ag", Integer, false),
            ("cv", Integer, false),
            ("r", String, false),
            ("q", Datetime, false),
            ("n", Integer, false),
            ("m", Integer, false),
            ("ak", String, true),
            ("t", Decimal, false),
            ("s", Integer, false),
            ("bn", Integer, false),
            ("bv", Decimal, false),
        ],
        &["cv"],
        &["ag"],
    );

    catalog.add_sharded(
        "p",
        &[
            ("ag", Integer, false),
            ("cv", Integer, false),
            ("l", Integer, false),
            ("au", Integer, true),
            ("av", Integer, true),
            ("aw", Integer, true),
            ("a", Datetime, false),
            ("af", Integer, true),
            ("bv", Decimal, false),
            ("cs", Datetime, true),
        ],
        &["cv"],
        &["ag"],
    );

    catalog.add_sharded(
        "q",
        &[
            ("ag", Integer, false),
            ("cv", Integer, false),
            ("j", Integer, false),
        ],
        &["cv"],
        &["ag"],
    );

    catalog.add_sharded(
        "r",
        &[
            ("ag", Integer, false),
            ("cv", Integer, false),
            ("k", Integer, false),
            ("ch", Datetime, false),
            ("ci", Decimal, false),
            ("o", Datetime, false),
            ("y", Decimal, true),
            ("an", Integer, false),
        ],
        &["cv"],
        &["ag"],
    );

    catalog.add_sharded(
        "s",
        &[
            ("ag", Integer, false),
            ("cv", Integer, false),
            ("bx", Decimal, true),
            ("bz", Decimal, true),
            ("ca", Decimal, true),
            ("cd", Decimal, true),
            ("cc", Decimal, true),
            ("ce", Decimal, true),
            ("cf", Decimal, true),
            ("i", Datetime, true),
            ("cg", Decimal, true),
            ("ah", String, true),
            ("ay", String, true),
        ],
        &["cv"],
        &["ag"],
    );

    catalog.add_sharded(
        "t",
        &[
            ("ag", Integer, false),
            ("cv", Integer, false),
            ("bx", Decimal, true),
            ("p", Decimal, true),
            ("bz", Decimal, true),
            ("i", Datetime, false),
        ],
        &["cv"],
        &["ag"],
    );

    catalog.add_sharded(
        "u",
        &[
            ("ag", Integer, false),
            ("v", Integer, false),
            ("bx", Decimal, true),
            ("bz", Decimal, true),
            ("ca", Decimal, true),
            ("cc", Decimal, true),
            ("cd", Decimal, true),
            ("ar", Datetime, false),
            ("cg", Decimal, true),
            ("ce", Decimal, true),
            ("i", Datetime, false),
            ("cf", Decimal, true),
            ("cv", Integer, true),
            ("aq", Boolean, true),
        ],
        &["cv"],
        &["ag"],
    );

    catalog.add_sharded(
        "v",
        &[
            ("ag", Integer, false),
            ("cv", Integer, false),
            ("v", Integer, false),
            ("ai", Decimal, false),
            ("b", Integer, false),
            ("a", Datetime, false),
            ("az", Decimal, false),
            ("au", Integer, false),
            ("i", Datetime, true),
        ],
        &["cv"],
        &["ag"],
    );

    catalog.add_sharded(
        "w",
        &[
            ("ag", Integer, false),
            ("x", Integer, false),
            ("d", Decimal, false),
            ("al", Integer, false),
            ("bt", Integer, true),
            ("db", Integer, true),
            ("da", Integer, true),
            ("bs", Integer, true),
            ("c", Integer, true),
            ("i", Datetime, true),
            ("cv", Integer, true),
        ],
        &["cv"],
        &["ag"],
    );

    catalog.add_sharded(
        "x",
        &[
            ("b", Integer, false),
            ("cv", Integer, true),
            ("u", Datetime, true),
            ("cw", Integer, true),
        ],
        &["cv"],
        &["b"],
    );

    catalog.add_sharded(
        "y",
        &[
            ("ag", Integer, false),
            ("cv", Integer, false),
            ("ay", String, true),
            ("cu", Integer, true),
            ("bg", String, true),
            ("ah", String, true),
            ("bx", Decimal, true),
            ("bz", Decimal, true),
            ("ca", Decimal, true),
            ("cd", Decimal, true),
            ("cc", Decimal, true),
            ("cq", Integer, true),
            ("aw", Integer, true),
            ("v", Integer, true),
            ("i", Datetime, true),
            ("cg", Decimal, true),
        ],
        &["cv"],
        &["ag"],
    );

    catalog.add_sharded(
        "z",
        &[("cv", Integer, false), ("i", Datetime, true)],
        &["cv"],
        &["cv"],
    );

    catalog.add_sharded(
        "aa",
        &[
            ("ag", Integer, false),
            ("cv", Integer, true),
            ("ay", String, true),
            ("r", String, true),
            ("q", Datetime, true),
            ("bq", Datetime, true),
            ("t", Decimal, false),
            ("bo", String, true),
            ("bh", String, true),
            ("cu", Integer, true),
            ("bu", Datetime, true),
            ("az", Decimal, true),
            ("i", Datetime, true),
            ("ba", String, true),
            ("bp", String, true),
        ],
        &["cv"],
        &["ag"],
    );

    catalog.add_sharded(
        "ab",
        &[
            ("cv", Integer, false),
            ("aj", String, true),
            ("ay", String, true),
            ("am", Integer, true),
        ],
        &["cv"],
        &["cv"],
    );

    catalog.add_sharded(
        "ac",
        &[
            ("cv", Integer, false),
            ("bx", Decimal, true),
            ("bz", Decimal, true),
            ("ca", Decimal, true),
            ("cb", Decimal, true),
            ("cg", Decimal, true),
            ("cp", Decimal, true),
            ("i", Datetime, true),
        ],
        &["cv"],
        &["cv"],
    );

    catalog.add_sharded(
        "ad",
        &[
            ("cv", Integer, false),
            ("ay", String, false),
            ("cx", String, true),
        ],
        &["cv"],
        &["cv", "ay"],
    );

    // Mirror test/int/sql/corpus_explain.sql, including unhinted indexes:
    // their creation order determines the IDs carried by INDEXED BY.
    catalog.add_index("ix_c", "a", &["cv", "aw", "cq", "a"]);
    catalog.add_index("ix_j", "a", &["cv", "ay"]);
    catalog.add_index("ix_k", "a", &["cv", "aw", "ay"]);
    catalog.add_index("ix_l", "a", &["cv", "a"]);
    catalog.add_index("ix_a", "a", &["cv", "aw", "a"]);
    catalog.add_index("ix_m", "a", &["cv", "aw", "bm", "bk"]);
    catalog.add_index("ix_b", "a", &["cv", "aw", "bm", "bk", "cq"]);
    catalog.add_index("ix_n", "b", &["cv", "aw", "bm"]);
    catalog.add_index("ix_o", "b", &["cv", "aw", "bm", "cq", "bk", "bl", "f", "e"]);
    catalog.add_index("ix_p", "c", &["cv", "bm"]);
    catalog.add_index("ix_q", "c", &["cv", "ay"]);
    catalog.add_index("ix_r", "d", &["cv", "ay"]);
    catalog.add_index("ix_s", "d", &["cv", "a"]);
    catalog.add_index("ix_t", "e", &["cv", "cq", "bm", "bl", "bk"]);
    catalog.add_index("ix_u", "e", &["cv", "ay"]);
    catalog.add_index("ix_v", "l", &["cv"]);
    catalog.add_index("ix_d", "l", &["cv", "ag"]);
    catalog.add_index("ix_w", "l", &["v"]);
    catalog.add_index("ix_x", "l", &["bb"]);
    catalog.add_index("ix_y", "l", &["au", "cv"]);
    catalog.add_index("ix_z", "l", &["cv", "cs"]);
    catalog.add_index("ix_aa", "l", &["cv", "a"]);
    catalog.add_index("ix_ab", "l", &["cv", "cw"]);
    catalog.add_index("ix_ac", "l", &["cv", "cw", "cs"]);
    catalog.add_index("ix_ad", "l", &["cv", "cw", "a"]);
    catalog.add_index("ix_ae", "m", &["cv"]);
    catalog.add_index("ix_af", "m", &["cv", "w"]);
    catalog.add_index("ix_ag", "m", &["cv", "aw"]);
    catalog.add_index("ix_ah", "m", &["cv", "cr"]);
    catalog.add_index("ix_ai", "m", &["cv", "ay"]);
    catalog.add_index("ix_aj", "m", &["cv", "ag"]);
    catalog.add_index("ix_ak", "m", &["cv", "aw", "w"]);
    catalog.add_index("ix_al", "n", &["cv"]);
    catalog.add_index("ix_am", "n", &["cv", "ag"]);
    catalog.add_index("ix_an", "n", &["au", "cv"]);
    catalog.add_index("ix_ao", "n", &["cv", "ay"]);
    catalog.add_index("ix_ap", "n", &["cv", "cu"]);
    catalog.add_index("ix_aq", "n", &["cv", "cs"]);
    catalog.add_index("ix_ar", "n", &["cv", "a"]);
    catalog.add_index("ix_e", "n", &["cv", "h"]);
    catalog.add_index("ix_as", "n", &["h"]);
    catalog.add_index("ix_at", "o", &["cv"]);
    catalog.add_index("ix_au", "o", &["cv", "s"]);
    catalog.add_index("ix_av", "p", &["cv"]);
    catalog.add_index("ix_aw", "p", &["l"]);
    catalog.add_index("ix_ax", "p", &["cv", "l"]);
    catalog.add_index("ix_ay", "q", &["cv"]);
    catalog.add_index("ix_az", "q", &["j"]);
    catalog.add_index("ix_ba", "q", &["cv", "j"]);
    catalog.add_index("ix_bb", "r", &["cv"]);
    catalog.add_index("ix_bc", "r", &["k"]);
    catalog.add_index("ix_bd", "r", &["cv", "k"]);
    catalog.add_index("ix_be", "s", &["cv"]);
    catalog.add_index("ix_bf", "s", &["cv", "ay"]);
    catalog.add_index("ix_bg", "t", &["cv"]);
    catalog.add_index("ix_bh", "u", &["cv"]);
    catalog.add_index("ix_bi", "u", &["cv", "ar"]);
    catalog.add_index("ix_bj", "u", &["cv", "ag"]);
    catalog.add_index("ix_bk", "v", &["cv"]);
    catalog.add_index("ix_bl", "v", &["cv", "ag"]);
    catalog.add_index("ix_bm", "v", &["v"]);
    catalog.add_index("ix_bn", "v", &["ai"]);
    catalog.add_index("ix_bo", "v", &["au", "cv"]);
    catalog.add_index("ix_f", "v", &["b", "cv"]);
    catalog.add_index("ix_bp", "w", &["cv"]);
    catalog.add_index("ix_bq", "w", &["bs", "cv", "al"]);
    catalog.add_index("ix_br", "w", &["c", "cv"]);
    catalog.add_index("ix_bs", "w", &["bt"]);
    catalog.add_index("ix_bt", "w", &["cv", "ag"]);
    catalog.add_index("ix_g", "w", &["x", "cv", "al"]);
    catalog.add_index("ix_bu", "x", &["cv"]);
    catalog.add_index("ix_h", "x", &["cv", "b"]);
    catalog.add_index("ix_bv", "y", &["cv", "ay"]);
    catalog.add_index("ix_bw", "y", &["bg"]);
    catalog.add_index("ix_bx", "y", &["cv", "aw"]);
    catalog.add_index("ix_by", "y", &["cv", "cu"]);
    catalog.add_index("ix_bz", "y", &["cv", "cq"]);
    catalog.add_index("ix_ca", "y", &["cv", "ay", "aw"]);
    catalog.add_index("ix_i", "y", &["cv", "ag"]);
    catalog.add_index("ix_cb", "ad", &["cv", "ay", "cx"]);

    catalog
}

#[cfg(test)]
mod tests {
    use super::corpus_catalog;
    use sql_ir::ir::metadata::Metadata;

    #[test]
    fn indexes_match_explain_schema() {
        let schema = include_str!("../../../test/int/sql/corpus_explain.sql");
        let expected: Vec<_> = schema
            .lines()
            .filter(|line| line.starts_with("CREATE INDEX "))
            .collect();
        // The reference schema uses bare identifiers; the renderer always quotes them.
        let actual: Vec<_> = corpus_catalog()
            .ddl()
            .lines()
            .filter(|line| line.starts_with("CREATE INDEX "))
            .map(|line| line.replace('"', ""))
            .collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn hinted_index_ids_match_explain_schema() {
        let catalog = corpus_catalog();
        // IDs follow each table's CREATE INDEX order, after the two implicit indexes.
        for (index, table, id) in [
            ("ix_a", "a", 6),
            ("ix_b", "a", 8),
            ("ix_c", "a", 2),
            ("ix_d", "l", 3),
            ("ix_e", "n", 9),
            ("ix_f", "v", 7),
            ("ix_g", "w", 7),
            ("ix_h", "x", 3),
            ("ix_i", "y", 8),
        ] {
            assert_eq!(catalog.get_index_id(index, table).unwrap(), id, "{index}");
        }
    }
}
