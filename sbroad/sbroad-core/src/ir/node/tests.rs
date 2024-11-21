use crate::ir::node::{Node136, Node232, Node32, Node64, Node96};

#[test]
fn test_node_size() {
    assert!(std::mem::size_of::<Node32>() == 40);
    assert!(std::mem::size_of::<Node64>() == 72);
    assert!(std::mem::size_of::<Node96>() == 96);
    assert!(std::mem::size_of::<Node136>() == 136);
    assert!(std::mem::size_of::<Node232>() == 232);
}
