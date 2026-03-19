use crate::ir::node::{Node136, Node232, Node32, Node64, Node96};

#[test]
fn test_node_size() {
    assert_eq!(std::mem::size_of::<Node32>(), 40);
    assert_eq!(std::mem::size_of::<Node64>(), 72);
    assert_eq!(std::mem::size_of::<Node96>(), 88);
    assert_eq!(std::mem::size_of::<Node136>(), 112);
    assert_eq!(std::mem::size_of::<Node232>(), 208);
}
