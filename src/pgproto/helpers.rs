/// See <https://doc.rust-lang.org/std/primitive.slice.html#method.split_array_ref>.
pub fn split_at_const<const N: usize>(bytes: &[u8]) -> Option<(&[u8; N], &[u8])> {
    (bytes.len() >= N).then(|| {
        let (head, tail) = bytes.split_at(N);
        (head.try_into().unwrap(), tail)
    })
}
