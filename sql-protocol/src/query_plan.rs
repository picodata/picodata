pub struct ExplainIter<'bytes, I>
where
    I: Iterator<Item = &'bytes [u8]>,
{
    port: I,
    idx: u64,
}

impl<'bytes, I> ExplainIter<'bytes, I>
where
    I: Iterator<Item = &'bytes [u8]>,
{
    pub fn new(port: I) -> Self {
        ExplainIter { port, idx: 0 }
    }
}

impl<'bytes, I> Iterator for ExplainIter<'bytes, I>
where
    I: Iterator<Item = &'bytes [u8]>,
{
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        let mp = self.port.next()?;
        let explain = rmp_serde::from_slice::<Vec<String>>(mp).expect("expected vec of strings");

        let line = if explain[0].starts_with("Query") {
            self.idx += 1;
            format!("{}. {}", self.idx, explain[0])
        } else {
            explain[0].clone()
        };

        Some(line)
    }
}
