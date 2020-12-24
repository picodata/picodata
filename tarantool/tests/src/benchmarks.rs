use tester::{Bencher, TDynBenchFn};

pub struct BenchCase1 {}

impl TDynBenchFn for BenchCase1 {
    fn run(&self, harness: &mut Bencher) {
        harness.iter(|| {
            assert!(true);
        });
    }
}
