use std::iter::repeat;
use std::time::{SystemTime, UNIX_EPOCH};

use tester::{Bencher, TDynBenchFn};

use tarantool::fiber::Fiber;
use tarantool::net_box::{Conn, ConnOptions, Options};
use tarantool::space::Space;

use crate::common::S1Record;

pub struct BulkInsertBenchmark {
    pub test_size: usize,
    pub num_fibers: usize,
    pub num_rows: usize,
}

impl TDynBenchFn for BulkInsertBenchmark {
    fn run(&self, harness: &mut Bencher) {
        let test_size = self.test_size;
        let num_fibers = self.num_fibers;
        let num_rows = self.num_rows;

        let text = repeat("X")
            .take(test_size)
            .collect::<String>()
            .into_boxed_str();

        let id_base = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as usize;

        harness.iter(|| {
            {
                let mut local_space = Space::find("bench_s1").unwrap();
                local_space.truncate().unwrap();
            }

            let mut fiber_pool = Vec::with_capacity(num_fibers);
            for fiber_id in 0..num_fibers {
                let mut fiber = Fiber::new("bench_fiber", &mut |_| {
                    let id_base = id_base + (fiber_id * num_rows);
                    let mut row = S1Record {
                        id: id_base as u32,
                        text: text.to_string(),
                    };

                    let conn = Conn::new(
                        "localhost:3301",
                        ConnOptions {
                            user: "bench_user".to_string(),
                            password: "password".to_string(),
                            ..ConnOptions::default()
                        },
                    )
                    .unwrap();
                    let mut remote_space = conn.space("bench_s1").unwrap().unwrap();
                    let insert_options = Options::default();

                    for _ in 0..num_rows {
                        let insert_result = remote_space.insert(&row, &insert_options).unwrap();
                        assert!(insert_result.is_some());

                        row.id += 1;
                    }
                    0
                });
                fiber.set_joinable(true);
                fiber.start(());
                fiber_pool.push(fiber)
            }

            for fiber in fiber_pool.iter() {
                fiber.join();
            }
        });
    }
}
