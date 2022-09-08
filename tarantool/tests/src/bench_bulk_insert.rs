use std::cell::{Cell, RefCell};
use std::io;
use std::io::Write;
use std::rc::Rc;

use serde::{Deserialize, Serialize};
use tester::{Bencher, TDynBenchFn};

use tarantool::fiber::Fiber;
use tarantool::net_box::{Conn, ConnOptions, Options};
use tarantool::tuple::Encode;

pub struct BulkInsertBenchmark {
    pub test_size: usize,
    pub num_fibers: usize,
    pub num_rows: usize,
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S1Record<'a> {
    pub id: u32,
    pub text: &'a str,
}

impl Encode for S1Record<'_> {}

impl TDynBenchFn for BulkInsertBenchmark {
    fn run(&self, harness: &mut Bencher) {
        let test_size = self.test_size;
        let num_fibers = self.num_fibers;
        let num_rows = self.num_rows;

        let text = RefCell::new("X".repeat(test_size));
        let id_counter: Cell<usize> = Cell::new(0);

        let conn = Rc::new(
            Conn::new(
                ("localhost", unsafe { crate::LISTEN }),
                ConnOptions {
                    user: "bench_user".to_string(),
                    password: "password".to_string(),
                    ..ConnOptions::default()
                },
                None,
            )
            .unwrap(),
        );

        conn.call("_cleanup", Vec::<()>::new().as_slice(), &Options::default())
            .unwrap();

        harness.iter(|| {
            let mut fiber_pool = Vec::with_capacity(num_fibers);
            for _ in 0..num_fibers {
                let mut fiber = Fiber::new("bench_fiber", &mut |base_id| {
                    let base_id = *base_id;
                    let mut row = S1Record {
                        id: base_id as u32,
                        text: &*text.borrow(),
                    };

                    let mut remote_space = conn.space("bench_s1").unwrap().unwrap();
                    let insert_options = Options {
                        ..Options::default()
                    };

                    for _ in 0..num_rows {
                        let insert_result = remote_space.insert(&row, &insert_options).unwrap();
                        assert!(insert_result.is_some());
                        row.id += 1;
                    }
                    0
                });
                fiber.set_joinable(true);
                fiber_pool.push(fiber)
            }

            for fiber in fiber_pool.iter_mut() {
                let id = id_counter.get();
                id_counter.set(id + num_rows);
                fiber.start(id)
            }

            for (_, fiber) in fiber_pool.iter().enumerate() {
                fiber.join();
            }

            print!(".");
            io::stdout().flush().unwrap();
        });
    }
}
