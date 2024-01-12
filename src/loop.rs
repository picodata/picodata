/// Creates the fiber and schedules it for execution. Doesn't yield.
#[macro_export]
macro_rules! loop_start {
    ($name:expr, $fn:expr, $state:expr $(,)?) => {
        ::tarantool::fiber::Builder::new()
            .name($name)
            .func(move || {
                ::tarantool::fiber::block_on(async {
                    let mut state = $state;
                    let iter_fn = $fn;
                    loop {
                        match iter_fn(&mut state).await {
                            std::ops::ControlFlow::Continue(()) => continue,
                            std::ops::ControlFlow::Break(()) => break,
                        };
                    }
                })
            })
            .defer()
            .unwrap()
            .into()
    };
}
