pub enum FlowControl {
    Continue,
    Break,
}

#[macro_export]
macro_rules! loop_start {
    ($name:expr, $fn:expr, $args:expr, $state:expr $(,)?) => {
        ::tarantool::fiber::Builder::new()
            .name($name)
            .proc(move || {
                ::tarantool::fiber::block_on(async {
                    let args = $args;
                    let mut state = $state;
                    let iter_fn = $fn;
                    loop {
                        match iter_fn(&args, &mut state).await {
                            FlowControl::Continue => continue,
                            FlowControl::Break => break,
                        };
                    }
                })
            })
            .start()
            .unwrap()
            .into()
    };
}
