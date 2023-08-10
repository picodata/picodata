pub enum FlowControl {
    Continue,
    Break,
}

#[macro_export]
macro_rules! loop_start {
    ($name:expr, $fn:expr, $state:expr $(,)?) => {
        ::tarantool::fiber::Builder::new()
            .name($name)
            .proc(move || {
                ::tarantool::fiber::block_on(async {
                    let mut state = $state;
                    let iter_fn = $fn;
                    loop {
                        match iter_fn(&mut state).await {
                            $crate::r#loop::FlowControl::Continue => continue,
                            $crate::r#loop::FlowControl::Break => break,
                        };
                    }
                })
            })
            .start()
            .unwrap()
            .into()
    };
}
