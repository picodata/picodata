use tarantool::fiber;

/// Fancy wrapper for tarantool fibers with a loop.
pub struct Loop(fiber::UnitJoinHandle<'static>);

pub enum FlowControl {
    Continue,
    Break,
}

impl Loop {
    pub fn start<A: 'static, S: 'static>(
        name: impl Into<String>,
        iter_fn: impl Fn(&A, &mut S) -> FlowControl + 'static,
        args: A,
        mut state: S,
    ) -> Self {
        #[allow(clippy::while_let_loop)]
        let loop_fn = move || loop {
            match iter_fn(&args, &mut state) {
                FlowControl::Continue => continue,
                FlowControl::Break => break,
            };
        };
        let fiber = fiber::Builder::new()
            .name(name)
            .proc(loop_fn)
            .start()
            .unwrap();

        Self(fiber)
    }

    pub fn join(self) {
        self.0.join()
    }
}
