/// Bridge between the tokio async world (which `ldap3` library lives in) and tarantool's world, which uses fibers.
use std::future::Future;

/// A tokio runtime bridged to tarantool.
///
/// The bridge allows blocking a fiber on results for a spawned future.
///
/// The inner runtime only houses a single tokio worker thread.
pub struct TokioBridge {
    rt: tokio::runtime::Runtime,
    cbus_endpoint_name: String,
    // FIXME: the fiber keeps running when the runtime is dropped
    // This is not very impactful right now, since `TarantoolRuntime`
    //  is only ever used as a singleton in a static variable.
    #[expect(unused)]
    cbus_loop_fiber: tarantool::fiber::FiberId,
}

impl TokioBridge {
    /// Create a new tokio-tarantool runtime bridge.
    ///
    /// `name` is used to derive:
    /// - the name of the thread spawned by tokio
    /// - the cbus endpoint name
    /// - the spawned cbus worker fiber
    ///
    /// # Tarantool Context
    ///
    /// This function can only be called in an initialized tarantool cord.
    /// The bridge will be bound to the cord it was called in, and will not work with other
    /// tarantool cords or outside of it.
    ///
    /// Bad things will happen if you don't adhere to this, including possibilities of UB.
    pub fn new(name: &str) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            // This is practically the same as a current-thread runtime,
            // but has a nicer API: with current-thread runtime
            // we will have to spawn a thread and call `Runtime::block_on` in there.
            // When using multi-thread runtime with a single thread,
            // it will spawn its own worker thread for us.
            .worker_threads(1)
            .name(format!("{name}-rt"))
            .thread_name(name.to_string())
            .enable_io()
            .enable_time()
            .build()
            .expect("Failed to build tokio runtime for TarantoolRuntime");

        let cbus_endpoint_name = format!("{name}-cbus-endpoint");

        let cbus_loop_fiber = tarantool::fiber::Builder::new()
            .name(format!("{name}-cbus-fiber"))
            .func({
                let cbus_endpoint_name = cbus_endpoint_name.clone();

                move || {
                    let cbus_endpoint = tarantool::cbus::Endpoint::new(&cbus_endpoint_name)
                        .expect("Failed to create a cbus endpoint for TarantoolRuntime");
                    cbus_endpoint.cbus_loop();
                }
            })
            .start_non_joinable()
            .expect("Failed to start the cbus fiber for TarantoolRuntime");

        Self {
            rt,
            cbus_endpoint_name,
            cbus_loop_fiber,
        }
    }

    /// Spawn a future into the inner tokio runtime, and block the current fiber
    ///  until it has completed execution. The future's output is returned.
    ///
    /// # Tarantool Context
    ///
    /// This method should only be called in the same tarantool `cord`
    ///  that created the bridge.
    ///
    /// Bad things will happen if you don't adhere to this,
    ///  including possibilities of UB.
    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        // TODO: add a runtime check that this is called in the correct cord
        // This method will only work in the same cord that the `new` was called in,
        // because that's the cord that has the `cbus_loop` fiber running.

        let (result_tx, result_rx) =
            tarantool::cbus::oneshot::channel::<F::Output>(&self.cbus_endpoint_name);

        self.rt.spawn(async move {
            let result = future.await;
            result_tx.send(result);
        });

        let result = result_rx
            .receive()
            // It's only possible to receive an error here if `result_tx` is dropped
            //  while we are blocked on receiving.
            // And `result_tx` won't be dropped by the future unless it panics.
            .expect("Failed to receive the block_on result");

        result
    }
}
