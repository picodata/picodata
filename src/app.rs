use serde::de::DeserializeOwned;
use serde::Serialize;
use std::error::Error;
use std::fmt::Display;
use std::time::Duration;
use tarantool::error::Error as TarantoolError;
use tarantool::index::IndexOptions;
use tarantool::space::{Space, SpaceCreateOptions};

pub trait Event {
    fn wait_timeout(&self, t: Duration) -> bool;
    fn set(&self);
}

pub trait AppError: Error {}
impl<T> AppError for T where T: Error {}

#[allow(dead_code)]
enum DDL {
    CreateSpace {
        name: &'static str,
        opts: SpaceCreateOptions,
    },
    CreateIndex {
        space_name: &'static str,
        name: &'static str,
        opts: IndexOptions,
    },
}

pub struct SchemaUpate {
    version: &'static str,
    ddl: Box<[DDL]>,
}

pub trait App {
    const NAME: &'static str;
    const VERSION: &'static str;

    fn schema() -> Box<[SchemaUpate]> {
        Box::new([SchemaUpate {
            version: "v0.0.0",
            ddl: [].into(),
        }])
    }

    /// initialization, no yields, no I/O
    fn new(rpc: &mut impl RpcRegister) -> Result<Box<Self>, Box<dyn AppError>>;

    /// blocking long-running call, can yield
    fn run(
        &mut self,
        stop: &impl Event,
        storage: &impl Storage,
        rpc: &impl RpcCall,
    ) -> Result<(), Box<dyn AppError>>;
}

pub trait Storage {
    // Cluster-wide Box API
    fn create_space(
        &mut self,
        name: &str,
        opts: &SpaceCreateOptions,
    ) -> Result<Space, TarantoolError>;
    fn space(&self, name: &str) -> Space;
    fn drop_space(&mut self, name: &str);
    // ...
}

pub trait RpcRequest: DeserializeOwned {}
impl<T> RpcRequest for T where T: DeserializeOwned {}

pub trait RpcResponse: Serialize {}
impl<T> RpcResponse for T where T: Serialize {}

pub trait RpcError: Display {}
impl<T> RpcError for T where T: Display {}

pub trait RpcRegister {
    #[allow(unused_variables)]
    fn register<Req, Res, E>(
        &mut self,
        name: &str,
        handler: impl FnMut(Req, &dyn Storage) -> Result<Res, E>,
    ) where
        Res: RpcResponse,
        Req: RpcRequest,
        E: RpcError,
    {
        todo!()
    }
}

pub trait RpcCall {
    #[allow(unused_variables)]
    fn call<Req, Res, E>(&self, name: &str, request: Req) -> Result<Res, E>
    where
        Res: RpcResponse,
        Req: RpcRequest,
        E: RpcError,
    {
        todo!()
    }
}
pub trait Rpc: RpcCall + RpcRegister {}

#[allow(clippy::all)]
#[allow(unused_variables)]
mod example {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::fmt::{Display, Formatter};
    use std::time::Duration;
    use tarantool::fiber;
    use thiserror::Error;

    mod rpc {
        use super::*;

        pub struct SomeRpc();

        impl RpcCall for SomeRpc {}

        impl Rpc for SomeRpc {}

        impl RpcRegister for SomeRpc {
            fn register<Req, Res, E>(
                &mut self,
                name: &str,
                handler: impl FnMut(Req, &dyn Storage) -> Result<Res, E>,
            ) where
                Res: RpcResponse,
                Req: RpcRequest,
                E: RpcError,
            {
                todo!()
            }
        }
    }

    mod storage {
        use super::*;

        pub struct SomeStorage();

        impl Storage for SomeStorage {
            fn create_space(
                &mut self,
                name: &str,
                opts: &SpaceCreateOptions,
            ) -> Result<Space, TarantoolError> {
                todo!()
            }

            fn space(&self, name: &str) -> tarantool::space::Space {
                todo!()
            }

            fn drop_space(&mut self, name: &str) {
                todo!()
            }
        }
    }

    mod event {
        use std::time::Duration;

        pub struct SomeEvent();
        impl super::Event for SomeEvent {
            fn wait_timeout(&self, t: Duration) -> bool {
                todo!()
            }
            fn set(&self) {
                todo!()
            }
        }
    }

    mod user_code {
        use super::*;

        pub struct App {
            greeting: String,
        }

        #[derive(Serialize, Deserialize)]
        struct HelloRequest {
            name: String,
        }

        #[derive(Serialize, Deserialize)]
        struct HelloResponse {
            greeting: String,
        }

        #[derive(Debug, Error)]
        #[allow(dead_code)]
        struct MyAppError {
            message: String,
        }

        impl Display for MyAppError {
            #[allow(unused_variables)]
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                unimplemented!();
            }
        }

        impl App {
            fn hello(
                &self,
                request: HelloRequest,
                storage: &dyn Storage,
            ) -> Result<HelloResponse, MyAppError> {
                let name = request.name;

                Ok(if Self::is_greeted(storage, &name) {
                    HelloResponse {
                        greeting: format!("{} again, {}!", self.greeting, name),
                    }
                } else {
                    Self::set_greeted(storage, &name);
                    HelloResponse {
                        greeting: format!("{}, {}!", self.greeting, name),
                    }
                })
            }

            fn is_greeted(storage: &dyn Storage, name: &str) -> bool {
                storage.space("hello_log").get(&(name,)).unwrap().is_some()
            }

            fn set_greeted(storage: &dyn Storage, name: &str) {
                storage.space("hello_log").insert(&(name,)).unwrap();
            }
        }

        impl From<tarantool::error::Error> for Box<dyn AppError> {
            fn from(_: tarantool::error::Error) -> Self {
                todo!()
            }
        }

        impl super::App for App {
            const NAME: &'static str = "MyApp";
            const VERSION: &'static str = "0.1.0";

            fn schema() -> Box<[SchemaUpate]> {
                Box::new([
                    // ! Only add elements, never delete or change.
                    SchemaUpate {
                        version: "1.0.0",
                        ddl: Box::new([
                            DDL::CreateSpace {
                                name: "hello_log",
                                opts: SpaceCreateOptions::default(),
                            },
                            DDL::CreateIndex {
                                space_name: "hello_log",
                                name: "pk",
                                opts: IndexOptions::default(),
                            },
                        ]),
                    },
                ])
            }

            fn new(rpc: &mut impl RpcRegister) -> Result<Box<Self>, Box<dyn AppError>> {
                let app = App {
                    greeting: "Hello".into(),
                };

                rpc.register("hello", |request, storage| app.hello(request, storage));

                Ok(Box::new(app))
            }

            fn run(
                &mut self,
                stop: &impl Event,
                storage: &impl Storage,
                rpc: &impl RpcCall,
            ) -> Result<(), Box<dyn AppError>> {
                // setup()

                loop {
                    if stop.wait_timeout(Duration::from_millis(100)) {
                        break;
                    }

                    storage.space("hello_log").delete(&("nobody",)).unwrap();

                    let _: Result<HelloResponse, MyAppError> =
                        rpc.call("hello", HelloRequest { name: "Bob".into() });
                }

                // teardown()

                Ok(())
            }
        }
    }

    struct RunAppError();

    fn exec_cluster_ddl(version: &str, ddl: &[DDL]) -> Result<(), RunAppError> {
        todo!()
    }

    #[allow(dead_code)]
    fn run_app() -> Result<(), RunAppError> {
        let mut rpc = rpc::SomeRpc();

        let mut app: user_code::App = *App::new(&mut rpc).unwrap();
        for s in user_code::App::schema().as_ref() {
            let SchemaUpate { version, ddl } = s;
            exec_cluster_ddl(version, ddl)?
        }

        let event = &event::SomeEvent();
        let storage = &storage::SomeStorage();

        let jh = fiber::defer_proc(move || app.run(event, storage, &rpc).unwrap());
        event.set();

        jh.join();
        Ok(())
    }
}
