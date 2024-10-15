use picodata_plugin::plugin::interface::{Service, ServiceRegistry};
use picodata_plugin::plugin::prelude::service_registrar;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct TestServiceConfig {
    the_answer: i32,
}

struct TestService {}

impl Service for TestService {
    type Config = TestServiceConfig;
}

impl TestService {
    pub fn new() -> Self {
        Self {}
    }
}

#[service_registrar]
pub fn service_registrar(reg: &mut ServiceRegistry) {
    reg.add("testservice", "0.1.0", TestService::new);
}
