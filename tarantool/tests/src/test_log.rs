use log::{warn, LevelFilter};

use tarantool::log::{say, SayLevel, TarantoolLogger};

pub fn test_log() {
    log::set_logger(&TarantoolLogger {}).unwrap();
    log::set_max_level(LevelFilter::Debug);
    warn!(target: "target", "message {}", 99);

    say(SayLevel::Warn, "<file>", 0, Some("<error>"), "<message>");
}
