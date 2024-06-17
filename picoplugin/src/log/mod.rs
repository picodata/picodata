//! Log module provides special log marcos for end-to-end logging and ability to change
//! default tarantool-logger templates (using `tarolog` lib).

pub use log as rs_log;

pub use tarolog::set_default_logger_format;
pub use tarolog::Format;
pub use tarolog::JsonInjector;
pub use tarolog::PlainInjector;

pub use crate::pico_debug as debug;
pub use crate::pico_error as error;
pub use crate::pico_info as info;
pub use crate::pico_warn as warn;

pub trait RequestIdOwner {
    fn request_id(&self) -> &str;
}

#[macro_export]
macro_rules! pico_error {
        (ctx: $ctx:expr, target: $target:expr, $fmt:expr, $($arg:tt)*) => {
            $crate::log::rs_log::error!(target: $target, concat!("[{}]: ", $fmt), $crate::log::RequestIdOwner::request_id($ctx), $($arg)*)
        };
        (ctx: $ctx:expr, $fmt:expr, $($arg:tt)*) => {
            $crate::log::rs_log::error!(concat!("[{}]: ", $fmt), $crate::log::RequestIdOwner::request_id($ctx), $($arg)*)
        };
        (target: $target:expr, $($arg:tt)+) => {
            $crate::log::rs_log::error!(target: $target, $($arg)+)
        };
        ($($arg:tt)+) => {
            $crate::log::rs_log::error!($($arg)+)
        };
    }

#[macro_export]
macro_rules! pico_warn {
        (ctx: $ctx:expr, target: $target:expr, $fmt:expr, $($arg:tt)*) => {
            $crate::log::rs_log::warn!(target: $target, concat!("[{}]: ", $fmt), $crate::log::RequestIdOwner::request_id($ctx), $($arg)*)
        };
        (ctx: $ctx:expr, $fmt:expr, $($arg:tt)*) => {
            $crate::log::rs_log::warn!(concat!("[{}]: ", $fmt), $crate::log::RequestIdOwner::request_id($ctx), $($arg)*)
        };
        (target: $target:expr, $($arg:tt)+) => {
            $crate::log::rs_log::warn!(target: $target, $($arg)+)
        };
        ($($arg:tt)+) => {
            $crate::log::rs_log::warn!($($arg)+)
        };
    }

#[macro_export]
macro_rules! pico_info {
        (ctx: $ctx:expr, target: $target:expr, $fmt:expr, $($arg:tt)*) => {
            $crate::log::rs_log::info!(target: $target, concat!("[{}]: ", $fmt), $crate::log::RequestIdOwner::request_id($ctx), $($arg)*)
        };
        (ctx: $ctx:expr, $fmt:expr, $($arg:tt)*) => {
            $crate::log::rs_log::info!(concat!("[{}]: ", $fmt), $crate::log::RequestIdOwner::request_id($ctx), $($arg)*)
        };
        (target: $target:expr, $($arg:tt)+) => {
            $crate::log::rs_log::info!(target: $target, $($arg)+)
        };
        ($($arg:tt)+) => {
            $crate::log::rs_log::info!($($arg)+)
        };
    }

#[macro_export]
macro_rules! pico_debug {
        (ctx: $ctx:expr, target: $target:expr, $fmt:expr, $($arg:tt)*) => {
            $crate::log::rs_log::debug!(target: $target, concat!("[{}]: ", $fmt), $crate::log::RequestIdOwner::request_id($ctx), $($arg)*)
        };
        (ctx: $ctx:expr, $fmt:expr, $($arg:tt)*) => {
            $crate::log::rs_log::debug!(concat!("[{}]: ", $fmt), $crate::log::RequestIdOwner::request_id($ctx), $($arg)*)
        };
        (target: $target:expr, $($arg:tt)+) => {
            $crate::log::rs_log::debug!(target: $target, $($arg)+)
        };
        ($($arg:tt)+) => {
            $crate::log::rs_log::debug!($($arg)+)
        };
    }

#[cfg(test)]
mod test {
    use crate::log::RequestIdOwner;
    use log::{Level, LevelFilter, Log, Metadata, Record};
    use std::sync::Mutex;
    use std::sync::OnceLock;

    struct BufferedLogger {
        buff: Mutex<Vec<(Level, String, String)>>,
    }

    impl Log for BufferedLogger {
        fn enabled(&self, _metadata: &Metadata) -> bool {
            true
        }

        fn log(&self, record: &Record) {
            self.buff.lock().unwrap().push((
                record.level(),
                record.target().to_string(),
                record.args().to_string(),
            ))
        }

        fn flush(&self) {
            self.buff.lock().unwrap().clear();
        }
    }

    struct TestContext {
        rid: &'static str,
    }

    impl RequestIdOwner for TestContext {
        fn request_id(&self) -> &str {
            self.rid
        }
    }

    static LOGGER: OnceLock<BufferedLogger> = OnceLock::new();

    fn logger() -> &'static BufferedLogger {
        LOGGER.get_or_init(|| BufferedLogger {
            buff: Mutex::default(),
        })
    }

    #[test]
    fn test_logger() {
        log::set_logger(logger())
            .map(|()| log::set_max_level(LevelFilter::Debug))
            .unwrap();

        test_logger_simple();
        test_logger_with_custom_target();
        test_logger_with_request_id();
        test_logger_with_custom_target_and_request_id();
    }

    fn test_logger_simple() {
        logger().flush();

        pico_debug!("simple log record {}", 1);
        pico_info!("simple log record {}", 2);
        pico_warn!("simple log record {}", 3);
        pico_error!("simple log record {}", 4);

        assert_eq!(
            &[
                (
                    Level::Debug,
                    "picoplugin::log::test".to_string(),
                    "simple log record 1".to_string()
                ),
                (
                    Level::Info,
                    "picoplugin::log::test".to_string(),
                    "simple log record 2".to_string()
                ),
                (
                    Level::Warn,
                    "picoplugin::log::test".to_string(),
                    "simple log record 3".to_string()
                ),
                (
                    Level::Error,
                    "picoplugin::log::test".to_string(),
                    "simple log record 4".to_string()
                ),
            ],
            logger().buff.lock().unwrap().as_slice()
        );
    }

    fn test_logger_with_custom_target() {
        logger().flush();

        pico_debug!(target: "test", "log record");
        pico_info!(target: "test","log record");
        pico_warn!(target: "test","log record");
        pico_error!(target: "test", "log record");

        assert_eq!(
            &[
                (Level::Debug, "test".to_string(), "log record".to_string()),
                (Level::Info, "test".to_string(), "log record".to_string()),
                (Level::Warn, "test".to_string(), "log record".to_string()),
                (Level::Error, "test".to_string(), "log record".to_string()),
            ],
            logger().buff.lock().unwrap().as_slice()
        );
    }

    fn test_logger_with_request_id() {
        logger().flush();

        let ctx = TestContext { rid: "12345" };

        pico_debug!(ctx: &ctx, "{}", "log record");
        pico_info!(ctx: &ctx, "{}", "log record");
        pico_warn!(ctx: &ctx, "{}", "log record");
        pico_error!(ctx: &ctx, "{}", "log record");

        assert_eq!(
            &[
                (
                    Level::Debug,
                    "picoplugin::log::test".to_string(),
                    "[12345]: log record".to_string()
                ),
                (
                    Level::Info,
                    "picoplugin::log::test".to_string(),
                    "[12345]: log record".to_string()
                ),
                (
                    Level::Warn,
                    "picoplugin::log::test".to_string(),
                    "[12345]: log record".to_string()
                ),
                (
                    Level::Error,
                    "picoplugin::log::test".to_string(),
                    "[12345]: log record".to_string()
                ),
            ],
            logger().buff.lock().unwrap().as_slice()
        );
    }

    fn test_logger_with_custom_target_and_request_id() {
        logger().flush();

        let ctx = TestContext { rid: "12345" };

        pico_debug!(ctx: &ctx, target: "test", "{}", "log record");
        pico_info!(ctx: &ctx, target: "test", "{}", "log record");
        pico_warn!(ctx: &ctx, target: "test", "{}", "log record");
        pico_error!(ctx: &ctx, target: "test", "{}", "log record");

        assert_eq!(
            &[
                (
                    Level::Debug,
                    "test".to_string(),
                    "[12345]: log record".to_string()
                ),
                (
                    Level::Info,
                    "test".to_string(),
                    "[12345]: log record".to_string()
                ),
                (
                    Level::Warn,
                    "test".to_string(),
                    "[12345]: log record".to_string()
                ),
                (
                    Level::Error,
                    "test".to_string(),
                    "[12345]: log record".to_string()
                ),
            ],
            logger().buff.lock().unwrap().as_slice()
        );
    }
}
