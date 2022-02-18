pub struct Drain;

pub fn root() -> slog::Logger {
    slog::Logger::root(Drain, slog::o!())
}

#[macro_export]
macro_rules! tlog {
    ($lvl:ident, $($args:tt)*) => {{
        let logger = $crate::tlog::root();
        slog::slog_log!(logger, slog::Level::$lvl, "", $($args)*);
    }}
}

impl slog::Drain for Drain {
    type Ok = ();
    type Err = slog::Never;
    fn log(
        &self,
        record: &slog::Record,
        values: &slog::OwnedKVList,
    ) -> Result<Self::Ok, Self::Err> {
        use ::tarantool::log::say;
        use ::tarantool::log::SayLevel;

        // Max level is constant = trace
        // It's hardcoded in Cargo.toml dependency features
        // In runtime it's managed by tarantool box.cfg.log_level
        let lvl = match record.level() {
            slog::Level::Critical => SayLevel::Crit,
            slog::Level::Error => SayLevel::Error,
            slog::Level::Warning => SayLevel::Warn,
            slog::Level::Info => SayLevel::Info,
            slog::Level::Debug => SayLevel::Verbose,
            slog::Level::Trace => SayLevel::Debug,
        };

        let mut s = StrSerializer {
            str: format!("{}", record.msg()),
        };

        use slog::KV;
        // It's safe to use .unwrap() here since
        // StrSerializer doesn't return anything but Ok()
        record.kv().serialize(record, &mut s).unwrap();
        values.serialize(record, &mut s).unwrap();

        say(lvl, record.file(), record.line() as i32, None, &s.str);
        Ok(())
    }
}

struct StrSerializer {
    pub str: String,
}

impl slog::Serializer for StrSerializer {
    fn emit_arguments(&mut self, key: slog::Key, val: &std::fmt::Arguments) -> slog::Result {
        self.str.push_str(&format!(", {}: {}", key, val));
        Ok(())
    }
}
