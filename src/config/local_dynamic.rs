use crate::cli::args::LogLevel;
use crate::traft::error::Error;
use ::sql::ir::{types::DomainType as SqlType, value::Value as SqlValue};
use tarantool::define_str_enum;

define_str_enum! {
    pub enum LocalDynamicParameter {
        LogLevel = "log_level",
    }
}

trait LikeSqlType {
    const TYPE: SqlType;
}

impl LikeSqlType for LogLevel {
    const TYPE: SqlType = SqlType::String;
}

fn convert_sql_to_rust<T: LikeSqlType + for<'de> serde::Deserialize<'de>>(
    name: LocalDynamicParameter,
    value: &SqlValue,
) -> Result<T, Error> {
    let expected_type = T::TYPE;

    let Some(encoded_value) = crate::util::cast_and_encode(value, &expected_type) else {
        let actual_type = crate::sql::value_type_str(value);
        return Err(Error::other(format!(
            "invalid value for '{name}': expected {expected_type}, got {actual_type}"
        )));
    };

    // this shouldn't fail
    let encoded_bytes = rmp_serde::to_vec(&encoded_value)
        .map_err(|e| Error::other(format!("can't serialize the value for '{name}': {e}")))?;

    rmp_serde::from_slice(&encoded_bytes)
        .map_err(|e| Error::other(format!("invalid value for '{name}': {e}")))
}

mod parameters {
    use crate::cli::args::LogLevel;
    use crate::config::PicodataConfig;
    use crate::tlog;
    use crate::traft::error::Error;

    pub fn get_default_log_level() -> LogLevel {
        PicodataConfig::get().instance.log_level()
    }
    pub fn get_current_log_level() -> LogLevel {
        let say_level = tarantool::log::current_level();

        LogLevel::from(say_level)
    }
    pub fn set_log_level(value: LogLevel) -> Result<(), Error> {
        // NB: we are not using `tarantool::log::set_current_level` here, because lua logs will
        // ignore the log configuration set with it. To make lua logging subsystem aware
        // of the changed level we have to go through `box.cfg`
        let say_level = tarantool::log::SayLevel::from(value) as i32;
        let lua = tarantool::lua_state();
        lua.exec_with(
            "
            local level = ...
            box.cfg { log_level = level }
        ",
            say_level,
        )?;

        tlog!(Info, "dynamically changed log_level to {value:?}");

        Ok(())
    }
}

// FIXME: perhaps, those functions can be generated with a macro
pub fn reset_dynamic_local_parameter(name: LocalDynamicParameter) -> Result<(), Error> {
    match name {
        LocalDynamicParameter::LogLevel => {
            let default = parameters::get_default_log_level();
            parameters::set_log_level(default)
        }
    }
}

pub fn validate_and_set_dynamic_local_parameter(
    name: LocalDynamicParameter,
    value: &SqlValue,
) -> Result<(), Error> {
    match name {
        LocalDynamicParameter::LogLevel => {
            let log_level = convert_sql_to_rust::<LogLevel>(name, value)?;

            parameters::set_log_level(log_level)
        }
    }
}

pub fn get_dynamic_local_parameter(name: LocalDynamicParameter) -> rmpv::Value {
    let serialized = match name {
        LocalDynamicParameter::LogLevel => {
            let log_level = parameters::get_current_log_level();

            rmp_serde::to_vec(&log_level).unwrap()
        }
    };

    rmp_serde::from_slice(&serialized).unwrap()
}
