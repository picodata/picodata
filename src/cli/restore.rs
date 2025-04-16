use crate::{
    cli::{args, tarantool::main_cb},
    config::{PicodataConfig, DEFAULT_CONFIG_FILE_NAME, GLOBAL_CONFIG},
    static_ref, tlog,
    traft::Result,
};
use std::path::PathBuf;

#[cfg(feature = "error_injection")]
use crate::error_injection;

pub fn main(args: args::Restore) -> ! {
    let tt_args: Vec<std::ffi::CString> = args.tt_args().unwrap();

    for (k, _) in std::env::vars() {
        let is_relevant = k.starts_with("TT_") || k.starts_with("TARANTOOL_");
        if !k.starts_with("TT_LDAP") && is_relevant {
            std::env::remove_var(k)
        }
    }

    main_cb(&tt_args, || -> Result<()> {
        #[cfg(feature = "error_injection")]
        error_injection::set_from_env();

        let backup_path_arg = args.backup_path;

        let backup_path = PathBuf::from(backup_path_arg.clone());

        if !backup_path.is_dir() {
            tlog!(Error, "Backup dir for restore doesn't exist");
            std::process::exit(1)
        }

        let config: &mut Box<PicodataConfig> = {
            // TODO: Instead of searching for default config file, we should search for
            //       any file with ".yaml" extension (but it doesn't have to be with extension).
            //       Better backup_dir should contain backup.metadata file in which the name
            //       of config is specified.
            //       See https://git.picodata.io/core/picodata/-/issues/2180.
            let config_name = args
                .config_name
                .unwrap_or_else(|| DEFAULT_CONFIG_FILE_NAME.to_string());

            let config_path = backup_path.join(config_name);
            let inner = PicodataConfig::read_yaml_file(&config_path);
            let Ok(inner) = inner else {
                tlog!(Error, "No config file to execute restore");
                std::process::exit(1)
            };
            unsafe {
                assert!(static_ref!(const GLOBAL_CONFIG).is_none());
                static_ref!(mut GLOBAL_CONFIG).insert(inner)
            }
        };

        let info = crate::info::VersionInfo::current();
        #[rustfmt::skip]
        tlog!(Info, "Picodata {} {} {}", info.picodata_version, info.build_type, info.build_profile);

        config.log_config_params();

        if let Err(error) = crate::restore_from_backup(config, &backup_path) {
            eprintln!("{}", error);
            std::process::exit(1);
        };

        std::process::exit(0)
    });
}
