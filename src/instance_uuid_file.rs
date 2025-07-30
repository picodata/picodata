use crate::config::PicodataConfig;
use crate::tlog;
use crate::traft::error::Error;
use std::io::Read;
use std::io::Write;

const INSTANCE_UUID_FILENAME: &'static str = "instance_uuid";

pub fn dump_instance_uuid_file(instance_uuid: &str, config: &PicodataConfig) -> Result<(), Error> {
    let filepath =
        std::path::Path::new(config.instance.instance_dir()).join(INSTANCE_UUID_FILENAME);
    let f = filepath.display();

    let mut file = match std::fs::File::create(&filepath) {
        Ok(v) => v,
        Err(e) => {
            return Err(Error::other(format!("failed to create file {f}: {e}")));
        }
    };

    if let Err(e) = file.write_all(instance_uuid.as_bytes()) {
        return Err(Error::other(format!(
            "failed to save instance_uuid {instance_uuid} to file {f}: {e}"
        )));
    }

    tlog!(Info, "saved instance_uuid to file {f}");

    Ok(())
}

pub fn read_instance_uuid_file(config: &PicodataConfig) -> Result<Option<String>, Error> {
    let filepath =
        std::path::Path::new(config.instance.instance_dir()).join(INSTANCE_UUID_FILENAME);
    let f = filepath.display();

    let mut file = match std::fs::File::open(&filepath) {
        Ok(v) => v,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                return Ok(None);
            }

            return Err(Error::other(format!("failed to read file {f}: {e}")));
        }
    };

    let mut instance_uuid = String::new();
    if let Err(e) = file.read_to_string(&mut instance_uuid) {
        return Err(Error::other(format!(
            "failed to read instance_uuid from file {f}: {e}"
        )));
    }

    tlog!(Info, "restored instance_uuid {instance_uuid} from file {f}");

    Ok(Some(instance_uuid))
}

pub fn remove_instance_uuid_file(config: &PicodataConfig) -> Result<(), Error> {
    let filepath =
        std::path::Path::new(config.instance.instance_dir()).join(INSTANCE_UUID_FILENAME);
    let f = filepath.display();

    if std::fs::exists(&filepath)? {
        if let Err(e) = std::fs::remove_file(&filepath) {
            return Err(Error::other(format!("failed to remove file {f}: {e}")));
        }

        tlog!(Info, "removed file {f}");
    }

    Ok(())
}
