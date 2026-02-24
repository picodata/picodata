use crate::traft::{error::Error, Result};
use crate::version::Version;

/// Compares the versions of instances before joining the cluster.
/// Also compares the versions of picodata-plugin and Picodata itself.
/// Versions are compatible in the following cases:
///
/// If the `new_version` is on the same major version it must be at most one minor ahead.
/// Or
/// If the `new_version` is on next-major (`old_version.major + 1`) then
/// - its minor must be in the initial rollout window (`minor <= 1`).
/// - `old_version` must be on the latest supported minor for its major ([`latest_minor_for_major`])
///
/// Otherwise the versions are incompatible.
///
/// For example, with `latest_minor_for_major(25) = 5`, old_version `25.5.*` accepts
/// new_version `26.0.*` and `26.1.*`, while `25.4.* -> 26.*.*` is rejected.
///
/// Returns
/// - `Ok(false)` if versions are the same up to the patch component (ignoring the tail).
/// - `Ok(true)` if versions are compatible but different
/// - `Err(e)` if versions are incompatible
///
/// # Panicking
///
/// passed versions to this function should be in correct format, otherwise it will panic.
pub fn compare_picodata_versions(old_version: &str, new_version: &str) -> Result<bool> {
    let version_mismatch = || Error::PicodataVersionMismatch {
        old_version: old_version.into(),
        new_version: new_version.into(),
    };

    let old = Version::try_from(old_version).expect("correct old picodata version");
    let new = Version::try_from(new_version).expect("correct new picodata version");
    let has_ok_versions = {
        if new.major == old.major {
            new.minor == old.minor || new.minor == old.minor + 1
        } else if new.major == old.major + 1 {
            old.minor == latest_minor_for_major(old.major) && new.minor <= 1
        } else {
            false
        }
    };

    if !has_ok_versions {
        return Err(version_mismatch());
    }

    let version_changed = new.cmp_up_to_patch(&old).is_ne();
    Ok(version_changed)
}

fn latest_minor_for_major(major_version_component: u64) -> u64 {
    match major_version_component {
        24 => 7, // for tests only
        25 => 5, // 25.5.X -> 26.X.X
        _ => unimplemented!(),
    }
}
