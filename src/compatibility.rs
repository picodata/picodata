use crate::traft::{error::Error, Result};
use crate::version::Version;

/// Compares the versions of instances before joining the cluster.
/// Versions are compatible in the following cases:
///
/// If the `joinee` is on the same major version it must be at most one minor ahead.
/// Or
/// If the `joinee` is on next-major (`leader.major + 1`) then
/// - its minor must be in the initial rollout window (`minor <= 1`).
/// - `leader` must be on the latest supported minor for its major ([`latest_minor_for_major`])
///
/// Otherwise the versions are incompatible.
///
/// For example, with `latest_minor_for_major(25) = 5`, leader `25.5.*` accepts
/// joinee `26.0.*` and `26.1.*`, while `25.4.* -> 26.*.*` is rejected.
///
/// Returns
/// - `Ok(false)` if versions are the same up to the patch component (ignoring the tail).
/// - `Ok(true)` if versions are compatible but different
/// - `Err(e)` if versions are incompatible
///
/// # Panicking
///
/// passed versions to this function should be in correct format, otherwise it will panic.
pub fn compare_picodata_versions(leader_version: &str, joinee_version: &str) -> Result<bool> {
    let version_mismatch = || Error::PicodataVersionMismatch {
        leader_version: leader_version.to_owned(),
        instance_version: joinee_version.to_owned(),
    };

    let leader = Version::try_from(leader_version).expect("correct picodata version");
    let joinee = Version::try_from(joinee_version).expect("correct picodata version");
    let has_ok_versions = {
        if joinee.major == leader.major {
            joinee.minor == leader.minor || joinee.minor == leader.minor + 1
        } else if joinee.major == leader.major + 1 {
            leader.minor == latest_minor_for_major(leader.major) && joinee.minor <= 1
        } else {
            false
        }
    };

    if !has_ok_versions {
        return Err(version_mismatch());
    }

    let version_changed = joinee.cmp_up_to_patch(&leader).is_ne();
    Ok(version_changed)
}

fn latest_minor_for_major(major_version_component: u64) -> u64 {
    match major_version_component {
        25 => 5, // 25.5.X -> 26.X.X
        _ => unimplemented!(),
    }
}
