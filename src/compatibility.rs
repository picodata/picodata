use crate::traft::{error::Error, Result};
use crate::version::Version;
use smol_str::ToSmolStr;
use std::collections::BTreeSet;

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

/// Rolling upgrades allow at most two Picodata (major, minor) pairs among
/// retained instances.
///
/// During a major upgrade `cluster_version` remains at the lowest version,
/// for example `25.5`, until every old instance is upgraded. Pairwise checks
/// against it allow both `26.0` and `26.1`, although `25.5`, `26.0`, and `26.1`
/// must not coexist. Therefore this function validates the complete resulting
/// set; callers must replace the restarting instance's old version with its
/// requested version and omit expelled instances.
///
/// Panics if a version is not in the Picodata version format.
pub(crate) fn ensure_no_more_than_two_minor_versions<'a>(
    versions: impl IntoIterator<Item = &'a str>,
) -> Result<()> {
    let minor_versions = versions
        .into_iter()
        .map(|version| {
            let version = Version::try_from(version).expect("correct picodata version");
            (version.major, version.minor)
        })
        .collect::<BTreeSet<_>>();

    if minor_versions.len() > 2 {
        return Err(Error::TooManyPicodataMinorVersions {
            versions: minor_versions
                .into_iter()
                .map(|(major, minor)| format!("{major}.{minor}"))
                .collect::<Vec<_>>()
                .join(", ")
                .to_smolstr(),
        });
    }

    Ok(())
}

fn latest_minor_for_major(major_version_component: u64) -> u64 {
    match major_version_component {
        24 => 7, // for tests only
        25 => 5, // 25.5.X -> 26.X.X
        _ => unimplemented!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minor_version_limit() {
        ensure_no_more_than_two_minor_versions(["25.5.1", "25.5.2", "26.0.0"]).unwrap();

        let error =
            ensure_no_more_than_two_minor_versions(["25.5.1", "26.0.0", "26.1.0"]).unwrap_err();
        assert_eq!(
            error.to_string(),
            "cluster cannot contain more than two Picodata minor versions, found: 25.5, 26.0, 26.1"
        );
    }
}
