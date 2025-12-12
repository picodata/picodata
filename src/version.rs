//! Used to unify parsing of versions in a single place.
//! There are neither `PICODATA_VERSION`, nor `RPC_API_VERSION`,
//! because Rust's capabilities in constant contexts are very poor.

use smol_str::SmolStr;
use std::{cmp::Ordering, fmt, str::FromStr};
use tarantool::error::BoxError;
use tarantool::error::TarantoolErrorCode;

/// Macro to automatically implement methods to compare versions in
/// [`Version`] struct considering the significance of each previous part.
macro_rules! impl_cmp {
    ($method:ident, [ $first:ident $(, $rest:ident)* ]) => {
        /// Implements comparison methods for [`Version`] components, ordered by significance.
        /// For example, if we use `cmp_up_to_minor`, it will compare both majors and minors
        /// by a significance and return appropriate [`std::cmp::Ordering`] after that:
        /// ```rust
        /// use std::cmp::Ordering;
        /// use picodata::version::Version;
        ///
        /// let greater_by_minor = Version::new_clean(1, 3, 0);
        /// let lesser_by_minor = Version::new_dirty(1, 2, 9, "-s0M3th1n");
        /// assert_eq!(
        ///     greater_by_minor.cmp_up_to_major(&lesser_by_minor),
        ///     Ordering::Equal
        /// );
        /// assert_eq!(
        ///     greater_by_minor.cmp_up_to_minor(&lesser_by_minor),
        ///     Ordering::Greater
        /// );
        /// assert_eq!(
        ///     greater_by_minor.cmp_up_to_patch(&lesser_by_minor),
        ///     Ordering::Greater
        /// );
        /// ```
        #[inline]
        pub fn $method(&self, other: &Self) -> Ordering {
            self.$first.cmp(&other.$first)
                $(.then_with(|| self.$rest.cmp(&other.$rest)))*
        }
    };
}

/// Macro to automatically implement methods to bump version parts in [`Version`].
macro_rules! impl_next {
    ($method:ident, $field:ident) => {
        /// Bump a specified part of the version by 1, returning a new instance of [`Version`].
        /// If version is dirty (`rest` field is not `None`), then it clones an underlying string.
        /// Otherwise, simple copy of clean parts of a version happens, which is very cheap to do.
        /// ```rust
        /// use picodata::version::Version;
        ///
        /// let initial_version = Version::new_dirty(1, 3, 5, "-vishel_zaychik_pogulyat");
        ///
        /// let greater_by_major = initial_version.clone().next_by_major();
        /// assert!(
        ///     &initial_version.major == &(&greater_by_major.major - 1)
        ///         && &initial_version.minor == &greater_by_major.minor
        ///         && &initial_version.patch == &greater_by_major.patch
        ///         && &initial_version.rest == &greater_by_major.rest
        /// );
        ///
        /// let greater_by_minor = initial_version.clone().next_by_minor();
        /// assert!(
        ///     &initial_version.major == &greater_by_minor.major
        ///         && &initial_version.minor == &(&greater_by_minor.minor - 1)
        ///         && &initial_version.patch == &greater_by_minor.patch
        ///         && &initial_version.rest == &greater_by_minor.rest
        /// );
        ///
        /// let greater_by_patch = initial_version.clone().next_by_patch();
        /// assert!(
        ///     &initial_version.major == &greater_by_patch.major
        ///         && &initial_version.minor == &greater_by_patch.minor
        ///         && &initial_version.patch == &(&greater_by_patch.patch - 1)
        ///         && &initial_version.rest == &greater_by_patch.rest
        /// );
        /// ````
        #[inline]
        pub fn $method(self) -> Self {
            Self {
                $field: self.$field + 1,
                ..self
            }
        }
    };
}

/// Struct to hold SemVer-like version with optional part at the end. Does not implement
/// any `*Eq` or `*Ord` traits because of multiple scenarios of comparisons between versions
/// like "do I care about minor?" or "do I need patch?". Instead, there are methods to
/// compare up to some part of the version, including optional part with a custom comparison
/// logic as a closure in API, or you can make your own comparison based on field access.
/// ```rust
/// use std::cmp::Ordering;
/// use picodata::version::Version;
///
/// let greater_by_minor = Version::new_clean(1, 3, 0);
/// let lesser_by_minor = Version::new_dirty(1, 2, 9, "-s0M3th1n");
/// assert_eq!(
///     greater_by_minor.cmp_up_to_major(&lesser_by_minor),
///     Ordering::Equal
/// );
/// assert_eq!(
///     greater_by_minor.cmp_up_to_minor(&lesser_by_minor),
///     Ordering::Greater
/// );
/// assert_eq!(
///     greater_by_minor.cmp_up_to_patch(&lesser_by_minor),
///     Ordering::Greater
/// );
/// ```
#[derive(Default, Debug, Clone)]
pub struct Version {
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
    pub rest: Option<SmolStr>,
}

impl Version {
    /// Create a clean version, e.g. "25.1.3". If you want to create a dirty
    /// version (with optional part after patch), see [`Version::new_dirty`] method.
    /// ```rust
    /// use picodata::version::Version;
    ///
    /// let version = Version::new_clean(25, 1, 3);
    /// assert_eq!(version.major, 25);
    /// assert_eq!(version.minor, 1);
    /// assert_eq!(version.patch, 3);
    /// assert_eq!(version.rest, None);
    /// ```
    #[inline(always)]
    pub const fn new_clean(major: u64, minor: u64, patch: u64) -> Self {
        Self {
            major,
            minor,
            patch,
            rest: None,
        }
    }

    /// Create a dirty version, e.g. "25.1.3-g989l1". If you want to create a clean
    /// version (without optional part after patch), see [`Version::new_clean`] method.
    /// You probably want to provide your own separator because separators may vary.
    /// ```rust
    /// use picodata::version::Version;
    ///
    /// let version = Version::new_dirty(25, 1, 3, "-g989l1");
    /// assert_eq!(version.major, 25);
    /// assert_eq!(version.minor, 1);
    /// assert_eq!(version.patch, 3);
    /// assert_eq!(version.rest.unwrap(), "-g989l1");
    /// ```
    #[inline(always)]
    pub fn new_dirty(major: u64, minor: u64, patch: u64, rest: &str) -> Self {
        Self {
            major,
            minor,
            patch,
            rest: Some(rest.into()),
        }
    }

    // see comments explaining those methods a `impl_cmp!` macro, because
    // decl macros must produce the doc comments as part of their expansion
    impl_next!(next_by_major, major);
    impl_next!(next_by_minor, minor);
    impl_next!(next_by_patch, patch);

    // see comments explaining those methods a `impl_cmp!` macro, because
    // decl macros must produce the doc comments as part of their expansion
    impl_cmp!(cmp_up_to_major, [major]);
    impl_cmp!(cmp_up_to_minor, [major, minor]);
    impl_cmp!(cmp_up_to_patch, [major, minor, patch]);
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{}.{}{}",
            self.major,
            self.minor,
            self.patch,
            self.rest.as_deref().unwrap_or_default(),
        )
    }
}

impl FromStr for Version {
    type Err = BoxError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let Some((major, rest)) = s.split_once('.') else {
            return Err(invalid_version(s));
        };
        let Some((minor, rest)) = rest.split_once('.') else {
            return Err(invalid_version(s));
        };

        let Ok(major) = major.parse() else {
            return Err(invalid_version(s));
        };
        let Ok(minor) = minor.parse() else {
            return Err(invalid_version(s));
        };

        // find numeric part of the patch and split it from rest if exists
        let patch_end = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        let patch = &rest[..patch_end];
        let Ok(patch) = patch.parse() else {
            return Err(invalid_version(s));
        };
        let rest = &rest[patch_end..];

        if rest.is_empty() {
            Ok(Self::new_clean(major, minor, patch))
        } else {
            Ok(Self::new_dirty(major, minor, patch, rest))
        }
    }
}

impl TryFrom<&str> for Version {
    type Error = BoxError;

    #[inline]
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::from_str(value)
    }
}

////////////////////////////////////////////////////////////////////////////////
// version_is_new_enough
////////////////////////////////////////////////////////////////////////////////

pub fn version_is_new_enough(
    system_catalog_version: &str,
    feature_implemented_in: &Version,
) -> crate::traft::Result<bool> {
    let system_catalog_version: Version = system_catalog_version.parse()?;
    let res = system_catalog_version
        .cmp_up_to_patch(feature_implemented_in)
        .is_ge();
    Ok(res)
}

////////////////////////////////////////////////////////////////////////////////
// ...
////////////////////////////////////////////////////////////////////////////////

#[track_caller]
#[inline]
fn invalid_version(actual: &str) -> BoxError {
    BoxError::new(
        TarantoolErrorCode::IllegalParams,
        format!("invalid version string: '{actual}', expected format: MAJOR.MINOR.PATCH[-TAIL]"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_output_and_parse() {
        let clean_raw = "25.1.3";
        let clean_parsed = Version::from_str(clean_raw)
            .expect("correct parsing")
            .to_string();
        let clean_created = Version::new_clean(25, 1, 3).to_string();
        assert!(clean_raw == clean_parsed && clean_parsed == clean_created);

        let dirty_raw = "25.1.3-dirty";
        let dirty_parsed = Version::from_str(dirty_raw)
            .expect("correct parsing")
            .to_string();
        let dirty_created = Version::new_dirty(25, 1, 3, "-dirty").to_string();
        assert!(dirty_raw == dirty_parsed && dirty_parsed == dirty_created);
    }

    #[test]
    fn version_cmp_up_to() {
        let greater_by_major = Version::new_clean(2, 0, 0);
        let lesser_by_major = Version::new_clean(1, 9, 9);
        assert_eq!(
            greater_by_major.cmp_up_to_major(&lesser_by_major),
            Ordering::Greater
        );
        assert_eq!(
            greater_by_major.cmp_up_to_minor(&lesser_by_major),
            Ordering::Greater
        );
        assert_eq!(
            greater_by_major.cmp_up_to_patch(&lesser_by_major),
            Ordering::Greater
        );

        let greater_by_minor = Version::new_clean(1, 3, 0);
        let lesser_by_minor = Version::new_clean(1, 2, 9);
        assert_eq!(
            greater_by_minor.cmp_up_to_major(&lesser_by_minor),
            Ordering::Equal
        );
        assert_eq!(
            greater_by_minor.cmp_up_to_minor(&lesser_by_minor),
            Ordering::Greater
        );
        assert_eq!(
            greater_by_minor.cmp_up_to_patch(&lesser_by_minor),
            Ordering::Greater
        );

        let greater_by_patch = Version::new_clean(1, 2, 4);
        let lesser_by_patch = Version::new_clean(1, 2, 3);
        assert_eq!(
            greater_by_patch.cmp_up_to_major(&lesser_by_patch),
            Ordering::Equal
        );
        assert_eq!(
            greater_by_patch.cmp_up_to_minor(&lesser_by_patch),
            Ordering::Equal
        );
        assert_eq!(
            greater_by_patch.cmp_up_to_patch(&lesser_by_patch),
            Ordering::Greater
        );

        let greater_by_minor_clean = Version::new_clean(1, 3, 0);
        let lesser_by_minor_dirty = Version::new_dirty(1, 2, 9, "-s0M3th1n");
        assert_eq!(
            greater_by_minor_clean.cmp_up_to_major(&lesser_by_minor_dirty),
            Ordering::Equal
        );
        assert_eq!(
            greater_by_minor_clean.cmp_up_to_minor(&lesser_by_minor_dirty),
            Ordering::Greater
        );
        assert_eq!(
            greater_by_minor_clean.cmp_up_to_patch(&lesser_by_minor_dirty),
            Ordering::Greater
        );
    }

    #[test]
    fn version_next_by() {
        let initial_version = Version::new_dirty(1, 3, 5, "-vishel_zaychik_pogulyat");

        let greater_by_major = initial_version.clone().next_by_major();
        assert!(
            &initial_version.major == &(&greater_by_major.major - 1)
                && &initial_version.minor == &greater_by_major.minor
                && &initial_version.patch == &greater_by_major.patch
                && &initial_version.rest == &greater_by_major.rest
        );

        let greater_by_minor = initial_version.clone().next_by_minor();
        assert!(
            &initial_version.major == &greater_by_minor.major
                && &initial_version.minor == &(&greater_by_minor.minor - 1)
                && &initial_version.patch == &greater_by_minor.patch
                && &initial_version.rest == &greater_by_minor.rest
        );

        let greater_by_patch = initial_version.clone().next_by_patch();
        assert!(
            &initial_version.major == &greater_by_patch.major
                && &initial_version.minor == &greater_by_patch.minor
                && &initial_version.patch == &(&greater_by_patch.patch - 1)
                && &initial_version.rest == &greater_by_patch.rest
        );
    }
}
