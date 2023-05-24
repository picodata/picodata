use ::serde::{Deserialize, Serialize};
use ::tarantool::tlua;

::tarantool::define_str_enum! {
    /// Activity state of an instance.
    #[derive(Default)]
    pub enum CurrentGradeVariant {
        /// Instance has gracefully shut down or has not been started yet.
        #[default]
        Offline = "Offline",
        /// Instance has configured replication.
        Replicated = "Replicated",
        /// Instance has configured sharding.
        ShardingInitialized = "ShardingInitialized",
        /// Instance is active and is handling requests.
        Online = "Online",
        /// Instance has permanently removed from cluster.
        Expelled = "Expelled",
    }
}

::tarantool::define_str_enum! {
    #[derive(Default)]
    pub enum TargetGradeVariant {
        /// Instance should be configured up
        Online = "Online",
        /// Instance should be gracefully shut down
        #[default]
        Offline = "Offline",
        /// Instance should be removed from cluster
        Expelled = "Expelled",
    }
}

////////////////////////////////////////////////////////////////////////////////

macro_rules! impl_constructors {
    (
        $(
            #[variant = $variant:expr]
            $(#[$meta:meta])*
            $vis:vis fn $constructor:ident(incarnation: u64) -> Self;
        )+
    ) => {
        $(
            $(#[$meta])*
            $vis fn $constructor(incarnation: u64) -> Self {
                Self { variant: $variant, incarnation }
            }
        )+
    };
}

/// A grade (current or target) associated with an incarnation (a monotonically
/// increasing number).
#[rustfmt::skip]
#[derive(Copy, Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[derive(tlua::LuaRead, tlua::Push, tlua::PushInto)]
pub struct Grade<V> {
    pub variant: V,
    pub incarnation: u64,
}

pub type TargetGrade = Grade<TargetGradeVariant>;

impl TargetGrade {
    impl_constructors! {
        #[variant = TargetGradeVariant::Offline]
        pub fn offline(incarnation: u64) -> Self;

        #[variant = TargetGradeVariant::Online]
        pub fn online(incarnation: u64) -> Self;

        #[variant = TargetGradeVariant::Expelled]
        pub fn expelled(incarnation: u64) -> Self;
    }
}

pub type CurrentGrade = Grade<CurrentGradeVariant>;

impl CurrentGrade {
    impl_constructors! {
        #[variant = CurrentGradeVariant::Offline]
        pub fn offline(incarnation: u64) -> Self;

        #[variant = CurrentGradeVariant::Replicated]
        pub fn replicated(incarnation: u64) -> Self;

        #[variant = CurrentGradeVariant::ShardingInitialized]
        pub fn sharding_initialized(incarnation: u64) -> Self;

        #[variant = CurrentGradeVariant::Online]
        pub fn online(incarnation: u64) -> Self;

        #[variant = CurrentGradeVariant::Expelled]
        pub fn expelled(incarnation: u64) -> Self;
    }
}

impl<G: PartialEq> PartialEq<G> for Grade<G> {
    fn eq(&self, other: &G) -> bool {
        &self.variant == other
    }
}

impl<G: std::fmt::Display> std::fmt::Display for Grade<G> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            variant,
            incarnation,
        } = self;
        write!(f, "{variant}({incarnation})")
    }
}

impl PartialEq<TargetGrade> for CurrentGrade {
    fn eq(&self, other: &TargetGrade) -> bool {
        self.incarnation == other.incarnation && self.variant.as_str() == other.variant.as_str()
    }
}

impl From<TargetGrade> for CurrentGrade {
    fn from(target_grade: TargetGrade) -> Self {
        let TargetGrade {
            variant,
            incarnation,
        } = target_grade;
        let variant = match variant {
            TargetGradeVariant::Online => CurrentGradeVariant::Online,
            TargetGradeVariant::Offline => CurrentGradeVariant::Offline,
            TargetGradeVariant::Expelled => CurrentGradeVariant::Expelled,
        };
        Self {
            variant,
            incarnation,
        }
    }
}

/// Check if instance's current and target grades match the specified pattern.
/// # Examples:
/// ```
/// # use picodata::{has_grades, instance::Instance};
/// # let instance = Instance::default();
/// #
/// // Check if current_grade == `Offline`, target_grade == `Online`
/// has_grades!(instance, Offline -> Online);
///
/// // Check if current grade == `Online`, target grade can be anything
/// has_grades!(instance, Online -> *);
///
/// // Check if target grade != `Expelled`, current grade can be anything
/// has_grades!(instance, * -> not Expelled);
///
/// // This is always `true`
/// has_grades!(instance, * -> *);
///
/// // Other combinations can also work
/// ```
#[macro_export]
macro_rules! has_grades {
    // Entry rule
    ($instance:expr, $($tail:tt)+) => {
        has_grades!(@impl $instance; current[] target[] $($tail)+)
    };

    // Parsing current
    (@impl $i:expr; current[] target[] not $($tail:tt)+) => {
        has_grades!(@impl $i; current[ ! ] target[] $($tail)+)
    };
    (@impl $i:expr; current[] target[] * -> $($tail:tt)+) => {
        has_grades!(@impl $i; current[ true ] target[] $($tail)+)
    };
    (@impl $i:expr; current[ $($not:tt)? ] target[] $current:ident -> $($tail:tt)+) => {
        has_grades!(@impl $i;
            current[
                $($not)?
                matches!($i.current_grade.variant, $crate::instance::grade::CurrentGradeVariant::$current)
            ]
            target[]
            $($tail)+
        )
    };

    // Parsing target
    (@impl $i:expr; current[ $($c:tt)* ] target[] not $($tail:tt)+) => {
        has_grades!(@impl $i; current[ $($c)* ] target[ ! ] $($tail)+)
    };
    (@impl $i:expr; current[ $($c:tt)* ] target[] *) => {
        has_grades!(@impl $i; current[ $($c)* ] target[ true ])
    };
    (@impl $i:expr; current[ $($c:tt)* ] target[ $($not:tt)? ] $target:ident) => {
        has_grades!(@impl $i;
            current[ $($c)* ]
            target[
                $($not)?
                matches!($i.target_grade.variant, $crate::instance::grade::TargetGradeVariant::$target)
            ]
        )
    };

    // Terminating rule
    (@impl $i:expr; current[ $($c:tt)+ ] target[ $($t:tt)+ ]) => {
        $($c)+ && $($t)+
    };
}

////////////////////////////////////////////////////////////////////////////////
/// tests
#[cfg(test)]
mod tests {
    use super::super::Instance;
    use super::{CurrentGradeVariant, TargetGradeVariant};
    use crate::has_grades;

    #[test]
    fn has_grades() {
        let mut i = Instance::default();
        i.current_grade.variant = CurrentGradeVariant::Online;
        i.target_grade.variant = TargetGradeVariant::Offline;

        assert!(has_grades!(i, * -> *));
        assert!(has_grades!(i, * -> Offline));
        assert!(has_grades!(i, * -> not Online));
        assert!(!has_grades!(i, * -> Online));
        assert!(!has_grades!(i, * -> not Offline));

        assert!(has_grades!(i, Online -> *));
        assert!(has_grades!(i, Online -> Offline));
        assert!(has_grades!(i, Online -> not Online));
        assert!(!has_grades!(i, Online -> Online));
        assert!(!has_grades!(i, Online -> not Offline));

        assert!(has_grades!(i, not Offline -> *));
        assert!(has_grades!(i, not Offline -> Offline));
        assert!(has_grades!(i, not Offline -> not Online));
        assert!(!has_grades!(i, not Offline -> Online));
        assert!(!has_grades!(i, not Offline -> not Offline));

        assert!(!has_grades!(i, Offline -> *));
        assert!(!has_grades!(i, Offline -> Offline));
        assert!(!has_grades!(i, Offline -> not Online));
        assert!(!has_grades!(i, Offline -> Online));
        assert!(!has_grades!(i, Offline -> not Offline));

        assert!(!has_grades!(i, not Online -> *));
        assert!(!has_grades!(i, not Online -> Offline));
        assert!(!has_grades!(i, not Online -> not Online));
        assert!(!has_grades!(i, not Online -> Online));
        assert!(!has_grades!(i, not Online -> not Offline));
    }
}
