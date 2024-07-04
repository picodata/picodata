use ::serde::{Deserialize, Serialize};
use ::tarantool::tlua;

::tarantool::define_str_enum! {
    /// Activity state of an instance.
    #[derive(Default)]
    pub enum StateVariant {
        /// Instance has gracefully shut down or has not been started yet.
        #[default]
        Offline = "Offline",
        /// Instance has configured replication.
        Replicated = "Replicated",
        /// Instance is active and is handling requests.
        Online = "Online",
        /// Instance has permanently removed from cluster.
        Expelled = "Expelled",
    }
}

////////////////////////////////////////////////////////////////////////////////

/// A state (current or target) associated with an incarnation (a monotonically
/// increasing number).
#[rustfmt::skip]
#[derive(Copy, Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[derive(tlua::LuaRead, tlua::Push, tlua::PushInto)]
pub struct State {
    pub variant: StateVariant,
    pub incarnation: u64,
}

impl State {
    #[inline(always)]
    pub fn new(variant: StateVariant, incarnation: u64) -> Self {
        Self {
            variant,
            incarnation,
        }
    }
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            variant,
            incarnation,
        } = self;
        write!(f, "{variant}({incarnation})")
    }
}

/// Check if instance's current and target states match the specified pattern.
/// # Examples:
/// ```
/// # use picodata::{has_states, instance::Instance};
/// # let instance = Instance::default();
/// #
/// // Check if current_state == `Offline`, target_state == `Online`
/// has_states!(instance, Offline -> Online);
///
/// // Check if current_state == `Online`, target_state can be anything
/// has_states!(instance, Online -> *);
///
/// // Check if target_state != `Expelled`, current_state can be anything
/// has_states!(instance, * -> not Expelled);
///
/// // This is always `true`
/// has_states!(instance, * -> *);
///
/// // Other combinations can also work
/// ```
#[macro_export]
macro_rules! has_states {
    // Entry rule
    ($instance:expr, $($tail:tt)+) => {
        has_states!(@impl $instance; current[] target[] $($tail)+)
    };

    // Parsing current
    (@impl $i:expr; current[] target[] not $($tail:tt)+) => {
        has_states!(@impl $i; current[ ! ] target[] $($tail)+)
    };
    (@impl $i:expr; current[] target[] * -> $($tail:tt)+) => {
        has_states!(@impl $i; current[ true ] target[] $($tail)+)
    };
    (@impl $i:expr; current[ $($not:tt)? ] target[] $current:ident -> $($tail:tt)+) => {
        has_states!(@impl $i;
            current[
                $($not)?
                matches!($i.current_state.variant, $crate::instance::state::StateVariant::$current)
            ]
            target[]
            $($tail)+
        )
    };

    // Parsing target
    (@impl $i:expr; current[ $($c:tt)* ] target[] not $($tail:tt)+) => {
        has_states!(@impl $i; current[ $($c)* ] target[ ! ] $($tail)+)
    };
    (@impl $i:expr; current[ $($c:tt)* ] target[] *) => {
        has_states!(@impl $i; current[ $($c)* ] target[ true ])
    };
    (@impl $i:expr; current[ $($c:tt)* ] target[ $($not:tt)? ] $target:ident) => {
        has_states!(@impl $i;
            current[ $($c)* ]
            target[
                $($not)?
                matches!($i.target_state.variant, $crate::instance::state::StateVariant::$target)
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
    use super::StateVariant;
    use crate::has_states;

    #[test]
    fn has_states() {
        let mut i = Instance::default();
        i.current_state.variant = StateVariant::Online;
        i.target_state.variant = StateVariant::Offline;

        assert!(has_states!(i, * -> *));
        assert!(has_states!(i, * -> Offline));
        assert!(has_states!(i, * -> not Online));
        assert!(!has_states!(i, * -> Online));
        assert!(!has_states!(i, * -> not Offline));

        assert!(has_states!(i, Online -> *));
        assert!(has_states!(i, Online -> Offline));
        assert!(has_states!(i, Online -> not Online));
        assert!(!has_states!(i, Online -> Online));
        assert!(!has_states!(i, Online -> not Offline));

        assert!(has_states!(i, not Offline -> *));
        assert!(has_states!(i, not Offline -> Offline));
        assert!(has_states!(i, not Offline -> not Online));
        assert!(!has_states!(i, not Offline -> Online));
        assert!(!has_states!(i, not Offline -> not Offline));

        assert!(!has_states!(i, Offline -> *));
        assert!(!has_states!(i, Offline -> Offline));
        assert!(!has_states!(i, Offline -> not Online));
        assert!(!has_states!(i, Offline -> Online));
        assert!(!has_states!(i, Offline -> not Offline));

        assert!(!has_states!(i, not Online -> *));
        assert!(!has_states!(i, not Online -> Offline));
        assert!(!has_states!(i, not Online -> not Online));
        assert!(!has_states!(i, not Online -> Online));
        assert!(!has_states!(i, not Online -> not Offline));
    }
}
