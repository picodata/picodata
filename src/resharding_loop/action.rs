//! This file defines [`ReshardingAction`] & [`ReshardingActionKind`] enums by
//! means of the [`define_ReshardingAction`] macro.
//!
//! Variants of enum [`ReshardingAction`] are structs which are also defined in
//! here and describe actions to be performed in [`resharding_loop`] as part of
//! the bucket rebalancing algorithm.
//!
//! For each variant of [`ReshardingAction`] there's an identically named variant
//! of [`ReshardingActionKind`], but the latter enum doesn't contain any
//! associated data, so it can be used for debugging, tracking progess, etc.
//! It's basically something like rust's [`std::mem::Discriminant`], but with
//! more functionality.
//!
//! [`resharding_loop`]: crate::resharding_loop::resharding_loop
//! [`define_ReshardingAction`]: crate::define_ReshardingAction
use crate::catalog::pico_bucket::BucketIdRange;
use crate::traft::op::Dml;
use crate::vshard::VshardBucketRecord;
use crate::vshard::VshardBucketState;

////////////////////////////////////////////////////////////////////////////////
// ReshardingAction
////////////////////////////////////////////////////////////////////////////////

// See module-level doc-comments.
crate::define_ReshardingAction! {
    pub struct ActualizeShardedState {
        pub from_state: Option<VshardBucketState>,
        pub to_state: VshardBucketState,
        pub range: BucketIdRange,
        pub changes: Vec<VshardBucketRecord>,
    }

    pub struct ActualizeBucketStateVersion {
        pub version_bump: Vec<Dml>,
    }
}

////////////////////////////////////////////////////////////////////////////////
// define_ReshardingAction macro
////////////////////////////////////////////////////////////////////////////////

// NOTE: it's marked with `macro_export` so that it can be used before it's
// declaration in the file. We do this for readability.
#[macro_export]
macro_rules! define_ReshardingAction {
    (
        $(
            $(#[$struct_meta:meta])*
            pub struct $action:ident {
                $(
                    $(#[$field_meta:meta])*
                    pub $field:ident: $field_ty:ty,
                )*
            }
        )+
    ) => {
        $(
            $(#[$struct_meta])*
            pub struct $action {
                $(
                    $(#[$field_meta])*
                    pub $field: $field_ty,
                )*
            }

            impl From<$action> for ReshardingAction {
                fn from(s: $action) -> Self {
                    Self::$action(s)
                }
            }

            impl $action {
                #[allow(dead_code)]
                pub const KIND: ReshardingActionKind = ReshardingActionKind::$action;
            }
        )+

        // We don't care about `large_enum_variant` in this case, because this
        // enum is only stored on the stack and never even gets copied.
        #[allow(clippy::large_enum_variant)]
        pub enum ReshardingAction {
            $(
                $(#[$struct_meta])*
                $action ( $action ),
            )+
            GoIdle,
        }

        impl ReshardingAction {
            pub fn kind(&self) -> ReshardingActionKind {
                match self {
                    $(
                        Self::$action { .. } => ReshardingActionKind::$action,
                    )+
                    Self::GoIdle => ReshardingActionKind::GoIdle,
                }
            }
        }

        #[derive(Default, Debug, PartialEq, Eq, Hash, Clone, Copy)]
        pub enum ReshardingActionKind {
            $(
                $(#[$struct_meta])*
                $action,
            )+
            #[default]
            GoIdle,
        }

        impl ::std::fmt::Display for ReshardingActionKind {
            fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                write!(f, "{self:?}")
            }
        }
    }
}

#[macro_export]
macro_rules! debug_assert_action_kind {
    ($action:expr, $pattern:pat) => {
        debug_assert!(
            matches!($action, $pattern),
            "Unexpected ReshardingActionKind: {:?}, expected this: {}",
            $action.kind(),
            stringify!($pattern)
        );
    };
}
