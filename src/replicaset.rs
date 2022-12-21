use super::instance::InstanceId;
use ::tarantool::tlua;
use ::tarantool::tuple::Encode;

crate::define_string_newtype! {
    /// Unique id of a replicaset.
    ///
    /// This is a new-type style wrapper around String,
    /// to distinguish it from other strings.
    pub struct ReplicasetId(pub String);
}

pub type Weight = f64;

////////////////////////////////////////////////////////////////////////////////
/// Replicaset info
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Replicaset {
    /// Primary identifier.
    pub replicaset_id: ReplicasetId,

    /// UUID used to identify replicasets by tarantool's subsystems.
    pub replicaset_uuid: String,

    /// Instance id of the current replication leader.
    pub master_id: InstanceId,

    /// Sharding weight of the replicaset.
    pub weight: weight::Info,

    /// Current schema version of the replicaset.
    pub current_schema_version: u64,
}
impl Encode for Replicaset {}

impl Replicaset {
    pub fn format() -> Vec<::tarantool::space::Field> {
        use ::tarantool::space::{Field, FieldType};
        vec![
            Field::from(("replicaset_id", FieldType::String)),
            Field::from(("replicaset_uuid", FieldType::String)),
            Field::from(("master_id", FieldType::String)),
            Field::from(("weight", FieldType::Array)),
            Field::from(("current_schema_version", FieldType::Unsigned)),
        ]
    }
}

impl std::fmt::Display for Replicaset {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "({}, master: {}, weight: {}, schema_version: {})",
            self.replicaset_id, self.master_id, self.weight, self.current_schema_version,
        )
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Replicaset weight
pub mod weight {
    /// Replicaset weight info
    #[derive(Default, Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
    pub struct Info {
        pub value: super::Weight,
        pub origin: Origin,
        pub state: State,
    }

    pub struct Value;
    impl Value {
        // FIXME: there's got to be a better way
        pub const PATH: &str = "weight[1]";
    }

    ::tarantool::define_str_enum! {
        /// Replicaset weight origin
        #[derive(Default)]
        pub enum Origin {
            /// Weight is determined by governor.
            #[default]
            Auto = "Auto",

            /// Weight is specified by user.
            User = "User",
        }
    }
    impl Origin {
        // FIXME: there's got to be a better way
        pub const PATH: &str = "weight[2]";
    }

    ::tarantool::define_str_enum! {
        /// Replicaset weight state
        #[derive(Default)]
        pub enum State {
            /// Weight is set to the inital value, which will be changed.
            #[default]
            Initial = "Initial",

            /// Weight is in progress of being updated.
            Updating = "Updating",

            /// Weight doesn't need updating.
            UpToDate = "UpToDate",
        }
    }
    impl State {
        // FIXME: there's got to be a better way
        pub const PATH: &str = "weight[3]";
    }

    impl std::fmt::Display for Info {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let Self {
                value,
                origin,
                state,
            } = self;
            write!(f, "({origin}, {state}, {value})")
        }
    }
}
