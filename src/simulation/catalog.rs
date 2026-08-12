use crate::catalog::pico_bucket::PicoBucket;
use crate::catalog::pico_resharding_state::PicoReshardingState;
use crate::simulation::instance::PretendInstance;
use crate::storage::do_dml_on_space;
use crate::storage::Catalog;
use crate::storage::Instances;
use crate::storage::Replicasets;
use crate::storage::ServiceRouteTable;
use crate::storage::SystemTable;
use crate::storage::Tiers;
use crate::topology_cache::TopologyChange;
use crate::traft::op::Dml;
use tarantool::space::Space;
use tarantool::space::SpaceId;
use tarantool::space::SpaceType;

/// Creates a set of temporary system tables for a given instance.
pub fn create_pretend_catalog(instance_name: &str) -> Catalog {
    // Idempotent, and every instance needs the same real tables underneath
    // its own copies.
    let mut catalog = Catalog::for_tests();

    let space_id = create_pretend_space::<Instances>(instance_name);
    catalog.instances = Instances::with_id(space_id);

    let space_id = create_pretend_space::<Replicasets>(instance_name);
    catalog.replicasets = Replicasets::with_id(space_id);

    let space_id = create_pretend_space::<Tiers>(instance_name);
    catalog.tiers = Tiers::with_id(space_id);

    let space_id = create_pretend_space::<ServiceRouteTable>(instance_name);
    catalog.service_route_table = ServiceRouteTable::with_id(space_id);

    let space_id = create_pretend_space::<PicoBucket>(instance_name);
    catalog.pico_bucket = PicoBucket::with_id(space_id);

    let space_id = create_pretend_space::<PicoReshardingState>(instance_name);
    catalog.pico_resharding_state = PicoReshardingState::with_id(space_id);

    catalog
}

pub fn create_pretend_space<T: SystemTable>(instance_name: &str) -> SpaceId {
    let space_name = format!("{}_{instance_name}", T::TABLE_NAME);
    let (space, _indexes) = T::create_space(&space_name, None, SpaceType::Temporary)
        .expect("simulation: instance's copy of a system table must be creatable");

    // Left over from whichever earlier test last had an instance by this name.
    space
        .truncate()
        .expect("simulation: instance's copy must be truncatable");

    space.id()
}

/// Apply the `dml` to the given instance's pretend storage and update
/// instance's topology cache.
pub fn apply_dml(instance: &PretendInstance, dml: &Dml) {
    let table_id = dml.table_id();
    let space = catalog_space_by_id(&instance.catalog, table_id);
    let (old, new) =
        do_dml_on_space(space, dml, true).expect("simulation: committed DML must apply");

    let decoded = TopologyChange::decode(table_id, old.as_ref(), new.as_ref())
        .expect("simulation: a committed DML must decode");
    let Some(decoded) = decoded else {
        panic!("simulation: table #{table_id} isn't cached by the TopologyCache");
    };

    instance.topology.update(decoded);
}

pub fn catalog_space_by_id(catalog: &Catalog, table_id: SpaceId) -> &Space {
    use crate::catalog::pico_bucket::PicoBucket;
    use crate::catalog::pico_resharding_state::PicoReshardingState;

    match table_id {
        Instances::TABLE_ID => &catalog.instances.space,
        Replicasets::TABLE_ID => &catalog.replicasets.space,
        Tiers::TABLE_ID => &catalog.tiers.space,
        ServiceRouteTable::TABLE_ID => &catalog.service_route_table.space,
        PicoBucket::TABLE_ID => &catalog.pico_bucket.space,
        PicoReshardingState::TABLE_ID => &catalog.pico_resharding_state.space,
        _ => {
            let table_name = Catalog::system_space_name_by_id(table_id).unwrap_or("?");
            panic!(
                "simulation: no PretendInstance storage for table {table_name} \
                 (#{table_id}), add it to `create_pretend_catalog`"
            );
        }
    }
}
