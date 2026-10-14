use crate::column_name;
use crate::replicaset::Replicaset;
use crate::storage::Replicasets;
use crate::storage::SystemTable;
use crate::traft::op::Dml;
use crate::Result;
use tarantool::space::UpdateOps;

pub fn make_actualization_dmls(replicaset: &Replicaset) -> Result<Vec<Dml>> {
    let mut dmls = vec![];

    let dml = Replicasets::dml_update(
        &[&replicaset.name],
        UpdateOps::new()
            // Actualize replicaset's bucket distribution version
            .into_assign(
                column_name!(Replicaset, current_bucket_state_version),
                replicaset.target_bucket_state_version,
            )?,
    );
    dmls.push(dml);

    Ok(dmls)
}
