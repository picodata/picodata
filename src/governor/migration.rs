use crate::traft::Migration;
use crate::traft::Replicaset;
use crate::traft::ReplicasetId;

pub(crate) fn waiting_migrations<'a>(
    migrations: &'a mut [Migration],
    replicasets: &'a [Replicaset],
    desired_schema_version: u64,
) -> Vec<(u64, Vec<ReplicasetId>)> {
    migrations.sort_by(|a, b| a.id.partial_cmp(&b.id).unwrap());
    let mut res: Vec<(u64, Vec<ReplicasetId>)> = Vec::new();
    for m in migrations {
        let mut rs: Vec<ReplicasetId> = Vec::new();
        for r in replicasets {
            if r.current_schema_version < m.id && m.id <= desired_schema_version {
                rs.push(r.replicaset_id.clone());
            }
        }
        if !rs.is_empty() {
            res.push((m.id, rs));
        }
    }
    res
}

#[cfg(test)]
mod tests {
    use crate::traft::InstanceId;
    use crate::traft::Migration;
    use crate::traft::Replicaset;
    use crate::traft::ReplicasetId;

    use super::waiting_migrations;

    macro_rules! m {
        ($id:literal, $body:literal) => {
            Migration {
                id: $id,
                body: $body.to_string(),
            }
        };
    }

    macro_rules! r {
        ($id:literal, $schema_version:literal) => {
            Replicaset {
                replicaset_id: ReplicasetId($id.to_string()),
                replicaset_uuid: "".to_string(),
                master_id: InstanceId("i0".to_string()),
                current_weight: 1.0,
                target_weight: 1.0,
                current_schema_version: $schema_version,
            }
        };
    }

    macro_rules! expect {
        [$(($migration_id:literal, $($replicaset_id:literal),*)),*] => [vec![
            $((
                $migration_id,
                vec![$(ReplicasetId($replicaset_id.to_string())),*].to_vec()
            )),*
        ].to_vec()];
    }

    #[test]
    fn test_waiting_migrations() {
        let ms = vec![m!(1, "m1"), m!(2, "m2"), m!(3, "m3")].to_vec();
        let rs = vec![r!("r1", 0), r!("r2", 2), r!("r3", 1)].to_vec();
        assert_eq!(waiting_migrations(&mut ms.clone(), &rs, 0), expect![]);
        assert_eq!(
            waiting_migrations(&mut ms.clone(), &rs, 1),
            expect![(1, "r1")]
        );
        assert_eq!(
            waiting_migrations(&mut ms.clone(), &rs, 2),
            expect![(1, "r1"), (2, "r1", "r3")]
        );
        assert_eq!(
            waiting_migrations(&mut ms.clone(), &rs, 3),
            expect![(1, "r1"), (2, "r1", "r3"), (3, "r1", "r2", "r3")]
        );
    }
}
