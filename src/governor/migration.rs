use crate::replicaset::Replicaset;

pub(crate) fn get_pending_migration<'r>(
    mut migration_ids: Vec<u64>,
    replicasets: &[&'r Replicaset],
    desired_schema_version: u64,
) -> Option<(u64, &'r Replicaset)> {
    migration_ids.sort();
    for m_id in migration_ids {
        for r in replicasets {
            if r.current_schema_version < m_id && m_id <= desired_schema_version {
                return Some((m_id, r));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(non_snake_case)]
    fn R(rid: &str, mid: u64) -> Replicaset {
        Replicaset {
            replicaset_id: rid.into(),
            replicaset_uuid: Default::default(),
            master_id: String::default().into(),
            weight: Default::default(),
            current_schema_version: mid,
        }
    }

    #[test]
    fn test_waiting_migrations() {
        let ms = vec![3, 2, 1];
        let rs = [R("r1", 0), R("r2", 2), R("r3", 1)];
        let rs = rs.iter().collect::<Vec<_>>();
        assert_eq!(get_pending_migration(ms.clone(), &rs, 0), None);

        let (m, r) = get_pending_migration(ms.clone(), &rs, 1).unwrap();
        assert_eq!((m, &*r.replicaset_id), (1, "r1"));

        let rs = [R("r1", 1), R("r2", 2), R("r3", 1)];
        let rs = rs.iter().collect::<Vec<_>>();
        assert_eq!(get_pending_migration(ms.clone(), &rs, 1), None);

        let (m, r) = get_pending_migration(ms.clone(), &rs, 2).unwrap();
        assert_eq!((m, &*r.replicaset_id), (2, "r1"));

        let rs = [R("r1", 2), R("r2", 2), R("r3", 1)];
        let rs = rs.iter().collect::<Vec<_>>();
        let (m, r) = get_pending_migration(ms.clone(), &rs, 2).unwrap();
        assert_eq!((m, &*r.replicaset_id), (2, "r3"));

        let rs = [R("r1", 2), R("r2", 2), R("r3", 2)];
        let rs = rs.iter().collect::<Vec<_>>();
        assert_eq!(get_pending_migration(ms.clone(), &rs, 2), None);

        let (m, r) = get_pending_migration(ms.clone(), &rs, 3).unwrap();
        assert_eq!((m, &*r.replicaset_id), (3, "r1"));

        let rs = [R("r1", 3), R("r2", 2), R("r3", 2)];
        let rs = rs.iter().collect::<Vec<_>>();
        let (m, r) = get_pending_migration(ms.clone(), &rs, 99).unwrap();
        assert_eq!((m, &*r.replicaset_id), (3, "r2"));

        let rs = [R("r1", 3), R("r2", 3), R("r3", 2)];
        let rs = rs.iter().collect::<Vec<_>>();
        let (m, r) = get_pending_migration(ms.clone(), &rs, 99).unwrap();
        assert_eq!((m, &*r.replicaset_id), (3, "r3"));

        let rs = [R("r1", 3), R("r2", 3), R("r3", 3)];
        let rs = rs.iter().collect::<Vec<_>>();
        assert_eq!(get_pending_migration(ms.clone(), &rs, 99), None);
    }
}
