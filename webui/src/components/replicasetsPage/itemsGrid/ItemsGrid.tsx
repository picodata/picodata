import { ReplicasetCard } from "../replicasetCard/ReplicasetCard";
import { useDispatch, useSelector } from "react-redux";
import { useEffect, useMemo } from "react";
import { getReplicasets } from "store/slices/clusterSlice";
import { AppDispatch, RootState } from "store";
import { Filters } from "./Filters/Filters";
import { useGroupByFilter } from "./Filters/GroupByFilter/hooks";
import { InstanceCard } from "../replicasetCard/instanceBlock/InstanceCard";

import styles from "./ItemsGrid.module.scss";

export const ItemsGrid = ({}) => {
  const dispatch = useDispatch<AppDispatch>();
  const replicasets = useSelector(
    (state: RootState) => state.cluster.replicasets
  );
  useEffect(() => {
    dispatch(getReplicasets());
  }, [dispatch]);

  const [groupByFilterValue, setGroupByFilterValue] = useGroupByFilter();

  // todo нужен уникальный id для instances
  const instances = useMemo(() => {
    return replicasets.map((replicaset) => replicaset.instances).flat(1);
  }, [replicasets]);

  return (
    <div className={styles.gridWrapper}>
      <Filters
        groupByFilterValue={groupByFilterValue}
        setGroupByFilterValue={setGroupByFilterValue}
      />
      <div className={styles.replicasetsWrapper}>
        {groupByFilterValue === "REPLICASETS" &&
          replicasets.map((rep) => (
            <ReplicasetCard key={rep.id} replicaset={rep} />
          ))}
        {groupByFilterValue === "INSTANCES" &&
          instances.map((instance) => (
            <InstanceCard
              key={instance.name}
              instance={instance}
              theme="secondary"
            />
          ))}
      </div>
    </div>
  );
};
