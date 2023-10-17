import { ReplicasetCard } from "../replicasetCard/ReplicasetCard";
import { useDispatch, useSelector } from "react-redux";
import { useEffect } from "react";
import { getReplicasets } from "store/slices/clusterSlice";
import { AppDispatch, RootState } from "store";
import { Filters } from "./Filters/Filters";
import { useGroupByFilter } from "./Filters/GroupByFilter/hooks";
import { InstanceCard } from "../replicasetCard/instanceBlock/InstanceCard";
import { useSortBy } from "./Filters/SortBy/hooks";

import styles from "./ItemsGrid.module.scss";
import { useSortedInstances } from "./hooks";

export const ItemsGrid = ({}) => {
  const dispatch = useDispatch<AppDispatch>();
  const { replicasets, instances } = useSelector((state: RootState) => {
    return {
      replicasets: state.cluster.replicasets,
      instances: state.cluster.instances,
    };
  });

  useEffect(() => {
    dispatch(getReplicasets());
  }, [dispatch]);

  const [groupByFilterValue, setGroupByFilterValue] = useGroupByFilter();
  const [sortByValue, setSortByValue] = useSortBy();

  const sortedInstances = useSortedInstances(instances, sortByValue);

  const groupedByReplicates = groupByFilterValue === "REPLICASETS";

  return (
    <div className={styles.gridWrapper}>
      <Filters
        groupByFilterValue={groupByFilterValue}
        setGroupByFilterValue={setGroupByFilterValue}
        sortByValue={sortByValue}
        showSortBy={!groupedByReplicates}
        setSortByValue={setSortByValue}
        showFilterBy={!groupedByReplicates}
      />
      <div className={styles.replicasetsWrapper}>
        {groupedByReplicates &&
          replicasets.map((rep) => (
            <ReplicasetCard key={rep.id} replicaset={rep} />
          ))}
        {!groupedByReplicates &&
          sortedInstances.map((instance) => (
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
