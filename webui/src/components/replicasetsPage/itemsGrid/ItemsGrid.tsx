import { ReplicasetCard } from "../replicasetCard/ReplicasetCard";
import { useDispatch, useSelector } from "react-redux";
import { useEffect } from "react";
import { getReplicasets } from "store/slices/clusterSlice";
import { AppDispatch, RootState } from "store";
import { TopBar } from "./TopBar/TopBar";
import { useGroupByFilter } from "./TopBar/GroupByFilter/hooks";
import { InstanceCard } from "../replicasetCard/instanceBlock/InstanceCard";
import { useSortBy } from "./TopBar/SortBy/hooks";

import { useFilteredInstances, useSortedInstances } from "./hooks";
import { useFilterBy } from "./TopBar/FilterBy/hooks";

import styles from "./ItemsGrid.module.scss";

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
  const [filterByValue, setFilterByValue] = useFilterBy();

  const filteredInstances = useFilteredInstances(instances, filterByValue);
  const sortedFilteredInstances = useSortedInstances(
    filteredInstances,
    sortByValue
  );

  const groupedByReplicates = groupByFilterValue === "REPLICASETS";

  return (
    <div className={styles.gridWrapper}>
      <TopBar
        groupByFilterValue={groupByFilterValue}
        setGroupByFilterValue={setGroupByFilterValue}
        sortByValue={sortByValue}
        showSortBy={!groupedByReplicates}
        setSortByValue={setSortByValue}
        showFilterBy={!groupedByReplicates}
        filterByValue={filterByValue}
        setFilterByValue={setFilterByValue}
      />
      <div className={styles.replicasetsWrapper}>
        {groupedByReplicates &&
          replicasets.map((rep) => (
            <ReplicasetCard key={rep.id} replicaset={rep} />
          ))}
        {!groupedByReplicates &&
          sortedFilteredInstances.map((instance) => (
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
