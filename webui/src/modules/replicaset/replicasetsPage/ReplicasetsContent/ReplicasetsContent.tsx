import { Content } from "shared/ui/layout/Content/Content";
import { NoData } from "shared/ui/NoData/NoData";
import { useReplicasets } from "shared/entity/replicaset/list";

import { ReplicasetCard } from "./ReplicasetCard/ReplicasetCard";
import { InstanceCard } from "./ReplicasetCard/instanceBlock/InstanceCard";
import { TopBar } from "./TopBar/TopBar";
import { useGroupByFilter } from "./TopBar/GroupByFilter/hooks";
import { useSortBy } from "./TopBar/SortBy/hooks";
import { useFilteredInstances, useSortedInstances } from "./hooks";
import { useFilterBy } from "./TopBar/FilterBy/hooks";

import styles from "./ReplicasetsContent.module.scss";

export const ReplicasetsContent = () => {
  const { data } = useReplicasets();

  const [groupByFilterValue, setGroupByFilterValue] = useGroupByFilter();
  const [sortByValue, setSortByValue] = useSortBy();
  const [filterByValue, setFilterByValue] = useFilterBy();

  const filteredInstances = useFilteredInstances(
    data?.instances,
    filterByValue
  );
  const sortedFilteredInstances = useSortedInstances(
    filteredInstances,
    sortByValue
  );

  const groupedByReplicates = groupByFilterValue === "REPLICASETS";

  const isNoData = data?.replicasets.length === 0;

  return (
    <Content className={styles.gridWrapper}>
      {isNoData ? (
        <NoData>No Data</NoData>
      ) : (
        <>
          <TopBar
            className={styles.topBar}
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
              data?.replicasets.map((rep) => (
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
        </>
      )}
    </Content>
  );
};
