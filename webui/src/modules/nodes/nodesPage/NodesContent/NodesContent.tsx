import { Content } from "shared/ui/layout/Content/Content";
import { NoData } from "shared/ui/NoData/NoData";
import { useTiers } from "shared/entity/tier";

import { ReplicasetCard } from "./ReplicasetCard/ReplicasetCard";
import { InstanceCard } from "./ReplicasetCard/instanceBlock/InstanceCard";
import { TopBar } from "./TopBar/TopBar";
import { useGroupByFilter } from "./TopBar/GroupByFilter/hooks";
import { useSortBy } from "./TopBar/SortBy/hooks";
import { useFilteredInstances, useSortedInstances } from "./hooks";
import { useFilterBy } from "./TopBar/FilterBy/hooks";
import { TierCard } from "./TierCard/TierCard";

import styles from "./NodesContent.module.scss";

export const NodesContent = () => {
  const { data } = useTiers();

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

  const groupedByTiers = groupByFilterValue === "TIERS";
  const groupedByReplicates = groupByFilterValue === "REPLICASETS";
  const groupedByInstances = groupByFilterValue === "INSTANCES";

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
            showSortBy={groupedByInstances}
            setSortByValue={setSortByValue}
            showFilterBy={groupedByInstances}
            filterByValue={filterByValue}
            setFilterByValue={setFilterByValue}
          />
          <div className={styles.list}>
            {groupedByTiers &&
              data?.tiers.map((tier) => (
                <TierCard key={tier.name} tier={tier} />
              ))}
            {groupedByReplicates &&
              data?.replicasets.map((rep) => (
                <ReplicasetCard key={rep.id} replicaset={rep} />
              ))}
            {groupedByInstances &&
              sortedFilteredInstances.map((instance) => (
                <InstanceCard
                  key={instance.name}
                  instance={instance}
                  theme="primary"
                />
              ))}
          </div>
        </>
      )}
    </Content>
  );
};
