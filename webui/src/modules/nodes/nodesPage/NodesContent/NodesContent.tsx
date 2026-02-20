import { Content } from "shared/ui/layout/Content/Content";
import { NoData } from "shared/ui/NoData/NoData";
import { TierType, useTiers } from "shared/entity/tier";
import { useTranslation } from "shared/intl";
import { ReplicasetType } from "shared/entity/replicaset";
import { InstanceType } from "shared/entity/instance";

import { ReplicasetCard } from "./ReplicasetCard/ReplicasetCard";
import { InstanceCard } from "./ReplicasetCard/instanceBlock/InstanceCard";
import { TopBar } from "./TopBar/TopBar";
import { useGroupByFilter } from "./TopBar/GroupByFilter/hooks";
import { useSortBy } from "./TopBar/SortBy/hooks";
import {
  sortByStringProp,
  useFilteredInstances,
  useSortedByString,
  useSortedInstances,
} from "./hooks";
import { useFilterBy } from "./TopBar/FilterBy/hooks";
import { TierCard } from "./TierCard/TierCard";
import { TFilterByValue } from "./TopBar/FilterBy/config";
import { TSortValue } from "./TopBar/SortBy/config";
import { gridWrapperSx, List, topBarSx } from "./StyledComponents";

export const NodesContent = () => {
  const { data } = useTiers();

  const [groupByFilterValue, setGroupByFilterValue] = useGroupByFilter();
  const [sortByValue, setSortByValue] = useSortBy();
  const [filterByValue, setFilterByValue] = useFilterBy();

  const { translation } = useTranslation();
  const instancesTranslations = translation.pages.instances;

  const groupedByTiers = groupByFilterValue === "TIERS";
  const groupedByReplicasets = groupByFilterValue === "REPLICASETS";
  const groupedByInstances = groupByFilterValue === "INSTANCES";

  const isNoData = data?.replicasets.length === 0;

  return (
    <Content sx={gridWrapperSx}>
      {isNoData ? (
        <NoData>{instancesTranslations.noData.text}</NoData>
      ) : (
        <>
          <TopBar
            sx={topBarSx}
            groupByFilterValue={groupByFilterValue}
            setGroupByFilterValue={setGroupByFilterValue}
            sortByValue={sortByValue}
            showSortBy={groupedByInstances}
            setSortByValue={setSortByValue}
            showFilterBy={groupedByInstances}
            filterByValue={filterByValue}
            setFilterByValue={setFilterByValue}
          />
          <List>
            {groupedByTiers && <Tiers tiers={data?.tiers} />}
            {groupedByReplicasets && (
              <Replicasets replicasets={data?.replicasets} />
            )}
            {groupedByInstances && (
              <Instances
                instances={data?.instances}
                filterByValue={filterByValue}
                sortByValue={sortByValue}
              />
            )}
          </List>
        </>
      )}
    </Content>
  );
};

function Tiers({ tiers }: { tiers?: TierType[] }) {
  const sortedTiers = useSortedByString(
    tiers?.map((tier) => ({
      ...tier,
      replicasets: sortByStringProp(tier.replicasets, (x) => x.name),
    })),
    (x) => x.name
  );

  return (
    <>
      {sortedTiers.map((tier) => (
        <TierCard key={tier.name} tier={tier} />
      ))}
    </>
  );
}

function Replicasets({ replicasets }: { replicasets?: ReplicasetType[] }) {
  const sortedRepicasets = useSortedByString(replicasets, (x) => x.name);

  return (
    <>
      {sortedRepicasets.map((rep) => (
        <ReplicasetCard key={rep.name} replicaset={rep} />
      ))}
    </>
  );
}

function Instances({
  instances,
  filterByValue,
  sortByValue,
}: {
  instances?: InstanceType[];
  filterByValue?: TFilterByValue;
  sortByValue?: TSortValue;
}) {
  const filteredInstances = useFilteredInstances(instances, filterByValue);
  const sortedFilteredInstances = useSortedInstances(
    filteredInstances,
    sortByValue
  );

  return (
    <>
      {sortedFilteredInstances.map((instance) => (
        <InstanceCard key={instance.name} instance={instance} theme="primary" />
      ))}
    </>
  );
}
