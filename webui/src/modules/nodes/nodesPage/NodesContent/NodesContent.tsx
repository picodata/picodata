import { memo, useCallback, useEffect, useState } from "react";
import { Virtuoso } from "react-virtuoso";

import { TierNodeType, TierType } from "shared/entity/tier";
import { useTranslation } from "shared/intl";
import { ReplicasetNodeType, ReplicasetType } from "shared/entity/replicaset";
import { InstanceNodeType, InstanceType } from "shared/entity/instance";

import { useGroupByFilter } from "./TopBar/GroupByFilter/hooks";
import { useSortBy } from "./TopBar/SortBy/hooks";
import { getInitialNodesData, getNodesListByOpenedNodes } from "./utils";
import { NodesFork } from "./NodesFork";
import { ContentWrapper } from "./Content";
import { NodesNoData } from "./StyledComponents";
import { useFilterTags, useFilterValue } from "./hooks";

type NodesContentProps = {
  data?: {
    tiers: TierType[];
    replicasets: ReplicasetType[];
    instances: InstanceType[];
  };
};
export const NodesContent = memo(({ data }: NodesContentProps) => {
  const [groupByFilterValue, setGroupByFilterValue] = useGroupByFilter();
  const [sortByValue, setSortByValue] = useSortBy();
  const filterTags = useFilterTags(data);
  const [filterValue, setFilterValue] = useFilterValue();
  const { translation } = useTranslation();
  const instancesTranslations = translation.pages.instances;
  const groupedByTiers = groupByFilterValue === "TIERS";

  const [nodesData, setNodesData] = useState(
    getInitialNodesData(data?.tiers || [])
  );
  const [nodesList, setNodesList] =
    useState<(TierNodeType | ReplicasetNodeType | InstanceNodeType)[]>();
  const [openedNodes, setOpenedNodes] = useState<string[]>([]);

  useEffect(() => {
    setNodesList(
      getNodesListByOpenedNodes(
        nodesData,
        openedNodes,
        filterValue,
        groupByFilterValue,
        sortByValue
      )
    );
  }, [nodesData, openedNodes, groupByFilterValue, sortByValue, filterValue]);

  useEffect(() => {
    setNodesData(getInitialNodesData(data?.tiers || []));
  }, [data?.tiers]);

  const nodeClickHandler = useCallback((id: string) => {
    setOpenedNodes((_openedNodes) => {
      if (_openedNodes.includes(id)) {
        return _openedNodes.filter((_id) => _id !== id);
      }
      return [..._openedNodes, id];
    });
  }, []);

  const itemContent = useCallback(
    (
      index: number,
      node: TierNodeType | ReplicasetNodeType | InstanceNodeType
    ) => (
      <NodesFork
        key={node.syntheticId}
        nodesList={nodesList || []}
        index={index}
        node={node}
        onClick={nodeClickHandler}
        fromReplicaset={groupedByTiers}
      />
    ),
    [nodesList, nodeClickHandler, groupedByTiers]
  );

  return (
    <>
      <ContentWrapper
        groupByFilterValue={groupByFilterValue}
        setGroupByFilterValue={setGroupByFilterValue}
        sortByValue={sortByValue}
        setSortByValue={setSortByValue}
        filterTags={filterTags}
        filterValue={filterValue}
        onFilterValueChange={setFilterValue}
      >
        {nodesList?.length ? (
          <Virtuoso
            style={{
              height: "100%",
              width: "100%",
            }}
            totalCount={nodesList?.length}
            data={nodesList}
            itemContent={itemContent}
          />
        ) : (
          <NodesNoData>{instancesTranslations.noData.text}</NodesNoData>
        )}
      </ContentWrapper>
    </>
  );
});
