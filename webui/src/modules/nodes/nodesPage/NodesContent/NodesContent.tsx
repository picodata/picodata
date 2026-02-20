import { memo, useEffect, useState } from "react";
import { Virtuoso } from "react-virtuoso";

import { TierNodeType, TierType } from "shared/entity/tier";
import { useTranslation } from "shared/intl";
import { ReplicasetNodeType, ReplicasetType } from "shared/entity/replicaset";
import { InstanceNodeType, InstanceType } from "shared/entity/instance";

import { useGroupByFilter } from "./TopBar/GroupByFilter/hooks";
import { useSortBy } from "./TopBar/SortBy/hooks";
import { useFilterBy } from "./TopBar/FilterBy/hooks";
import { getInitialNodesData, getNodesListByOpenedNodes } from "./utils";
import { NodesFork } from "./NodesFork";
import { ContentWrapper } from "./Content";
import { NodesNoData } from "./StyledComponents";

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
  const [filterByValue, setFilterByValue] = useFilterBy();
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
        groupByFilterValue,
        sortByValue,
        filterByValue
      )
    );
  }, [nodesData, openedNodes, groupByFilterValue, sortByValue, filterByValue]);

  useEffect(() => {
    setNodesData(getInitialNodesData(data?.tiers || []));
  }, [data?.tiers]);

  const nodeClickHandler = (id: string) => {
    setOpenedNodes((_openedNodes) => {
      if (_openedNodes.includes(id)) {
        return _openedNodes.filter((_id) => _id !== id);
      }
      return [..._openedNodes, id];
    });
  };

  return (
    <>
      <ContentWrapper
        groupByFilterValue={groupByFilterValue}
        setGroupByFilterValue={setGroupByFilterValue}
        sortByValue={sortByValue}
        setFilterByValue={setFilterByValue}
        filterByValue={filterByValue}
        setSortByValue={setSortByValue}
      >
        {nodesList?.length ? (
          <Virtuoso
            style={{
              height: "100%",
              width: "100%",
            }}
            totalCount={nodesList?.length}
            data={nodesList}
            itemContent={(index, node) => {
              return (
                <NodesFork
                  key={node.syntheticId}
                  nodesList={nodesList || []}
                  index={index}
                  node={node}
                  onClick={nodeClickHandler}
                  fromReplicaset={groupedByTiers}
                />
              );
            }}
          />
        ) : (
          <NodesNoData>{instancesTranslations.noData.text}</NodesNoData>
        )}
      </ContentWrapper>
    </>
  );
});
