import { memo } from "react";

import { NodeType, TierNodeType } from "../../../../shared/entity/tier";
import { ReplicasetNodeType } from "../../../../shared/entity/replicaset";
import { InstanceNodeType } from "../../../../shared/entity/instance";

import { TierCardAlt } from "./TierCard/TierCard";
import { ReplicasetCardAlt } from "./ReplicasetCard/ReplicasetCard";
import { InstanceCardAlt } from "./ReplicasetCard/instanceBlock/InstanceCard";

type NodesForkProps = {
  nodesList: (TierNodeType | ReplicasetNodeType | InstanceNodeType)[];
  index: number;
  node: TierNodeType | ReplicasetNodeType | InstanceNodeType;
  onClick: (id: string) => void;
  fromReplicaset: boolean;
};
export const NodesFork = memo(
  ({ nodesList, index, node, onClick, fromReplicaset }: NodesForkProps) => {
    const nextNode = nodesList[index + 1];
    const nextNodeIsTierType = nextNode && nextNode.type === NodeType.Tier;
    const nextNodeIsReplicasetType =
      nextNode && nextNode.type === NodeType.Replicaset;

    switch (node.type) {
      case NodeType.Tier:
        return (
          <TierCardAlt
            isFirst={index === 0}
            isLast={!nextNode}
            tier={node}
            onClick={onClick}
          />
        );
      case NodeType.Replicaset:
        return (
          <>
            <ReplicasetCardAlt
              replicaset={node}
              isLast={!nextNode}
              onClick={onClick}
              nextNodeIsTierType={nextNodeIsTierType}
            />
          </>
        );
      default:
        return (
          <>
            <InstanceCardAlt
              isLast={!nextNode}
              instance={node}
              fromReplicaset={fromReplicaset}
              nextNodeIsTierType={nextNodeIsTierType}
              nextNodeIsReplicasetType={nextNodeIsReplicasetType}
            />
          </>
        );
    }
  }
);
