import { InstanceNodeType, InstanceType } from "shared/entity/instance";
import { sortByString } from "shared/utils/string/sort";
import {
  NodeType,
  sortTiers,
  TierNodeType,
  TierType,
} from "shared/entity/tier";
import { ReplicasetNodeType } from "shared/entity/replicaset";

import { TSortValue } from "./TopBar/SortBy/config";
import { sortByStringProp } from "./hooks";
import { TFilterByValue } from "./TopBar/FilterBy/config";

export const formatFailDomain = (domain: {
  key: string;
  value: string | string[];
}) => {
  return `${domain.key}: ${
    Array.isArray(domain.value)
      ? domain.value.slice().sort().join(", ")
      : domain.value
  }`;
};

export const formatFailDomains = (
  domains: Array<{ key: string; value: string }>
) => {
  return domains.map(formatFailDomain).sort().join(", ");
};

export const sortInstances = (
  instances?: (InstanceType | InstanceNodeType)[],
  sortBy?: TSortValue
) => {
  if (!instances) return [];

  if (!sortBy) return instances;

  switch (sortBy.by) {
    default:
    case "NAME":
      return sortByStringProp(instances, (a) => a.name, {
        order: sortBy.order,
      });

    case "FAILURE_DOMAIN":
      return [...instances].sort((a, b) => {
        const domainA = formatFailDomains(a.failureDomain);
        const domainB = formatFailDomains(b.failureDomain);

        // Fallback to sorting by-name if failure domain is empty or similar
        // Make sure to make empty instances "float to the top" by prefixing with `_`
        const domainANameStub = `_${b.name}`;
        const domainBNameStub = `_${a.name}`;

        if (domainA === domainB) {
          return sortByString(domainANameStub, domainBNameStub, {
            order: sortBy.order,
          });
        }

        return sortByString(
          domainA || domainANameStub,
          domainB || domainBNameStub,
          {
            order: sortBy.order,
          }
        );
      });
  }
};

export const filterInstances = (
  instances?: (InstanceType | InstanceNodeType)[],
  filteredBy?: TFilterByValue
) => {
  if (!instances) return [];

  if (!filteredBy) return instances;

  let filteredInstances = instances;

  if (filteredBy.domain !== undefined) {
    filteredInstances = instances.filter((instance) => {
      return filteredBy.domain?.every((domainFilter) => {
        return instance.failureDomain.find(
          (domain) =>
            domain.key === domainFilter.key &&
            domainFilter.value.includes(domain.value)
        );
      });
    });
  }

  return filteredInstances;
};

export const getInitialNodesData = (tiers: TierType[]): TierNodeType[] => {
  const tierNodes: TierNodeType[] = tiers.map((tier) => ({
    ...tier,
    syntheticId: tier.name,
    open: false,
    type: NodeType.Tier,
    replicasets: tier.replicasets.map((replicaset) => {
      const replicasetSyntheticId = `${tier.name}_${replicaset.name}`;
      return {
        ...replicaset,
        syntheticId: replicasetSyntheticId,
        open: false,
        type: NodeType.Replicaset,
        instances: replicaset.instances.map((instance) => ({
          ...instance,
          syntheticId: `${replicasetSyntheticId}_${instance.name}`,
          type: NodeType.Instance,
        })),
      };
    }),
  }));
  return tierNodes;
};
export const getNodesListByOpenedNodes = (
  tiers: TierNodeType[],
  openedNodes: string[],
  grouping?: "TIERS" | "REPLICASETS" | "INSTANCES",
  sort?: TSortValue,
  filterByValue?: TFilterByValue
): (TierNodeType | ReplicasetNodeType | InstanceNodeType)[] => {
  let list: (TierNodeType | ReplicasetNodeType | InstanceNodeType)[] = [];

  if (grouping === "INSTANCES") {
    tiers.forEach((tier) => {
      (tier.replicasets as ReplicasetNodeType[]).forEach((replicaset) => {
        (replicaset.instances as InstanceNodeType[]).forEach((instance) => {
          list.push(instance);
        });
      });
    });
    const filteredInstances = filterInstances(
      list as InstanceNodeType[],
      filterByValue
    );
    const sortedInstances = sortInstances(
      filteredInstances as InstanceNodeType[],
      sort
    );
    return sortedInstances as InstanceNodeType[];
  }

  const sortedTiers = sortTiers(tiers);
  sortedTiers.forEach((tier) => {
    const tierIsOpened = Boolean(
      openedNodes.includes(tier.syntheticId) && tier.replicasets.length
    );
    list.push({
      ...tier,
      open: tierIsOpened,
    });
    if (tierIsOpened) {
      (tier.replicasets as ReplicasetNodeType[]).forEach((replicaset) => {
        const replicasetIsOpened = Boolean(
          openedNodes.includes(replicaset.syntheticId) &&
            replicaset.instances.length
        );
        list.push({
          ...replicaset,
          open: replicasetIsOpened,
        });
        if (replicasetIsOpened) {
          list = [...list, ...(replicaset.instances as InstanceNodeType[])];
        }
      });
    }
  });
  return list;
};
