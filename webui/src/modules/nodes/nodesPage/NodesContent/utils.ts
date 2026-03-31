import AirlineStopsIcon from "@mui/icons-material/AirlineStops";
import StorageIcon from "@mui/icons-material/Storage";
import LayersIcon from "@mui/icons-material/Layers";
import DynamicFeedIcon from "@mui/icons-material/DynamicFeed";
import AltRouteIcon from "@mui/icons-material/AltRoute";
import AlbumIcon from "@mui/icons-material/Album";
import CircleIcon from "@mui/icons-material/Circle";
import { v4 as uuidv4 } from 'uuid';
import { InstanceNodeType, InstanceType } from "shared/entity/instance";
import { sortByString } from "shared/utils/string/sort";
import {
  NodeType,
  sortTiers,
  TierNodeType,
  TierType,
} from "shared/entity/tier";
import { ReplicasetNodeType, ReplicasetType } from "shared/entity/replicaset";

import {
  ExpressionEnum,
  FilterProps,
  FilterValue,
  SEARCH_TEXT_KEY,
  TagOption,
} from "../../../../shared/ui/Filter";
import { TIntlContext } from "../../../../shared/intl";

import { TSortValue } from "./TopBar/SortBy/config";
import { sortByStringProp } from "./hooks";

const getTagOptions = <T extends TierType | ReplicasetType | InstanceType>(
  items: T[],
  keys: (keyof T)[]
): TagOption[][] => {
  const keysMap = new Map<keyof T, Set<string>>();
  items.forEach((item) => {
    keys.forEach((key) => {
      let currentSet = keysMap.get(key);
      if (!currentSet) {
        currentSet = new Set();
      }
      currentSet.add(String(item[key]));
      keysMap.set(key, currentSet);
    });
  });

  return keys.map((key) => {
    return Array.from(keysMap.get(key) as Set<string>).map((value) => ({
      value,
      label: value,
    }));
  });
};

const getFailureDomainTags = (
  instances: InstanceType[],
  translation: TIntlContext["translation"]
) => {
  const prevDomainKeysSet = new Set<string>();
  instances.forEach(({ failureDomain }) => {
    failureDomain.forEach(({ key }) => prevDomainKeysSet.add(key));
  });
  return Array.from(prevDomainKeysSet).map((key) => {
    const optionSet = new Set<string>();
    instances.forEach(({ failureDomain }) => {
      failureDomain.forEach(({ key: _key, value }) => {
        if (_key === key) {
          optionSet.add(value);
        }
      });
    });
    const options = Array.from(optionSet).map((value) => ({
      value,
      label: value,
    }));
    const currentTag = {
      id: uuidv4(),
      key: key,
      label: `${translation.components.filterTags.failureDomain} - ${key}`,
      icon: AirlineStopsIcon,
      options,
    };
    return currentTag;
  });
};
export const getFilterTags = (
  data:
    | {
        tiers: TierType[];
        replicasets: ReplicasetType[];
        instances: InstanceType[];
      }
    | undefined,
  translation: TIntlContext["translation"]
): FilterProps["tags"] => {
  if (!data) {
    return [];
  }
  const { tiers, replicasets, instances } = data;

  const failureDomainTags = getFailureDomainTags(instances, translation).sort(
    (a, b) => (a.key > b.key ? -1 : 1)
  );

  const [tierNameOptions] = getTagOptions(tiers, ["name"]);
  const [replicasetNameOptions, replicasetStateOptions] = getTagOptions(
    replicasets,
    ["name", "state"]
  );
  const [
    instanceNameOptions,
    instanceVersionOptions,
    instanceCurrentStateOptions,
  ] = getTagOptions(instances, ["name", "version", "currentState"]);
  return [
    {
      id: uuidv4(),
      key: "tier",
      label: translation.components.filterTags.tier,
      icon: StorageIcon,
      options: tierNameOptions,
    },
    {
      id: uuidv4(),
      key: "replicaset",
      label: translation.components.filterTags.replicaset,
      icon: LayersIcon,
      options: replicasetNameOptions,
    },
    {
      id: uuidv4(),
      key: "instance",
      label: translation.components.filterTags.instance,
      icon: DynamicFeedIcon,
      options: instanceNameOptions,
    },
    {
      id: uuidv4(),
      key: "version",
      label: translation.components.filterTags.version,
      icon: AltRouteIcon,
      options: instanceVersionOptions,
    },
    {
      id: uuidv4(),
      key: "leaderState",
      label: translation.components.filterTags.leaderState,
      icon: CircleIcon,
      options: replicasetStateOptions,
    },
    {
      id: uuidv4(),
      key: "currentState",
      label: translation.components.filterTags.currentState,
      icon: AlbumIcon,
      options: instanceCurrentStateOptions,
    },
    ...failureDomainTags,
  ];
};

export const formatFailDomain = (domain: {
  key: string;
  value: string | string[];
}) => {
  return `${domain.key}: ${
    Array.isArray(domain.value)
      ? domain.value
          .slice()
          .sort((a, b) => a.localeCompare(b))
          .join(", ")
      : domain.value
  }`;
};

export const formatFailDomains = (
  domains: Array<{ key: string; value: string }>
) => {
  return domains
    .map(formatFailDomain)
    .sort((a, b) => a.localeCompare(b))
    .join(", ");
};

export const sortInstances = (
  instances?: (InstanceType | InstanceNodeType)[],
  sortBy?: TSortValue
) => {
  if (!instances) return [];

  if (!sortBy) return instances;

  switch (sortBy.by) {
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
    default:
  }
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

const filterFailureDomainByExpressionType = (
  fdFilterValue: FilterValue,
  instance: InstanceNodeType
) => {
  switch (fdFilterValue.expression.type) {
    case ExpressionEnum.Is:
      return instance.failureDomain.some(
        ({ key, value }) =>
          fdFilterValue.tagKey === key && fdFilterValue.value === value
      );
    case ExpressionEnum.IsNotOneOf:
      return instance.failureDomain.some(
        ({ key, value }) =>
          fdFilterValue.tagKey === key && fdFilterValue.value !== value
      );
    case ExpressionEnum.IsOneOf:
      return instance.failureDomain.some(
        ({ key, value }) =>
          fdFilterValue.tagKey === key &&
          (fdFilterValue.value as string[]).includes(value)
      );
    default:
      return true;
  }
};

const filterValueByExpressionType = (
  expressionType: ExpressionEnum,
  valueA: unknown,
  valueB: unknown | unknown[]
) => {
  switch (expressionType) {
    case ExpressionEnum.Is:
      return valueA === valueB;
    case ExpressionEnum.IsNotOneOf:
      return valueA !== valueB;
    case ExpressionEnum.IsOneOf:
      return (valueB as unknown[]).includes(valueA);
    default:
      return true;
  }
};
const filterNodeByExpressionType = <
  T extends TierNodeType | ReplicasetNodeType | InstanceNodeType
>(
  node: T,
  key: keyof T,
  value: unknown | unknown[],
  expressionType: ExpressionEnum
) => {
  return filterValueByExpressionType(expressionType, node[key], value);
};

const tagKeyMapping: Record<string, string> = {
  tier: "name",
  replicaset: "name",
  leaderState: "state",
  instance: "name",
  version: "version",
  currentState: "currentState",
  [SEARCH_TEXT_KEY]: SEARCH_TEXT_KEY,
};

const getIncludesFilterValue = <
  T extends TierNodeType | ReplicasetNodeType | InstanceNodeType
>(
  filterValues: FilterValue[],
  node: T
) => {
  return filterValues.every((filterValueItem) => {
    const itemKey = tagKeyMapping[filterValueItem.tagKey] as keyof T;
    return filterNodeByExpressionType(
      node,
      itemKey,
      filterValueItem.value,
      filterValueItem.expression.type
    );
  });
};

const getIncludesSearchText = (
  node: TierNodeType | ReplicasetNodeType | InstanceNodeType,
  filterValues: FilterValue[]
) => {
  return filterValues
    .map(({ value }) => value)
    .every((_value) => {
      return Object.values(node)
        .filter(
          (v) =>
            typeof v === "string" ||
            typeof v === "number" ||
            typeof v === "boolean"
        )
        .some((_itemValue) => String(_itemValue).includes(_value as string));
    });
};

const getFilteredTiers = (
  tiers: TierNodeType[],
  filterValue: FilterProps["value"]
) => {
  const tierFilterValues = filterValue.filter(({ tagKey }) =>
    ["tier"].includes(tagKey)
  );
  const replicasetFilterValues = filterValue.filter(({ tagKey }) =>
    ["replicaset", "leaderState"].includes(tagKey)
  );
  const instanceFilterValues = filterValue.filter(({ tagKey }) =>
    ["instance", "version", "currentState"].includes(tagKey)
  );
  const failureDomainsFilterValues = filterValue.filter(
    ({ tagKey }) => !Object.keys(tagKeyMapping).includes(tagKey)
  );
  const searchTextFilterValues = filterValue.filter(
    ({ tagKey }) => tagKey === SEARCH_TEXT_KEY
  );

  const resultTiers = tiers.reduce((_tiers, _tier) => {
    const tierIsInclude = getIncludesFilterValue(tierFilterValues, _tier);

    const resultReplicasets = (
      _tier.replicasets as ReplicasetNodeType[]
    ).reduce((_replicasets, _replicaset) => {
      const replicasetIsInclude = getIncludesFilterValue(
        replicasetFilterValues,
        _replicaset
      );

      if (
        replicasetIsInclude &&
        !instanceFilterValues.length &&
        !failureDomainsFilterValues.length &&
        !searchTextFilterValues.length
      ) {
        _replicasets.push(_replicaset);
        return _replicasets;
      }

      const resultInstances = (
        _replicaset.instances as InstanceNodeType[]
      ).reduce((_instances, _instance) => {
        const instanceIsInclude = getIncludesFilterValue(
          instanceFilterValues,
          _instance
        );

        const instanceFailureDomainsIsInclude =
          failureDomainsFilterValues.every((fdFilterValue) =>
            filterFailureDomainByExpressionType(fdFilterValue, _instance)
          );

        const instanceSearchTextIncludes = getIncludesSearchText(
          _instance,
          searchTextFilterValues
        );

        if (
          instanceIsInclude &&
          instanceFailureDomainsIsInclude &&
          instanceSearchTextIncludes
        ) {
          _instances.push(_instance);
        }

        return _instances;
      }, [] as InstanceNodeType[]);

      if (
        replicasetIsInclude &&
        (instanceFilterValues.length ||
          failureDomainsFilterValues.length ||
          searchTextFilterValues.length) &&
        resultInstances.length
      ) {
        _replicasets.push({
          ..._replicaset,
          instances: resultInstances,
        });
        return _replicasets;
      }

      if (
        replicasetIsInclude &&
        searchTextFilterValues.length &&
        !resultInstances.length
      ) {
        const replicasetSearchTextIncludes = getIncludesSearchText(
          _replicaset,
          searchTextFilterValues
        );
        if (replicasetSearchTextIncludes) {
          _replicasets.push(_replicaset);
        }
      }

      return _replicasets;
    }, [] as ReplicasetNodeType[]);

    if (
      tierIsInclude &&
      !replicasetFilterValues.length &&
      !instanceFilterValues.length &&
      !failureDomainsFilterValues.length &&
      !searchTextFilterValues.length
    ) {
      _tiers.push(_tier);
      return _tiers;
    }

    if (
      tierIsInclude &&
      (replicasetFilterValues.length ||
        instanceFilterValues.length ||
        failureDomainsFilterValues.length ||
        searchTextFilterValues.length) &&
      resultReplicasets.length
    ) {
      _tiers.push({
        ..._tier,
        replicasets: resultReplicasets,
      });
      return _tiers;
    }
    if (
      tierIsInclude &&
      searchTextFilterValues.length &&
      !resultReplicasets.length
    ) {
      const tierSearchTextIncludes = getIncludesSearchText(
        _tier,
        searchTextFilterValues
      );
      if (tierSearchTextIncludes) {
        _tiers.push(_tier);
        return _tiers;
      }
    }
    return _tiers;
  }, [] as TierNodeType[]);

  return resultTiers;
};

export const getNodesListByOpenedNodes = (
  tiers: TierNodeType[],
  openedNodes: string[],
  filterValue: FilterProps["value"],
  grouping?: "TIERS" | "REPLICASETS" | "INSTANCES",
  sort?: TSortValue
): (TierNodeType | ReplicasetNodeType | InstanceNodeType)[] => {
  const filteredTiers = getFilteredTiers(tiers, filterValue);
  let list: (TierNodeType | ReplicasetNodeType | InstanceNodeType)[] = [];

  if (grouping === "INSTANCES") {
    filteredTiers.forEach((tier) => {
      (tier.replicasets as ReplicasetNodeType[]).forEach((replicaset) => {
        (replicaset.instances as InstanceNodeType[]).forEach((instance) => {
          list.push(instance);
        });
      });
    });

    const sortedInstances = sortInstances(list as InstanceNodeType[], sort);
    return sortedInstances as InstanceNodeType[];
  }

  const sortedTiers = sortTiers(filteredTiers);
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
