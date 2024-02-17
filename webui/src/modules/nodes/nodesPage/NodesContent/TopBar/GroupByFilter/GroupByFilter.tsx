import React from "react";

import { GroupByButton } from "shared/components/buttons/GroupByButton/GroupByButton";
import { useTranslation } from "shared/intl";

import { TGroupByValue } from "./config";

export type GroupByFilterProps = {
  groupByFilterValue?: TGroupByValue;
  setGroupByFilterValue: (value: TGroupByValue) => void;
};

export const GroupByFilter: React.FC<GroupByFilterProps> = (props) => {
  const { translation } = useTranslation();
  const groupByTranslation = translation.pages.instances.groupBy;

  const groupByOptions: Array<{ label: string; value: TGroupByValue }> = [
    {
      label: groupByTranslation.options.tiers,
      value: "TIERS",
    },
    {
      label: groupByTranslation.options.replicasets,
      value: "REPLICASETS",
    },
    {
      label: groupByTranslation.options.instances,
      value: "INSTANCES",
    },
  ];

  return (
    <GroupByButton
      items={groupByOptions}
      value={props.groupByFilterValue}
      onChange={props.setGroupByFilterValue}
    />
  );
};
