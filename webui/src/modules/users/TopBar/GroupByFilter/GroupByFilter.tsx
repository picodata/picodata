import React from "react";

import { ListDashesIcon } from "shared/icons/ListDashesIcon";
import { ButtonSelect } from "shared/ui/ButtonSelect/ButtonSelect";

import { TGroupByValue, groupByOptions } from "./config";

export type GroupByFilterProps = {
  groupByFilterValue?: TGroupByValue;
  setGroupByFilterValue: (value: TGroupByValue) => void;
};

export const GroupByFilter: React.FC<GroupByFilterProps> = (props) => {
  const groupBy = "Группировать";

  return (
    <ButtonSelect
      size="small"
      rightIcon={<ListDashesIcon />}
      items={groupByOptions}
      value={props.groupByFilterValue}
      onChange={props.setGroupByFilterValue}
    >
      {groupBy}
    </ButtonSelect>
  );
};
