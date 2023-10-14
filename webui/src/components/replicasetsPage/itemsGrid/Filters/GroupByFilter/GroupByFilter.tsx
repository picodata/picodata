import { ListDashesIcon } from "components/icons/ListDashesIcon";
import { ButtonSelect } from "components/shared/ui/ButtonSelect/ButtonSelect";
import React from "react";
import { TGroupByValue, groupByOptions } from "./config";

export type GroupByFilterProps = {
  groupByFilterValue?: TGroupByValue;
  setGroupByFilterValue: (value: TGroupByValue) => void;
};

export const GroupByFilter: React.FC<GroupByFilterProps> = (props) => {
  return (
    <ButtonSelect
      size="normal"
      rightIcon={<ListDashesIcon />}
      items={groupByOptions}
      value={props.groupByFilterValue}
      onChange={props.setGroupByFilterValue}
    >
      Group by
    </ButtonSelect>
  );
};
