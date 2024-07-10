import React from "react";

import { ListDashesIcon } from "shared/icons/ListDashesIcon";
import { ButtonSelect } from "shared/ui/ButtonSelect/ButtonSelect";
import { useTranslation } from "shared/intl";

import { TGroupByValue, groupByOptions } from "./config";

export type GroupByFilterProps = {
  groupByFilterValue?: TGroupByValue;
  setGroupByFilterValue: (value: TGroupByValue) => void;
};

export const GroupByFilter: React.FC<GroupByFilterProps> = (props) => {
  const { translation } = useTranslation();

  return (
    <ButtonSelect
      size="small"
      rightIcon={<ListDashesIcon />}
      items={groupByOptions}
      value={props.groupByFilterValue}
      onChange={props.setGroupByFilterValue}
    >
      {translation.components.buttons.groupBy.label}
    </ButtonSelect>
  );
};
