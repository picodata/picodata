import React from "react";
import { TFilterByValue } from "./config";
import { FunnelIcon } from "components/icons/FunnelIcon";

import { ButtonModal } from "components/shared/ui/ButtonModal/ButtonModal";
import { FilterByModal } from "./FilterByModal/FilterByModal";

import styles from "./FilterBy.module.scss";

export type FilterByProps = {
  filterByValue?: TFilterByValue;
  setFilterByValue: (value?: TFilterByValue) => void;
};

export const FilterBy: React.FC<FilterByProps> = () => {
  // const { filterByValue, setFilterByValue } = props;

  return (
    <ButtonModal
      buttonProps={{
        size: "normal",
        rightIcon: <FunnelIcon />,
        children: "Filter by",
      }}
      modalProps={{
        title: "Filter by",
        children: (
          <FilterByModal
            keys={[
              "srv",
              "dc",
              "region",
              "region 2",
              "region 3",
              "region 4",
              "region 5",
            ]}
          />
        ),
        bodyClassName: styles.modal,
      }}
    />
  );
};
