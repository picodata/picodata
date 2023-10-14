import React from "react";

import {
  GroupByFilter,
  GroupByFilterProps,
} from "./GroupByFilter/GroupByFilter";

import styles from "./Filters.module.scss";

type FiltersProps = GroupByFilterProps;

export const Filters: React.FC<FiltersProps> = (props) => {
  const { groupByFilterValue, setGroupByFilterValue } = props;

  return (
    <div className={styles.container}>
      <div />
      <div>
        <GroupByFilter
          groupByFilterValue={groupByFilterValue}
          setGroupByFilterValue={setGroupByFilterValue}
        />
      </div>
    </div>
  );
};
