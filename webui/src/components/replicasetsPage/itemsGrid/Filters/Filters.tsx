import React, { useEffect } from "react";

import {
  GroupByFilter,
  GroupByFilterProps,
} from "./GroupByFilter/GroupByFilter";
import { SortBy, SortByProps } from "./SortBy/SortBy";

import styles from "./Filters.module.scss";

type FiltersProps = GroupByFilterProps &
  SortByProps & {
    showSortBy: boolean;
  };

export const Filters: React.FC<FiltersProps> = (props) => {
  const {
    groupByFilterValue,
    setGroupByFilterValue,
    showSortBy,
    sortByValue,
    setSortByValue,
  } = props;

  useEffect(() => {
    if (showSortBy && sortByValue === undefined) {
      setSortByValue("NAME");
    }

    if (!showSortBy) {
      setSortByValue();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [showSortBy]);

  return (
    <div className={styles.container}>
      <div />
      <div className={styles.right}>
        {showSortBy && (
          <SortBy sortByValue={sortByValue} setSortByValue={setSortByValue} />
        )}
        <GroupByFilter
          groupByFilterValue={groupByFilterValue}
          setGroupByFilterValue={setGroupByFilterValue}
        />
      </div>
    </div>
  );
};
