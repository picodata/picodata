import React, { useEffect } from "react";

import {
  GroupByFilter,
  GroupByFilterProps,
} from "./GroupByFilter/GroupByFilter";
import { SortBy, SortByProps } from "./SortBy/SortBy";

import styles from "./Filters.module.scss";
import { FilterBy } from "./FilterBy/FilterBy";

type FiltersProps = GroupByFilterProps &
  SortByProps & {
    showSortBy: boolean;
    showFilterBy: boolean;
  };

export const Filters: React.FC<FiltersProps> = (props) => {
  const {
    groupByFilterValue,
    setGroupByFilterValue,
    showSortBy,
    sortByValue,
    setSortByValue,
    showFilterBy,
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
        <GroupByFilter
          groupByFilterValue={groupByFilterValue}
          setGroupByFilterValue={setGroupByFilterValue}
        />
        {showSortBy && (
          <SortBy sortByValue={sortByValue} setSortByValue={setSortByValue} />
        )}
        {showFilterBy && (
          <FilterBy filterByValue={1} setFilterByValue={() => null} />
        )}
      </div>
    </div>
  );
};
