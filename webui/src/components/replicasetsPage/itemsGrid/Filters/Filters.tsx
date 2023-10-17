import React, { useEffect } from "react";

import {
  GroupByFilter,
  GroupByFilterProps,
} from "./GroupByFilter/GroupByFilter";
import { SortBy, SortByProps } from "./SortBy/SortBy";

import styles from "./Filters.module.scss";
import { FilterBy } from "./FilterBy/FilterBy";
import { DEFAULT_SORT_BY, DEFAULT_SORT_ORDER } from "./SortBy/config";

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
      setSortByValue({
        by: DEFAULT_SORT_BY,
        order: DEFAULT_SORT_ORDER,
      });
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
