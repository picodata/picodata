import React, { useEffect } from "react";

import {
  GroupByFilter,
  GroupByFilterProps,
} from "./GroupByFilter/GroupByFilter";
import { SortBy, SortByProps } from "./SortBy/SortBy";
import { FilterBy, FilterByProps } from "./FilterBy/FilterBy";
import { DEFAULT_SORT_BY, DEFAULT_SORT_ORDER } from "./SortBy/config";
import { Filters } from "./Filters/Filters";
import { Controls, filtersSx, Right, Root } from "./StyledComponents";

type TopBarProps = GroupByFilterProps &
  SortByProps &
  FilterByProps & {
    showSortBy: boolean;
    showFilterBy: boolean;
  };

export const TopBar: React.FC<TopBarProps> = (props) => {
  const {
    groupByFilterValue,
    setGroupByFilterValue,
    showSortBy,
    sortByValue,
    setSortByValue,
    showFilterBy,
    filterByValue,
    setFilterByValue,
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
  }, [showSortBy, sortByValue, setSortByValue]);

  useEffect(() => {
    if (!showFilterBy) {
      setFilterByValue();
    }
  }, [showFilterBy, setFilterByValue]);

  return (
    <Root>
      <Controls>
        <div />
        <Right>
          {showSortBy && (
            <SortBy sortByValue={sortByValue} setSortByValue={setSortByValue} />
          )}
          {showFilterBy && (
            <FilterBy
              filterByValue={filterByValue}
              setFilterByValue={setFilterByValue}
            />
          )}
          <GroupByFilter
            groupByFilterValue={groupByFilterValue}
            setGroupByFilterValue={setGroupByFilterValue}
          />
        </Right>
      </Controls>
      {filterByValue && (
        <Filters
          sx={filtersSx}
          filterByValue={filterByValue}
          setFilterByValue={setFilterByValue}
        />
      )}
    </Root>
  );
};
