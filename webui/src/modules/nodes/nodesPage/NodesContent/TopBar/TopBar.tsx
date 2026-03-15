import React, { useEffect } from "react";
import { Box } from "@mui/material";

import {
  GroupByFilter,
  GroupByFilterProps,
} from "./GroupByFilter/GroupByFilter";
import { SortBy, SortByProps } from "./SortBy/SortBy";
import { DEFAULT_SORT_BY, DEFAULT_SORT_ORDER } from "./SortBy/config";
import { ButtonsContainer, Root } from "./StyledComponents";
import { Filter, FilterModuleProps } from "./Filter";

type TopBarProps = GroupByFilterProps &
  SortByProps & {
    showSortBy: boolean;
  } & FilterModuleProps;

export const TopBar: React.FC<TopBarProps> = (props) => {
  const {
    groupByFilterValue,
    setGroupByFilterValue,
    showSortBy,
    sortByValue,
    setSortByValue,
    filterTags,
    filterValue,
    onFilterValueChange,
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

  return (
    <Root>
      <Box overflow={"hidden"}>
        <Filter
          filterTags={filterTags}
          filterValue={filterValue}
          onFilterValueChange={onFilterValueChange}
        />
      </Box>
      <ButtonsContainer>
        {showSortBy && (
          <SortBy sortByValue={sortByValue} setSortByValue={setSortByValue} />
        )}
        <GroupByFilter
          groupByFilterValue={groupByFilterValue}
          setGroupByFilterValue={setGroupByFilterValue}
        />
      </ButtonsContainer>
    </Root>
  );
};
