import { forwardRef, PropsWithChildren } from "react";

import { TopBar } from "./TopBar/TopBar";
import { ContentContainer, Root, ToolBarContainer } from "./StyledComponents";
import { GroupByFilterProps } from "./TopBar/GroupByFilter/GroupByFilter";
import { SortByProps } from "./TopBar/SortBy/SortBy";
import { FilterByProps } from "./TopBar/FilterBy/FilterBy";

type ContentWrapperProps = PropsWithChildren<
  Pick<GroupByFilterProps, "setGroupByFilterValue" | "groupByFilterValue"> &
    Pick<SortByProps, "sortByValue" | "setSortByValue"> &
    Pick<FilterByProps, "filterByValue" | "setFilterByValue">
>;

export const ContentWrapper = forwardRef<
  HTMLElement | undefined,
  ContentWrapperProps
>(
  (
    {
      groupByFilterValue,
      setGroupByFilterValue,
      sortByValue,
      setFilterByValue,
      filterByValue,
      setSortByValue,
      children,
    },
    ref
  ) => {
    const groupedByInstances = groupByFilterValue === "INSTANCES";

    return (
      <Root ref={ref}>
        <ToolBarContainer>
          <TopBar
            groupByFilterValue={groupByFilterValue}
            setGroupByFilterValue={setGroupByFilterValue}
            sortByValue={sortByValue}
            showSortBy={groupedByInstances}
            setSortByValue={setSortByValue}
            showFilterBy={groupedByInstances}
            filterByValue={filterByValue}
            setFilterByValue={setFilterByValue}
          />
        </ToolBarContainer>
        <ContentContainer>{children}</ContentContainer>
      </Root>
    );
  }
);
