import { forwardRef, PropsWithChildren } from "react";

import { TopBar } from "./TopBar/TopBar";
import { ContentContainer, Root, ToolBarContainer } from "./StyledComponents";
import { GroupByFilterProps } from "./TopBar/GroupByFilter/GroupByFilter";
import { SortByProps } from "./TopBar/SortBy/SortBy";
import { FilterModuleProps } from "./TopBar/Filter";

type ContentWrapperProps = PropsWithChildren<
  Pick<GroupByFilterProps, "setGroupByFilterValue" | "groupByFilterValue"> &
    Pick<SortByProps, "sortByValue" | "setSortByValue"> &
    FilterModuleProps
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
      setSortByValue,
      children,
      filterTags,
      filterValue,
      onFilterValueChange,
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
            filterTags={filterTags}
            filterValue={filterValue}
            onFilterValueChange={onFilterValueChange}
          />
        </ToolBarContainer>
        <ContentContainer>{children}</ContentContainer>
      </Root>
    );
  }
);
