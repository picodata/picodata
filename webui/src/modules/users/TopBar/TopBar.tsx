import React from "react";

import { Input } from "shared/ui/Input/Input";
import { SearchIcon } from "shared/icons/SearchIcon";
import { useTranslation } from "shared/intl";

import {
  GroupByFilter,
  GroupByFilterProps,
} from "./GroupByFilter/GroupByFilter";
import {
  Controls,
  inputContainerSx,
  RightContainer,
  Root,
  searchIconSx,
} from "./StyledComponents";

type TopBarProps = GroupByFilterProps & {
  search: string;
  setSearch: (v: string) => void;
};

export const TopBar: React.FC<TopBarProps> = (props) => {
  const { groupByFilterValue, setGroupByFilterValue, search, setSearch } =
    props;

  const { translation } = useTranslation();

  return (
    <Root>
      <Controls>
        <div>
          <Input
            containerSx={inputContainerSx}
            value={search}
            onChange={setSearch}
            placeholder={translation.pages.users.search}
            rightIcon={<SearchIcon style={searchIconSx} />}
          />
        </div>
        <RightContainer>
          <GroupByFilter
            groupByFilterValue={groupByFilterValue}
            setGroupByFilterValue={setGroupByFilterValue}
          />
        </RightContainer>
      </Controls>
    </Root>
  );
};
