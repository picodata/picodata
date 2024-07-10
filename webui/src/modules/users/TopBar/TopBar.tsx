import React from "react";
import cn from "classnames";

import { Input } from "shared/ui/Input/Input";
import { SearchIcon } from "shared/icons/SearchIcon";
import { useTranslation } from "shared/intl";

import {
  GroupByFilter,
  GroupByFilterProps,
} from "./GroupByFilter/GroupByFilter";

import styles from "./TopBar.module.scss";

type TopBarProps = GroupByFilterProps & {
  search: string;
  setSearch: (v: string) => void;
};

export const TopBar: React.FC<TopBarProps> = (props) => {
  const { groupByFilterValue, setGroupByFilterValue, search, setSearch } =
    props;

  const { translation } = useTranslation();

  return (
    <div className={cn(styles.container)}>
      <div className={styles.controls}>
        <div className={styles.left}>
          <Input
            value={search}
            classes={{ container: styles.searchInput }}
            onChange={setSearch}
            placeholder={translation.pages.users.search}
            rightIcon={<SearchIcon className={styles.searchIcon} />}
          />
        </div>
        <div className={styles.right}>
          <GroupByFilter
            groupByFilterValue={groupByFilterValue}
            setGroupByFilterValue={setGroupByFilterValue}
          />
        </div>
      </div>
    </div>
  );
};
