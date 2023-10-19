import React from "react";
import cn from "classnames";

import { Button } from "components/shared/ui/Button/Button";
import { Tag } from "components/shared/ui/Tag/Tag";
import { TFilterByValue } from "../FilterBy/config";

import styles from "./Filters.module.scss";
// eslint-disable-next-line no-restricted-imports
import { formatFailDomain } from "../../utils";

type FiltersProps = {
  filterByValue: TFilterByValue;
  className?: string;
  setFilterByValue: (value?: TFilterByValue) => void;
};

export const Filters: React.FC<FiltersProps> = (props) => {
  const { filterByValue, className, setFilterByValue } = props;

  const deleteItem = (filterForDelete: { key: string; value: string }) => {
    setFilterByValue({
      ...filterByValue,
      domain: filterByValue.domain?.filter(
        (domain) =>
          !(
            domain.key === filterForDelete.key &&
            domain.value === filterForDelete.value
          )
      ),
    });
  };

  if (!filterByValue.domain?.length) return null;

  return (
    <div className={cn(styles.container, className)}>
      <Button
        theme="secondary"
        size="extraSmall"
        onClick={() => setFilterByValue()}
      >
        Clear All
      </Button>
      {filterByValue.domain.map((domain) => {
        const label = formatFailDomain(domain);

        return (
          <Tag
            key={label}
            theme="secondary"
            size="extraSmall"
            onIconClick={() => deleteItem(domain)}
          >
            {label}
          </Tag>
        );
      })}
    </div>
  );
};
