import React from "react";
import cn from "classnames";

import { Button } from "shared/ui/Button/Button";
import { Tag } from "shared/ui/Tag/Tag";
import { useTranslation } from "shared/intl";

import { TFilterByValue } from "../FilterBy/config";
import { formatFailDomain } from "../../utils";

import styles from "./Filters.module.scss";

type FiltersProps = {
  filterByValue: TFilterByValue;
  className?: string;
  setFilterByValue: (value?: TFilterByValue) => void;
};

export const Filters: React.FC<FiltersProps> = (props) => {
  const { filterByValue, className, setFilterByValue } = props;

  const { translation } = useTranslation();
  const filtersTranslations = translation.pages.instances.filters;

  const deleteItem = (filterForDelete: { key: string; value: string[] }) => {
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
        {filtersTranslations.clearAll}
      </Button>
      {filterByValue.domain.map((domain) => {
        const label = formatFailDomain(domain);

        return (
          <Tag
            key={label}
            theme="secondary"
            size="extraSmall"
            closeIcon
            onClose={() => deleteItem(domain)}
          >
            {label}
          </Tag>
        );
      })}
    </div>
  );
};
