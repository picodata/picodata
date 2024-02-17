import React from "react";

import { SortByButton } from "shared/components/buttons/SortByButton/SortByButton";
import { useTranslation } from "shared/intl";

import { DEFAULT_SORT_ORDER, TSortByValue, TSortValue } from "./config";

export type SortByProps = {
  sortByValue?: TSortValue;
  setSortByValue: (value?: TSortValue) => void;
};

export const SortBy: React.FC<SortByProps> = (props) => {
  const { sortByValue, setSortByValue } = props;

  const { translation } = useTranslation();
  const sortByTranslation = translation.pages.instances.sortBy;

  const sortByOptions: Array<{ label: string; value: TSortByValue }> = [
    {
      label: sortByTranslation.options.name,
      value: "NAME" as const,
    },
    {
      label: sortByTranslation.options.failureDomain,
      value: "FAILURE_DOMAIN" as const,
    },
  ];

  return (
    <SortByButton
      items={sortByOptions}
      value={sortByValue?.by}
      onChange={(newValue) =>
        setSortByValue({
          by: newValue,
          order: sortByValue?.order ?? DEFAULT_SORT_ORDER,
        })
      }
      onIconClick={() => {
        if (!sortByValue) return;

        setSortByValue({
          by: sortByValue.by,
          order: sortByValue.order === "ASC" ? "DESC" : "ASC",
        });
      }}
    />
  );
};
