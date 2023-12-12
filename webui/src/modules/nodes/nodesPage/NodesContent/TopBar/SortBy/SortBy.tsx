import React from "react";

import { ButtonSelect } from "shared/ui/ButtonSelect/ButtonSelect";
import { ArrowsUpDown } from "shared/icons/ArrowsUpDown";

import { DEFAULT_SORT_ORDER, TSortValue, sortByOptions } from "./config";

export type SortByProps = {
  sortByValue?: TSortValue;
  setSortByValue: (value?: TSortValue) => void;
};

export const SortBy: React.FC<SortByProps> = (props) => {
  const { sortByValue, setSortByValue } = props;

  return (
    <ButtonSelect
      size="normal"
      rightIcon={
        <ArrowsUpDown
          onClick={(event) => {
            event.stopPropagation();

            if (!sortByValue) return;

            setSortByValue({
              by: sortByValue.by,
              order: sortByValue.order === "ASC" ? "DESC" : "ASC",
            });
          }}
        />
      }
      items={sortByOptions}
      value={sortByValue?.by}
      onChange={(newValue) =>
        setSortByValue({
          by: newValue,
          order: sortByValue?.order ?? DEFAULT_SORT_ORDER,
        })
      }
    >
      Sort by
    </ButtonSelect>
  );
};
