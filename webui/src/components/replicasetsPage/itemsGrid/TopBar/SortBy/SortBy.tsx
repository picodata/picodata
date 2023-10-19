import { ButtonSelect } from "components/shared/ui/ButtonSelect/ButtonSelect";
import React from "react";
import { DEFAULT_SORT_ORDER, TSortValue, sortByOptions } from "./config";
import { ArrowsUpDown } from "components/icons/ArrowsUpDown";

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
