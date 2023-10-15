import { ButtonSelect } from "components/shared/ui/ButtonSelect/ButtonSelect";
import React from "react";
import { TSortByValue, sortByOptions } from "./config";
import { ArrowsUpDown } from "components/icons/ArrowsUpDown";

export type SortByProps = {
  sortByValue?: TSortByValue;
  setSortByValue: (value?: TSortByValue) => void;
};

export const SortBy: React.FC<SortByProps> = (props) => {
  const { sortByValue, setSortByValue } = props;

  return (
    <ButtonSelect
      size="normal"
      rightIcon={<ArrowsUpDown />}
      items={sortByOptions}
      value={sortByValue}
      onChange={setSortByValue}
    >
      Sort by
    </ButtonSelect>
  );
};
