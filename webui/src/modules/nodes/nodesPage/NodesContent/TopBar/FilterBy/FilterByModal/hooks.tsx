import { useState } from "react";

import { updateArrayItem } from "shared/utils/array/updateArrayItem";

import { TKeyValueFilter } from "./DomainField/types";
import { generateId, getEmptyKeyValueFilter } from "./utils";

export const useKeyValues = (
  propsKeyValueFilters: Omit<TKeyValueFilter, "id">[] = []
) => {
  const [keyValueFilters, setKeyValueFilters] = useState<TKeyValueFilter[]>(
    propsKeyValueFilters.length > 0
      ? propsKeyValueFilters?.map((filter) => ({
          ...filter,
          id: generateId(),
        }))
      : [getEmptyKeyValueFilter()]
  );

  const addNewKeyValueFilter = () => {
    setKeyValueFilters((prevFilters) => {
      return [...prevFilters, getEmptyKeyValueFilter()];
    });
  };

  const updateKeyValueFilter = (
    id: number,
    updData: Partial<Omit<TKeyValueFilter, "id">>
  ) => {
    setKeyValueFilters((prevFilters) => {
      return updateArrayItem(prevFilters, { id, ...updData }, "id");
    });
  };

  const deleteKeyValue = (id: number) => {
    setKeyValueFilters((prevFilters) => {
      return prevFilters.filter((f) => f.id !== id);
    });
  };

  return [
    keyValueFilters,
    addNewKeyValueFilter,
    updateKeyValueFilter,
    deleteKeyValue,
  ] as const;
};
