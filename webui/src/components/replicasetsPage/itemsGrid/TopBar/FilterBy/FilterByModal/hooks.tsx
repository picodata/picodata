import { useState } from "react";
import { TKeyValueFilter } from "./DomainField/types";
import { generateId, getEmptyKeyValueFilter } from "./utils";
import { updateArrayItem } from "components/shared/utils/array/updateArrayItem";

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

  return [keyValueFilters, addNewKeyValueFilter, updateKeyValueFilter] as const;
};
