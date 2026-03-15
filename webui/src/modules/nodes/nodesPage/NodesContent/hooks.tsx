import { useCallback, useMemo } from "react";
import { useSearchParams } from "react-router-dom";

import { sortByString, SortByStringOptions } from "shared/utils/string/sort";
import { TierType } from "shared/entity/tier";
import { ReplicasetType } from "shared/entity/replicaset";
import { InstanceType } from "shared/entity/instance";
import { FilterProps, FilterValue } from "shared/ui/Filter";

import { useTranslation } from "../../../../shared/intl";

import { getFilterTags } from "./utils";

export function sortByStringProp<T>(
  array: T[],
  prop: (x: T) => string,
  options?: SortByStringOptions
) {
  return [...array].sort((a, b) => sortByString(prop(a), prop(b), options));
}

export const useFilterTags = (
  data:
    | {
        tiers: TierType[];
        replicasets: ReplicasetType[];
        instances: InstanceType[];
      }
    | undefined
) => {
  const { translation } = useTranslation();
  return useMemo(() => getFilterTags(data, translation), [data, translation]);
};

export const useFilterValue = (): [
  FilterProps["value"],
  (filterValue: FilterProps["value"]) => void
] => {
  const [sp, setSp] = useSearchParams();
  const filterValueString = sp.get("filter");
  const filterValueChangeHandler = useCallback(
    (_filterValue: FilterProps["value"]) => {
      setSp((_sp) => {
        if (_filterValue.length) {
          _sp.set("filter", JSON.stringify(_filterValue));
        } else {
          _sp.delete("filter");
        }
        return _sp;
      });
    },
    [setSp]
  );

  const filterValue = useMemo(() => {
    let resultFilterValue: FilterValue[] = [];
    try {
      resultFilterValue = JSON.parse(
        filterValueString || "[]"
      ) as FilterValue[];
    } catch (error) {
      console.error(error);
    }
    return resultFilterValue;
  }, [filterValueString]);

  return useMemo(
    () => [filterValue, filterValueChangeHandler],
    [filterValue, filterValueChangeHandler]
  );
};
