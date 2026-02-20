import { sortByString, SortByStringOptions } from "shared/utils/string/sort";

export function sortByStringProp<T>(
  array: T[],
  prop: (x: T) => string,
  options?: SortByStringOptions
) {
  return [...array].sort((a, b) => sortByString(prop(a), prop(b), options));
}
