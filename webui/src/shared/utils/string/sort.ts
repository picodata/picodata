export const sortByString = (
  strA: string,
  strB: string,
  options: SortByStringOptions = { order: "ASC" }
) => {
  if (strA < strB) return options.order === "ASC" ? -1 : 1;
  if (strA > strB) return options.order === "ASC" ? 1 : -1;
  return 0;
};

export type SortByStringOptions = { order: "DESC" | "ASC" };
