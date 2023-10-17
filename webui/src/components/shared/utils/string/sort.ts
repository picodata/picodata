export const sortByString = (
  strA: string,
  strB: string,
  options: { order: "DESC" | "ASC" } = { order: "ASC" }
) => {
  if (strA < strB) return options.order === "ASC" ? -1 : 1;
  if (strA > strB) return options.order === "ASC" ? 1 : -1;
  return 0;
};
