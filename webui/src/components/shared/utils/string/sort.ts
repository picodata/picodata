export const sortByString = (
  strA: string,
  strB: string,
  options: { order: "desc" | "asc" } = { order: "asc" }
) => {
  if (strA < strB) return options.order === "asc" ? -1 : 1;
  if (strA > strB) return options.order === "asc" ? 1 : -1;
  return 0;
};
