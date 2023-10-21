export const updateArrayItem = <T>(
  arr: T[],
  updItem: Partial<T>,
  key: keyof T
) => {
  return arr.map((item) => {
    if (item[key] === updItem[key]) {
      return {
        ...item,
        ...updItem,
      };
    }

    return item;
  });
};
