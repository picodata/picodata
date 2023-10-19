export const getUniqueArrayBy = <T>(arr: T[], key: keyof T): T[] => {
  const itemsMap = new Map<number, T>();

  arr.forEach((item) => {
    itemsMap.set(item[key] as number, item);
  });

  return Array.from(itemsMap.values());
};
