export const getUniqueArrayBy = <T>(arr: T[], key: keyof T): T[] => {
  const itemsMap = new Map();

  arr.forEach((item) => {
    itemsMap.set(item[key], item);
  });

  return Array.from(itemsMap.values());
};
