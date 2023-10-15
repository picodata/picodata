export const generateId = () => Date.now();

export const getEmptyKeyValueFilter = () => {
  return { id: generateId(), key: "", value: "" };
};
