let ID = -1;

export const generateId = () => ID--;

export const getEmptyKeyValueFilter = () => {
  return { id: generateId(), key: "", value: [] };
};
