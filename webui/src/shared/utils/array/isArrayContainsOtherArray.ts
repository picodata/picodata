export const isArrayContainsOtherArray = <T>(array1: T[], array2: T[]) => {
  return array2.every((item) => array1.includes(item));
};
