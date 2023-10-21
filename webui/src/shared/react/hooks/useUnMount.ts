import { useEffect } from "react";

export const useUnMount = (callback: () => void) => {
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => callback, []);
};
