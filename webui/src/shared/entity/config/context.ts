import { createContext, useContext } from "react";

import { configSync } from "./api";

export const AppConfigContext = createContext(configSync);

export const useAppConfig = () => {
  return useContext(AppConfigContext);
};
