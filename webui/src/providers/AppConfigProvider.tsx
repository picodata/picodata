import React from "react";

import { AppConfigContext, configSync } from "shared/entity/config";
import {
  ConfigQueryContext,
  useConfigQuery,
} from "shared/entity/config/useQuery";

type ConfigProviderProps = {
  children: React.ReactNode;
};

export const AppConfigProvider: React.FC<ConfigProviderProps> = (props) => {
  const config = useConfigQuery();

  return (
    <ConfigQueryContext.Provider value={config}>
      <AppConfigContext.Provider value={config.data ?? configSync}>
        {props.children}
      </AppConfigContext.Provider>
    </ConfigQueryContext.Provider>
  );
};
