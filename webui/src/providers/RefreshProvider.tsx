import React from "react";

import { RefreshContext } from "shared/entity/session/context";
import { useRefresh } from "shared/entity/session/useQuery/useRefresh";

type RefreshProviderProps = {
  children: React.ReactNode;
};

export const RefreshProvider: React.FC<RefreshProviderProps> = (props) => {
  const refresh = useRefresh();

  return (
    <RefreshContext.Provider value={refresh}>
      {props.children}
    </RefreshContext.Provider>
  );
};
