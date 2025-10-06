import React from "react";
import { QueryClientProvider } from "react-query";

import { queryClient } from "shared/api";

type ApiProviderProps = {
  children: React.ReactNode;
};

export const ApiProvider: React.FC<ApiProviderProps> = (props) => {
  return (
    <QueryClientProvider client={queryClient}>
      {props.children}
    </QueryClientProvider>
  );
};
