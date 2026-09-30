import { FC, PropsWithChildren, useContext } from "react";
import { Navigate, Outlet } from "react-router-dom";

import { LoadingIndicator, LoadingType } from "shared/ui/LoadingIndicator";
import { useSessionStore } from "shared/session/hooks/useSessionStore";
import { Routes } from "shared/router/config";
import { RefreshContext } from "shared/entity/session";
import { useAppConfig } from "shared/entity/config";

export const Protected: FC<PropsWithChildren> = ({ children }) => {
  const [tokens] = useSessionStore();
  const refresh = useContext(RefreshContext);
  const config = useAppConfig();

  if (refresh?.isLoading && config.isAuthEnabled && !tokens.auth) {
    return <LoadingIndicator type={LoadingType.ABSOLUTE} />;
  }

  if (tokens.auth || !config.isAuthEnabled) {
    return <>{children ?? <Outlet />}</>;
  }

  return <Navigate to={Routes.LOGIN} />;
};
