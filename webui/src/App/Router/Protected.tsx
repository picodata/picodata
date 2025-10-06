import { FC, useContext, useEffect } from "react";
import { Navigate, Outlet } from "react-router-dom";

import { LoadingIndicator, LoadingType } from "shared/ui/LoadingIndicator";
import { useSessionStore } from "shared/session/hooks/useSessionStore";
import { Routes } from "shared/router/config";
import { RefreshContext, useLogout } from "shared/entity/session";
import { useAppConfig } from "shared/entity/config";

export const Protected: FC = () => {
  const [tokens] = useSessionStore();
  const refresh = useContext(RefreshContext);
  const { mutate: logout } = useLogout();
  const config = useAppConfig();

  useEffect(() => {
    if (refresh?.error && config.isAuthEnabled) {
      logout();
    }
  }, [config.isAuthEnabled, logout, refresh?.error]);

  if (refresh?.isLoading) {
    return <LoadingIndicator type={LoadingType.ABSOLUTE} />;
  }

  if ((!refresh?.error && tokens.auth) || !config.isAuthEnabled) {
    return <Outlet />;
  }

  return <Navigate to={Routes.LOGIN} />;
};
