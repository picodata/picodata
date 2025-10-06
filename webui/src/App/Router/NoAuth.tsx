import { FC, useEffect } from "react";
import { Navigate, Outlet, useNavigate } from "react-router";

import { useAppConfig } from "shared/entity/config";
import { Routes } from "shared/router/config";
import { useSessionStore } from "shared/session/hooks/useSessionStore";

export const NoAuth: FC = () => {
  const navigate = useNavigate();
  const [tokens] = useSessionStore();
  const config = useAppConfig();

  useEffect(() => {
    if (tokens.auth || !config.isAuthEnabled) {
      navigate(Routes.HOME);
    }
  }, [config.isAuthEnabled, navigate, tokens]);

  if (!tokens.auth && config.isAuthEnabled) {
    return <Outlet />;
  }

  return <Navigate to={Routes.HOME} />;
};
