import { useMemo } from "react";
import { Box } from "@mui/material";
import { useNavigate } from "react-router-dom";

import logo from "assets/logo.svg";
import { useTranslation } from "shared/intl";
import { useLogout } from "shared/entity/session";
import { useSessionStore } from "shared/session";
import { useAppConfig } from "shared/entity/config";
import { Routes } from "shared/router/config";

import { Button } from "../Button/Button";
import { useClusterInfo } from "../../entity/cluster/info";

import { Actions, Cluster, StyledHeader } from "./StyledComponents";

export const Header = () => {
  const [tokens] = useSessionStore();
  const { mutate: logout } = useLogout();
  const { translation } = useTranslation();
  const { signout } = translation.components;
  const { isAuthEnabled } = useAppConfig();
  const navigate = useNavigate();
  const canReadCluster = !isAuthEnabled || Boolean(tokens.auth);
  const { data: clusterInfoData } = useClusterInfo(canReadCluster);
  const clusterName = useMemo(
    () => clusterInfoData?.clusterName,
    [clusterInfoData]
  );

  return (
    <StyledHeader>
      <Box>
        <img src={logo} />
      </Box>
      <Box>
        {canReadCluster && <Cluster>{clusterName || "Cluster ID"}</Cluster>}
      </Box>
      <Box>
        {tokens.auth && (
          <Actions>
            <Button
              theme="secondary"
              size="extraSmall"
              onClick={() => logout()}
            >
              {signout}
            </Button>
          </Actions>
        )}
        {isAuthEnabled && !tokens.auth && (
          <Actions>
            <Button
              theme="secondary"
              size="extraSmall"
              onClick={() => navigate(Routes.LOGIN)}
            >
              {translation.pages.pluginUi.signIn}
            </Button>
          </Actions>
        )}
      </Box>
    </StyledHeader>
  );
};
