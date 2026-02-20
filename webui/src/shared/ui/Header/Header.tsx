import { useMemo } from "react";
import { Box } from "@mui/material";

import logo from "assets/logo.svg";
import { useTranslation } from "shared/intl";
import { useLogout } from "shared/entity/session";
import { useSessionStore } from "shared/session";

import { Button } from "../Button/Button";
import { useClusterInfo } from "../../entity/cluster/info";

import { Actions, Cluster, StyledHeader } from "./StyledComponents";

export const Header = () => {
  const [tokens] = useSessionStore();
  const { mutate: logout } = useLogout();
  const { signout } = useTranslation().translation.components;
  const { data: clusterInfoData } = useClusterInfo();
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
        <Cluster>{clusterName || "Cluster ID"}</Cluster>
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
      </Box>
    </StyledHeader>
  );
};
