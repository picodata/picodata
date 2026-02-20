import { Outlet } from "react-router-dom";

import { Root } from "./StyledComponents";

export const CenteredLayout = () => {
  return (
    <Root>
      <Outlet />
    </Root>
  );
};
