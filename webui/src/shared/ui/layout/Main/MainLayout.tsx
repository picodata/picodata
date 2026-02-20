import { Outlet } from "react-router-dom";

import { Header } from "shared/ui/Header";

import { SideMenu } from "./SideMenu/SideMenu";
import { BodyWrapper, LayoutMain, Root } from "./StyledComponents";

export const MainLayout = () => {
  return (
    <Root>
      <Header />
      <LayoutMain>
        <SideMenu />
        <BodyWrapper>
          <Outlet />
        </BodyWrapper>
      </LayoutMain>
    </Root>
  );
};
