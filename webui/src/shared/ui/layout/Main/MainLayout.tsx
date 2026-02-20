import { Outlet } from "react-router-dom";

import { Header } from "shared/ui/Header";

import { LayoutMain, Root, WorkSpace } from "./StyledComponents";
import { SideMenu } from "./SideMenu";

export const MainLayout = () => {
  return (
    <Root>
      <SideMenu />
      <WorkSpace>
        <Header />
        <LayoutMain>
          <Outlet />
        </LayoutMain>
      </WorkSpace>
    </Root>
  );
};
