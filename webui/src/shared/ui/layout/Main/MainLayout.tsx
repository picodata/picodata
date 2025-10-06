import { Outlet } from "react-router-dom";

import { Header } from "shared/ui/Header";

import { SideMenu } from "./SideMenu/SideMenu";

import styles from "./MainLayout.module.scss";

export const MainLayout = () => {
  return (
    <div className={styles.container}>
      <Header />
      <main className={styles.layoutMain}>
        <SideMenu />
        <div className={styles.bodyWrapper}>
          <Outlet />
        </div>
      </main>
    </div>
  );
};
