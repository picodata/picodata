import { useMemo, useState } from "react";
import { NavLink } from "react-router-dom";

import { BurgerIcon } from "shared/icons/BurgerIcon";
import { NodesIcon } from "shared/icons/navLinks/NodesIcon";

import styles from "./SideMenu.module.scss";

export const SideMenu = () => {
  const [isActive] = useState<boolean>(false);

  const nonActiveMenu = useMemo(
    () => (
      <div className={styles.nonActiveWrapper}>
        <div className={styles.menuIcon}>
          <BurgerIcon fill="#848484" />
        </div>
      </div>
    ),
    []
  );

  const activeMenu = useMemo(
    () => (
      <div className={styles.activeWrapper}>
        <div className={styles.linksWrapper}>
          <NavLink to="/" className={styles.link}>
            <NodesIcon fill="#848484" className={styles.linkIcon} />
            Nodes
          </NavLink>
        </div>
      </div>
    ),
    []
  );

  return <>{isActive ? activeMenu : nonActiveMenu}</>;
};
