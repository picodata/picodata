import { Outlet } from "react-router-dom";

import styles from "./Centered.module.scss";

export const CenteredLayout = () => {
  return (
    <div className={styles.container}>
      <Outlet />
    </div>
  );
};
