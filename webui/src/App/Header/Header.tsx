import logo from "assets/logo.svg";

import styles from "./Header.module.scss";

export const Header = () => (
  <header className={styles.header}>
    <img className={styles.logo} src={logo} />
  </header>
);
