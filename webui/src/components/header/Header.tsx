import logo from "assets/logo.svg";
import styles from "./Header.module.css";

export const Header = () => (
  <header className={styles.header}>
    <img src={logo} />
  </header>
);
