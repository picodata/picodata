import logo from "assets/logo.svg";
import { useTranslation } from "shared/intl";
import { useLogout } from "shared/entity/session";
import { useSessionStore } from "shared/session";

import { Button } from "../Button/Button";

import styles from "./Header.module.scss";

export const Header = () => {
  const [tokens] = useSessionStore();
  const { mutate: logout } = useLogout();
  const { signout } = useTranslation().translation.components;

  return (
    <header className={styles.header}>
      <img src={logo} />
      {tokens.auth && (
        <div className={styles.actions}>
          <Button theme="secondary" size="extraSmall" onClick={() => logout()}>
            {signout}
          </Button>
        </div>
      )}
    </header>
  );
};
