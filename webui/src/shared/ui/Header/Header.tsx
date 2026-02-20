import logo from "assets/logo.svg";
import { useTranslation } from "shared/intl";
import { useLogout } from "shared/entity/session";
import { useSessionStore } from "shared/session";

import { Button } from "../Button/Button";

import { Actions, StyledHeader } from "./StyledComponents";

export const Header = () => {
  const [tokens] = useSessionStore();
  const { mutate: logout } = useLogout();
  const { signout } = useTranslation().translation.components;

  return (
    <StyledHeader>
      <img src={logo} />
      {tokens.auth && (
        <Actions>
          <Button theme="secondary" size="extraSmall" onClick={() => logout()}>
            {signout}
          </Button>
        </Actions>
      )}
    </StyledHeader>
  );
};
