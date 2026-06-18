import { ReactNode } from "react";

import { SnackBarProvider } from "../shared/ui/SnackBar/SnackBar";

import { ApiProvider } from "./ApiProvider";
import { AppConfigProvider } from "./AppConfigProvider";
import { IntlProvider } from "./IntlProvider";
import { RefreshProvider } from "./RefreshProvider";

export const AppProviders = (props: { children: ReactNode }) => (
  <IntlProvider>
    <ApiProvider>
      <AppConfigProvider>
        <RefreshProvider>
          <SnackBarProvider>{props.children}</SnackBarProvider>
        </RefreshProvider>
      </AppConfigProvider>
    </ApiProvider>
  </IntlProvider>
);
