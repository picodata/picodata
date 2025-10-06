import { ReactNode } from "react";

import { ApiProvider } from "./ApiProvider";
import { AppConfigProvider } from "./AppConfigProvider";
import { IntlProvider } from "./IntlProvider";
import { RefreshProvider } from "./RefreshProvider";

export const AppProviders = (props: { children: ReactNode }) => (
  <IntlProvider>
    <ApiProvider>
      <AppConfigProvider>
        <RefreshProvider>{props.children}</RefreshProvider>
      </AppConfigProvider>
    </ApiProvider>
  </IntlProvider>
);
