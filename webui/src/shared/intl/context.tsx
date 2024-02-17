import React, { useContext } from "react";

import { TIntlContext } from "./types";

export const IntlContext = React.createContext<TIntlContext | null>(null);

export const useTranslation = (): TIntlContext => {
  const intlContext = useContext(IntlContext);

  if (!intlContext) {
    throw new Error("useTranslation must be used within a IntlProvider");
  }

  return intlContext;
};
