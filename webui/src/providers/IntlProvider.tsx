import React, { useMemo } from "react";

import { IntlContext } from "shared/intl";
import { LOCALES_MAP } from "shared/intl/constants";
import { useStoreLocale } from "shared/intl/store";

type IntlProviderProps = {
  children: React.ReactNode;
};

export const IntlProvider: React.FC<IntlProviderProps> = (props) => {
  const { children } = props;

  const [locale, setLocale] = useStoreLocale();

  const localeContextValue = useMemo(() => {
    return {
      translation: LOCALES_MAP[locale].translation,
      locale,
      setLocale,
    };
  }, [locale, setLocale]);

  return (
    <IntlContext.Provider value={localeContextValue}>
      {children}
    </IntlContext.Provider>
  );
};
