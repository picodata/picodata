import { z } from "zod";
import { useCallback } from "react";

import { useLsState } from "../localStorage/hooks/useLsState";

import { IntlLocale } from "./types";
import { DEFAULT_LOCALE } from "./constants";

export const useStoreLocale = () => {
  const [locale, setLocale] = useLsState({
    key: "locale",
    schema: z.nativeEnum(IntlLocale),
    defaultValue: DEFAULT_LOCALE,
  });

  const onLocaleChange = useCallback(
    (argsLocale?: IntlLocale) => {
      const newLocale = argsLocale ?? DEFAULT_LOCALE;

      setLocale(newLocale);
    },
    [setLocale]
  );

  return [locale, onLocaleChange] as const;
};
