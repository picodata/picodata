import { en, ru } from "./translations";
import { IntlLocale } from "./types";

export const INTL_LOCALES = Object.values(IntlLocale);

export const DEFAULT_LOCALE = IntlLocale.RU;

export const LOCALES_MAP = {
  [IntlLocale.EN]: {
    translation: en,
  },
  [IntlLocale.RU]: {
    translation: ru,
  },
};
