import { Translation } from "./translations";

export enum IntlLocale {
  EN = "en",
  RU = "ru",
}

export type TIntlContext = {
  translation: Translation;
  locale: IntlLocale;
  setLocale: (locale?: IntlLocale) => void;
};
