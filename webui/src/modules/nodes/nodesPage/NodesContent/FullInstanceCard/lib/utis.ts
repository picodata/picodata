import { TIntlContext } from "shared/intl";

import { Tab } from "../model/types";

export const getTabTranslation = (tab: Tab, t: TIntlContext) => {
  const tabsTranslation =
    t.translation.pages.instances.list.fullInstanceCard.tabs;
  switch (tab) {
    case Tab.Common:
      return tabsTranslation.common;
    case Tab.Storage:
      return tabsTranslation.storage;
    case Tab.Replica:
      return tabsTranslation.replication;
  }
};
